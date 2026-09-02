# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Discovery step handler — invoked by the scan pipeline state machine.

Reads the data source configuration from DynamoDB, dispatches to the
appropriate connector, discovers metadata, and persists to DataZone.

Input (from Step Functions):
    {
        "datasourceId": "DS#<id>",
        "scanJobId": "SCAN#<jobId>",
        "namespaceId": "<namespace-id>",
        "scanType": "full" | "incremental"
    }
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC
from typing import Any

from coa_common.dao import DynamoDBDAO
from coa_common.domain_models import DiscoveredMetadata
from coa_control_plane_server.models.source_status import SourceStatus
from coa_control_plane_server.models.source_sub_type import SourceSubType

from coa_sources.database.connectors import (
    MetadataConnector,
    get_connector,
)
from coa_sources.database.metadata_writer import write_to_datazone
from coa_sources.database.metrics import emit_metric

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DATASOURCES_TABLE = os.environ["SOURCES_TABLE"]
SCAN_JOBS_TABLE = os.environ["SOURCE_SCAN_JOBS_TABLE"]
SMUS_DOMAIN_ID = os.environ["SMUS_DOMAIN_ID"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Guardrail: maximum number of tables a single source may register as DataZone
# assets in one discovery pass. Above this, the scan fails fast with an
# actionable message rather than silently hitting the Lambda timeout. 0 (or
# unset) disables the cap. Discovery itself is cheap; the cost is the per-table
# DataZone asset write, so the cap is sized to what the parallel writer clears
# well within the 15-minute Lambda timeout.
try:
    MAX_TABLES_PER_SOURCE = int(os.environ.get("MAX_TABLES_PER_SOURCE", "0"))
except ValueError:
    logger.warning(
        "Invalid MAX_TABLES_PER_SOURCE=%r; expected an integer. Disabling the table cap (0).",
        os.environ.get("MAX_TABLES_PER_SOURCE"),
    )
    MAX_TABLES_PER_SOURCE = 0

# Cap on the failed-table names stored on the scan-job record. A DynamoDB item is
# limited to 400 KB and this list is a signal, not an inventory — the count next
# to it is always exact.
_MAX_REPORTED_FAILED_TABLES = 50

_ds_dao: DynamoDBDAO | None = None
_scan_dao: DynamoDBDAO | None = None
_ns_dao: DynamoDBDAO | None = None


def _get_ds_dao() -> DynamoDBDAO:
    global _ds_dao
    if _ds_dao is None:
        _ds_dao = DynamoDBDAO(DATASOURCES_TABLE, region=AWS_REGION)
    return _ds_dao


def _get_scan_dao() -> DynamoDBDAO:
    global _scan_dao
    if _scan_dao is None:
        _scan_dao = DynamoDBDAO(SCAN_JOBS_TABLE, region=AWS_REGION)
    return _scan_dao


def _get_ns_dao() -> DynamoDBDAO:
    global _ns_dao
    if _ns_dao is None:
        table_name = os.environ.get("NAMESPACES_TABLE", "")
        if not table_name:
            raise RuntimeError("NAMESPACES_TABLE environment variable not set")
        _ns_dao = DynamoDBDAO(table_name, region=AWS_REGION)
    return _ns_dao


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point for the Discovery step."""
    datasource_id = event["datasourceId"]  # "DS#<uuid>"
    scan_job_id = event["scanJobId"]
    namespace_id = event["namespaceId"]
    scan_type = event.get("scanType", "full")
    scan_job_sk = event.get("scanJobSK", scan_job_id)  # ISO timestamp SK for source-scan-jobs

    # Strip "DS#" prefix to get the bare source UUID
    source_id = datasource_id.removeprefix("DS#")

    logger.info(
        "Discovery started: datasource=%s scan_job=%s type=%s",
        datasource_id,
        scan_job_id,
        scan_type,
    )

    # New sources-table key schema: PK=NS#{namespaceId}, SK=SRC#{sourceId}
    source_key = {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"}
    # source-scan-jobs key schema: PK=SRC#{sourceId}, SK=<ISO timestamp>
    scan_job_key = {"PK": f"SRC#{source_id}", "SK": scan_job_sk}

    try:
        # Fetch source configuration from sources-table
        item = _get_ds_dao().get(source_key)
        if not item:
            raise ValueError(f"Data source not found: {datasource_id}")

        # Mark source as SCANNING. Conditional update guards against
        # writing to a record that has been deleted concurrently (race vs.
        # delete-source): if the row no longer exists we surface a
        # ConditionalCheckFailedException rather than silently re-creating
        # an attribute on a tombstoned key.
        _get_ds_dao().update(
            key=source_key,
            update_fields={"status": SourceStatus.SCANNING},
            condition="attribute_exists(PK)",
        )

        source_type = item["sourceSubType"]  # GLUE_DATABASE or JDBC_DATABASE
        raw_config = item.get("configuration", "{}")
        config = json.loads(raw_config) if isinstance(raw_config, str) else raw_config

        # Athena federation for JDBC sources is provisioned by a separate,
        # dedicated Step Functions step (FederationProvisionerFn) — kept out of
        # discovery so its Lake Formation admin privilege stays isolated.

        # Dispatch to the appropriate connector. All JDBC engines (incl. Snowflake)
        # are discovered directly via their driver dialect (see connectors/dialects.py).
        connector = get_connector(source_type)
        scan_start = time.monotonic()
        metadata = _discover(connector, config, datasource_id, namespace_id, source_type, item)

        # Guardrail: fail fast on sources too large to register within the
        # Lambda timeout. Discovery is cheap; the per-table DataZone asset write
        # is the bottleneck. Surface an actionable message (scope with
        # schema/table filters) rather than letting the pipeline time out and
        # retry fruitlessly.
        if MAX_TABLES_PER_SOURCE and len(metadata.tables) > MAX_TABLES_PER_SOURCE:
            raise ValueError(
                f"Source has {len(metadata.tables)} tables, exceeding the limit of "
                f"{MAX_TABLES_PER_SOURCE}. Narrow the scope with schemaFilter / "
                f"schemaExcludeFilter / tableFilter on the source configuration "
                f"(e.g. exclude system schemas), then re-scan."
            )

        # Persist discovered metadata to DataZone
        project_id = _get_project_id(namespace_id)
        write_result = write_to_datazone(
            domain_id=SMUS_DOMAIN_ID,
            project_id=project_id,
            metadata=metadata,
            data_source_id=datasource_id,
        )

        # Update scan job with discovery counts
        scan_job_update: dict[str, Any] = {
            "tablesDiscovered": len(metadata.tables),
            "columnsDiscovered": metadata.total_columns,
        }
        # A connector that reads each table separately can lose a table without
        # failing the scan, and that table reaches review with no columns, no
        # comments, and no keys while enrichment backfills AI descriptions over
        # the gap. Recording it on the scan job makes the degradation visible to
        # the steward reviewing the result rather than only to whoever reads the
        # logs. The names are capped because this is a signal, not an inventory —
        # the count is the part that must always be right.
        if metadata.failed_tables:
            scan_job_update["tablesFailed"] = len(metadata.failed_tables)
            scan_job_update["failedTables"] = metadata.failed_tables[:_MAX_REPORTED_FAILED_TABLES]
        _get_scan_dao().update(
            key=scan_job_key,
            update_fields=scan_job_update,
            condition="attribute_exists(PK)",
        )

        emit_metric("ScanDuration", (time.monotonic() - scan_start) * 1000, "Milliseconds", SourceType=source_type)
        emit_metric("TablesDiscovered", len(metadata.tables), "Count", SourceType=source_type)
        if metadata.failed_tables:
            emit_metric("TablesFailed", len(metadata.failed_tables), "Count", SourceType=source_type)
            logger.warning(
                "Discovery completed with %d unreadable table(s) for %s: %s",
                len(metadata.failed_tables),
                datasource_id,
                metadata.failed_tables[:_MAX_REPORTED_FAILED_TABLES],
            )

        # Update source record with discovery results
        from datetime import datetime

        source_update: dict[str, Any] = {
            "tablesDiscovered": len(metadata.tables),
            # Distinct schemas (Athena databases for federated JDBC catalogs);
            # used by the federation step to scope Lake Formation grants.
            "discoveredSchemas": sorted({t.database for t in metadata.tables if t.database}),
            "lastScanAt": datetime.now(UTC).isoformat(),
            "lastScanJobId": scan_job_sk,
        }
        # Native Glue sources are queryable via AwsDataCatalog as soon as they're
        # scanned. JDBC sources become queryable only after the federation step
        # provisions the catalog, so that handler sets `queryable` for them.
        if source_type == SourceSubType.GLUE_DATABASE:
            source_update["queryable"] = True
        _get_ds_dao().update(
            key=source_key,
            update_fields=source_update,
            condition="attribute_exists(PK)",
        )

    except Exception as exc:
        logger.exception("Discovery failed for datasource %s", datasource_id)
        # Failure-path updates use raise_on_error=False: if the underlying
        # row has been deleted we still want to surface the original
        # exception, not a ConditionalCheckFailedException from cleanup.
        from datetime import datetime

        _get_ds_dao().update(
            key=source_key,
            update_fields={
                "status": SourceStatus.SCAN_FAILED,
                "lastScanJobId": scan_job_sk,
                "lastScanAt": datetime.now(UTC).isoformat(),
            },
            condition="attribute_exists(PK)",
            raise_on_error=False,
        )
        _get_scan_dao().update(
            key=scan_job_key,
            update_fields={"errorMessage": str(exc)},
            condition="attribute_exists(PK)",
            raise_on_error=False,
        )
        raise RuntimeError(str(exc)) from exc

    return {
        "datasourceId": datasource_id,
        "scanJobId": scan_job_id,
        "namespaceId": namespace_id,
        "scanType": scan_type,
        "tablesDiscovered": len(metadata.tables),
        "columnsDiscovered": metadata.total_columns,
        "assetsCreated": write_result.get("assets_created", 0),
    }


def _discover(
    connector: MetadataConnector,
    config: dict,
    datasource_id: str,
    namespace_id: str,
    source_type: str = "",
    source_item: dict[str, Any] | None = None,
) -> DiscoveredMetadata:
    """Run discovery with the given connector and configuration.

    ``source_item`` is the sources-table record. It carries attributes that are
    not part of the stored ``configuration`` blob — notably the Athena data
    catalog name a custom-connector source is registered under, which the control
    plane derives at create time rather than accepting from the caller.
    """
    item = source_item or {}
    connector_config = {
        "database_name": config["databaseName"],
        # Custom-connector (ATHENA_CONNECTOR) sources only. Read from the source
        # record, not from `config`: the name is derived by the control plane and
        # stored as a top-level attribute, so it is not in the configuration blob
        # the caller supplied. Other connectors ignore it.
        "athena_data_catalog_name": item.get("athenaDataCatalogName", ""),
        "catalog_id": config.get("catalogId", ""),
        "region": config.get("region", AWS_REGION),
        "cross_account_role_arn": config.get("crossAccountRoleArn"),
        # ExternalId for the cross-account role's trust policy (JDBC secret
        # fetch + Glue metadata fetch). Optional; ignored when no role is set.
        "external_id": config.get("externalId"),
        # JDBC-only — passed through for schema-aware discovery (PG/Redshift).
        # Glue connector ignores these.
        "schema_filter": config.get("schemaFilter"),
        "schema_exclude_filter": config.get("schemaExcludeFilter"),
        "table_filter": config.get("tableFilter"),
        "table_exclude_filter": config.get("tableExcludeFilter"),
        # JDBC connection fields (no-op for Glue)
        "host": config.get("host"),
        "port": config.get("port"),
        "engine": config.get("engine"),
        "credential_secret_arn": config.get("credentialSecretArn"),
        # Snowflake-only: warehouse (required for INFORMATION_SCHEMA) + optional role.
        "warehouse": config.get("warehouse"),
        "role": config.get("role"),
        "data_source_id": datasource_id,
        "namespace_id": namespace_id,
    }

    # Test connection first
    validate_start = time.monotonic()
    test_result = connector.test_connection(connector_config)
    emit_metric("ValidationLatency", (time.monotonic() - validate_start) * 1000, "Milliseconds", SourceType=source_type)
    emit_metric(
        "ConnectionValidation",
        1,
        "Count",
        SourceType=source_type,
        Result="Success" if test_result.success else "Failure",
    )
    if not test_result.success:
        raise RuntimeError(
            f"Connection test failed: {test_result.message} "
            f"checks={json.dumps([c.__dict__ for c in test_result.checks])}"
        )

    discover_start = time.monotonic()
    metadata = connector.discover_metadata(connector_config)
    emit_metric("DiscoveryDuration", (time.monotonic() - discover_start) * 1000, "Milliseconds", SourceType=source_type)
    return metadata


def _get_project_id(namespace_id: str) -> str:
    """Resolve the DataZone project ID for a namespace."""
    item = _get_ns_dao().get({"PK": f"NS#{namespace_id}", "SK": "METADATA"})
    if not item or "dataZoneProjectId" not in item:
        raise ValueError(f"Namespace {namespace_id} not found or missing dataZoneProjectId")
    return item["dataZoneProjectId"]
