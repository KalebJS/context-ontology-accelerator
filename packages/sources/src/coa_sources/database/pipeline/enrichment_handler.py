# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Enrichment step handler — ECS Fargate task entry point.

Reads discovered metadata from SageMaker Catalog (DataZone), runs
AI-powered enrichment via Amazon Bedrock, and writes enriched business
metadata back as asset revisions.

Environment variables (injected by Step Functions container overrides):
    DATASOURCE_ID: The data source being enriched
    SCAN_JOB_ID: The current scan job ID
    NAMESPACE_ID: The namespace owning this data source
    SCAN_TYPE: "full" or "incremental"
    SMUS_DOMAIN_ID: DataZone domain ID
    DATASOURCES_TABLE: DynamoDB table for data source configs
    SCAN_JOBS_TABLE: DynamoDB table for scan job tracking
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

from coa_control_plane_server.models.source_status import SourceStatus

from coa_sources.database.pipeline.enrichment_metrics import (
    EnrichmentContext,
    EnrichmentMetricEmitter,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), stream=sys.stdout)


def handler(event: dict[str, Any] | None = None, context: Any = None) -> None:
    """Metadata enrichment entry point.

    Two invocation styles:
    - ECS task (production): called with no arguments; every parameter comes
      from the environment (Step Functions container overrides).
    - Lambda-style (local Docker stack): the scan worker calls
      ``handler(event, None)``; values in the event win, env is the fallback,
      so the same handler serves both without an env-var handoff.
    """
    event = event or {}
    datasource_id = event.get("datasourceId") or os.environ["DATASOURCE_ID"]  # "DS#<uuid>"
    scan_job_id = event.get("scanJobId") or os.environ["SCAN_JOB_ID"]
    namespace_id = event.get("namespaceId") or os.environ["NAMESPACE_ID"]
    scan_type = event.get("scanType") or os.environ.get("SCAN_TYPE", "full")
    scan_job_sk = event.get("scanJobSK") or os.environ.get("SCAN_JOB_SK", scan_job_id)  # ISO timestamp SK
    domain_id = event.get("smusDomainId") or os.environ["SMUS_DOMAIN_ID"]

    # Strip "DS#" prefix to get the bare source UUID
    source_id = datasource_id.removeprefix("DS#")

    logger.info(
        "Enrichment started: datasource=%s scan_job=%s namespace=%s type=%s",
        datasource_id,
        scan_job_id,
        namespace_id,
        scan_type,
    )

    # New sources-table key schema: PK=NS#{namespaceId}, SK=SRC#{sourceId}
    source_key = {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"}
    # source-scan-jobs key schema: PK=SRC#{sourceId}, SK=<ISO timestamp>
    scan_job_key = {"PK": f"SRC#{source_id}", "SK": scan_job_sk}

    # Resolve DataZone project ID from namespace
    from coa_common.dao import DynamoDBDAO

    region = os.environ.get("AWS_REGION", "us-east-1")
    namespaces_table = os.environ.get("NAMESPACES_TABLE")
    if not namespaces_table:
        raise ValueError("NAMESPACES_TABLE environment variable is required")
    ns_dao = DynamoDBDAO(namespaces_table, region=region)
    ns_item = ns_dao.get({"PK": f"NS#{namespace_id}", "SK": "METADATA"})
    if not ns_item or "dataZoneProjectId" not in ns_item:
        raise ValueError(f"Namespace {namespace_id} not found or missing dataZoneProjectId")
    project_id = ns_item["dataZoneProjectId"]

    ds_dao = DynamoDBDAO(os.environ["SOURCES_TABLE"], region=region)

    # Read source record so we can honour the per-source metadata enrichment
    # toggle. The discovery step requires the source to exist before this
    # task runs; if it's missing here the source was concurrently deleted
    # (or never created), and continuing would either crash on a missing
    # asset or write a partial record back via the DDB update below. Match
    # the discovery handler's pattern and fail loudly so the SFN error
    # chain transitions the source/scan job to SCAN_FAILED.
    source_item = ds_dao.get(source_key)
    if source_item is None:
        raise ValueError(f"Source record not found: {source_key}")

    # Default-enabled when the field is absent so legacy records (created
    # before the toggle existed) keep their historical behaviour.
    enrichment_enabled = bool(source_item.get("metadataEnrichmentEnabled", True))

    if not enrichment_enabled:
        logger.info(
            "Enrichment skipped (metadataEnrichmentEnabled=false): datasource=%s",
            datasource_id,
        )
        ds_dao.update(
            key=source_key,
            update_fields={"status": SourceStatus.PENDING_REVIEW},
            condition="attribute_exists(PK)",
        )
        return

    # Mark source as ENRICHING. Conditional update guards against writing to
    # a record deleted concurrently (race vs. delete-source): if the row no
    # longer exists we surface a ConditionalCheckFailedException rather than
    # silently re-creating a tombstoned key. Matches discovery_handler.
    ds_dao.update(
        key=source_key,
        update_fields={"status": SourceStatus.ENRICHING},
        condition="attribute_exists(PK)",
    )

    ctx = EnrichmentContext(source_item=source_item, namespace_id=namespace_id)
    emitter = EnrichmentMetricEmitter(ctx)

    try:
        from coa_sources.database.enrichment.table_enricher import run as enrich_tables

        enrich_start = time.monotonic()
        result = enrich_tables(
            datasource_id=datasource_id,
            namespace_id=project_id,
            domain_id=domain_id,
            scan_type=scan_type,
            emitter=emitter,
        )
        enrich_duration_ms = (time.monotonic() - enrich_start) * 1000

        emitter.emit_metric("EnrichmentJobDurationMs", enrich_duration_ms, "Milliseconds")
        emitter.emit_metric("TablesEnriched", result["tables_enriched"], "Count")
        emitter.emit_metric("TablesFailed", result["tables_failed"], "Count")
        emitter.emit_metric("TablesSkippedUnchanged", result["tables_skipped_unchanged"], "Count")
        emitter.emit_enrichment_estimated_cost()

        if result["tables_enriched"] == 0 and result["tables_failed"] == 0:
            ds_item = ds_dao.get(source_key)
            discovered = (ds_item or {}).get("tablesDiscovered", 0)
            if discovered:
                logger.warning(
                    "Discovery found %d tables but enrichment found 0 assets in DataZone. "
                    "Assets may not have been indexed yet or search query mismatch.",
                    discovered,
                )

        # Mark source as PENDING_REVIEW after successful enrichment
        ds_dao.update(
            key=source_key,
            update_fields={"status": SourceStatus.PENDING_REVIEW},
            condition="attribute_exists(PK)",
        )
    except Exception as exc:
        logger.exception("Enrichment failed for datasource %s", datasource_id)
        from datetime import UTC, datetime

        # Failure-path updates use condition + raise_on_error=False: if the
        # row was deleted concurrently we still want to surface the original
        # enrichment exception, not a ConditionalCheckFailedException from
        # cleanup. Matches discovery_handler.
        ds_dao.update(
            key=source_key,
            update_fields={
                "status": SourceStatus.SCAN_FAILED,
                "lastScanJobId": scan_job_sk,
                "lastScanAt": datetime.now(UTC).isoformat(),
            },
            condition="attribute_exists(PK)",
            raise_on_error=False,
        )
        scan_dao = DynamoDBDAO(os.environ["SOURCE_SCAN_JOBS_TABLE"], region=region)
        scan_dao.update(
            key=scan_job_key,
            update_fields={"errorMessage": str(exc)},
            condition="attribute_exists(PK)",
            raise_on_error=False,
        )
        raise RuntimeError(str(exc)) from exc

    logger.info(
        "Enrichment complete: datasource=%s enriched=%d failed=%d",
        datasource_id,
        result["tables_enriched"],
        result["tables_failed"],
    )


if __name__ == "__main__":
    handler()
