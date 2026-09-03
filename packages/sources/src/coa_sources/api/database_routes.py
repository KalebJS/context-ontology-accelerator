# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Database-source route handlers.

Handles all DATABASE source operations:
  POST   /namespaces/{namespaceId}/sources                                   — create database source
  GET    /namespaces/{namespaceId}/sources/{sourceId}/tables                 — list tables (DataZone)
  GET    /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}       — get table (DataZone)
  PUT    /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/review            — review one table
  PATCH  /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/metadata          — edit table metadata
  PUT    /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/review    — review column
  PATCH  /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/metadata — edit column meta
  GET    /namespaces/{namespaceId}/sources/{sourceId}/scan/{jobId}           — get scan job
  PUT    /namespaces/{namespaceId}/sources/{sourceId}/metadata               — update source-level metadata

Bulk approve/reject (ApproveSource, RejectSource) live in approve_reject_routes.py
because they are async via SQS.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from typing import Any
from urllib.parse import unquote

import boto3
import structlog
from botocore.exceptions import BotoCoreError, ClientError
from coa_common import sync_boto_config
from coa_common.metadata_store import SMUSClient
from coa_common.response import api_response, iso_to_epoch
from coa_control_plane_server.models.custom_connector_configuration import CustomConnectorConfiguration
from coa_control_plane_server.models.glue_configuration import GlueConfiguration
from coa_control_plane_server.models.glue_execution_engine import GlueExecutionEngine
from coa_control_plane_server.models.jdbc_configuration import JdbcConfiguration
from coa_control_plane_server.models.query_engine import QueryEngine
from coa_control_plane_server.models.source_status import SourceStatus
from coa_control_plane_server.models.source_sub_type import SourceSubType
from coa_control_plane_server.models.source_type import SourceType
from pydantic import ValidationError as PydanticValidationError

from coa_sources.database.connectors.athena_catalog import (
    AthenaCatalogConflictError,
    AthenaCatalogError,
    delete_lambda_catalog,
    derive_catalog_name,
    register_lambda_catalog,
)
from coa_sources.database.connectors.jdbc import DIRECT_QUERY_ENGINES
from coa_sources.database.metrics import emit_metric

from .namespace_counters import adjust_namespace_source_count
from .sources_handler import (
    _AWS_REGION,
    _PROJECT_ACCESS_ROLE_ARN,
    _REVIEW_QUEUE_URL,
    _SCAN_QUEUE_URL,
    _SMUS_DOMAIN_ID,
    _get_dao,
    _get_ns_dao,
    _get_scan_dao,
    _get_sqs,
    _now_iso,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# SMUS helpers
# ---------------------------------------------------------------------------


def _get_smus_client() -> SMUSClient:
    return SMUSClient(
        domain_id=_SMUS_DOMAIN_ID,
        region_name=_AWS_REGION,
        assume_role_arn=_PROJECT_ACCESS_ROLE_ARN or None,
        session_name="sources-api",
    )


def _resolve_project_id(namespace_id: str) -> str | None:
    try:
        item = _get_ns_dao().get({"PK": f"NS#{namespace_id}", "SK": "METADATA"})
    except ClientError:
        logger.exception("ddb_get_namespace_failed", namespace_id=namespace_id)
        return None
    if not item or "dataZoneProjectId" not in item:
        return None
    return item["dataZoneProjectId"]


# ---------------------------------------------------------------------------
# CREATE DATABASE SOURCE
# ---------------------------------------------------------------------------

# Athena's account-default Glue Data Catalog name.
_DEFAULT_ATHENA_CATALOG = "AwsDataCatalog"


def _resolve_glue_athena_catalog(glue_config: Any) -> str:
    """Resolve the Athena QueryExecutionContext.Catalog for a Glue source.

    - An explicit ``athenaDataCatalogName`` wins (caller knows their catalog).
    - A nested/cross-account ``catalogId`` of the form ``account:catalogName``
      maps to the nested catalog name.
    - A bare account-id ``catalogId`` (the account's root Data Catalog) maps to
      Athena's default ``AwsDataCatalog``.
    """
    if glue_config.athena_data_catalog_name:
        return glue_config.athena_data_catalog_name
    catalog_id = (glue_config.catalog_id or "").strip()
    if ":" in catalog_id:
        return catalog_id.split(":", 1)[1]
    return _DEFAULT_ATHENA_CATALOG


def _optional_int(value: Any) -> int | None:
    """Coerce a DynamoDB number to ``int``, passing ``None`` through.

    Needed wherever a response is built as a raw dict rather than through a Smithy
    model. The DAO reads via boto3's resource interface, so numbers arrive as
    ``Decimal``, and ``api_response`` serialises with ``default=str`` — so an
    uncoerced ``Decimal`` ships as a JSON string and a typed client hands its
    consumer a string where the schema promised a number.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning("non_numeric_dynamodb_number_ignored", value=repr(value))
        return None


def _arn_region(arn: str) -> str:
    """Region segment of an ARN, or ``""`` when it has no parsable one."""
    parts = (arn or "").split(":")
    return parts[3] if len(parts) > 5 else ""


def _arn_account(arn: str) -> str:
    """Account-id segment of an ARN, or ``""`` when it has no parsable one."""
    parts = (arn or "").split(":")
    return parts[4] if len(parts) > 5 else ""


# ---------------------------------------------------------------------------
# Credential-secret → namespace binding
# ---------------------------------------------------------------------------
#
# A credential secret is scoped to the namespace that registered the source: the
# secret must be tagged ``coa:namespace = <namespaceId>`` matching the namespace
# the source is created under. This keeps a namespace from registering a source
# that reads a credential secret belonging to a different namespace.
#
# We enforce it here, at the one point the caller supplies the ARN — an ARN that
# is not bound to this namespace is never persisted, so neither scan-time
# discovery nor serve-time query can read it. The tag is verified with
# DescribeSecret (metadata only — never GetSecretValue on this path). The IAM
# ``aws:ResourceTag`` conditions on the discovery/federation/serve roles enforce
# the same binding at the platform layer; this check is the fail-fast front door
# and guarantees the tag exists so those conditions can match.
_NAMESPACE_TAG_KEY = "coa:namespace"

_deployment_account: str | None = None


def _deployment_account_id() -> str:
    """This deployment's AWS account id (cached STS lookup)."""
    global _deployment_account
    if _deployment_account is None:
        sts = boto3.client("sts", region_name=_AWS_REGION, config=sync_boto_config())
        _deployment_account = sts.get_caller_identity()["Account"]
    return _deployment_account


def _describe_secret_tags(secret_arn: str, region: str) -> dict[str, str]:
    """Return the secret's tags as a ``{key: value}`` map via DescribeSecret.

    Metadata only — this never calls GetSecretValue. Raises ``ClientError`` /
    ``BotoCoreError`` on failure; the caller fails closed on those.
    """
    client = boto3.client("secretsmanager", region_name=region, config=sync_boto_config())
    described = client.describe_secret(SecretId=secret_arn)
    return {t["Key"]: t.get("Value", "") for t in described.get("Tags", []) if "Key" in t}


def _validate_credential_secret_binding(jdbc_config: Any, namespace_id: str) -> dict[str, Any] | None:
    """Bind a JDBC source's ``credentialSecretArn`` to the registering namespace.

    An IN-ACCOUNT secret must carry the tag ``coa:namespace == <namespaceId>``.
    Fail-closed on every uncertainty: a malformed ARN, an inability to resolve
    this deployment's account, a failure to read the secret's tags, or a
    missing/mismatched tag all refuse the request — we never persist a source we
    cannot prove is bound to this namespace, and we never treat an uncertainty as
    "cross-account, skip".

    Cross-account secrets (ARN account resolvable and != this deployment's
    account) are out of scope for tag binding: the deployment owns neither that
    account nor its tags, and access there is gated by the secret's own resource
    policy plus the cross-account assume-role, validated by connectivity at scan
    time.

    Returns an error response, or ``None`` when the binding holds or does not apply.
    """
    if jdbc_config is None:
        return None
    secret_arn = getattr(jdbc_config, "credential_secret_arn", None)
    if not secret_arn:
        return None

    # Fail closed on an unattributable ARN rather than treating it as
    # cross-account and skipping (defense in depth — the Smithy pattern normally
    # rejects a malformed ARN upstream, but this must not depend on that).
    secret_account = _arn_account(secret_arn)
    if not secret_account:
        return api_response(
            400,
            {"error": "credentialSecretArn is malformed: cannot determine its AWS account."},
        )

    # Resolving this deployment's account is required to tell in-account from
    # cross-account. If STS is unavailable we cannot make that call safely, so we
    # fail closed with a retryable 5xx instead of skipping the check (which would
    # bypass the binding) or raising an unhandled 500.
    try:
        deployment_account = _deployment_account_id()
    except (ClientError, BotoCoreError):
        logger.warning("deployment_account_resolution_failed", exc_info=True)
        return api_response(
            503,
            {"error": "Could not verify credentialSecretArn binding (account resolution failed); retry."},
        )

    if secret_account != deployment_account:
        # Cross-account secret — not bindable by a tag this deployment controls.
        return None

    region = _arn_region(secret_arn) or _AWS_REGION
    try:
        tags = _describe_secret_tags(secret_arn, region)
    except (ClientError, BotoCoreError):
        logger.warning(
            "credential_secret_binding_unverifiable",
            namespace_id=namespace_id,
            exc_info=True,
        )
        return api_response(
            400,
            {
                "error": (
                    "credentialSecretArn could not be verified. The secret must exist in this "
                    f"account and be readable (secretsmanager:DescribeSecret) and carry the tag "
                    f"'{_NAMESPACE_TAG_KEY}={namespace_id}'."
                )
            },
        )

    if tags.get(_NAMESPACE_TAG_KEY) != namespace_id:
        logger.warning(
            "credential_secret_binding_rejected",
            namespace_id=namespace_id,
            has_tag=_NAMESPACE_TAG_KEY in tags,
            bound_namespace=tags.get(_NAMESPACE_TAG_KEY),
        )
        return api_response(
            400,
            {
                "error": (
                    f"credentialSecretArn must be bound to this namespace: tag the secret with "
                    f"'{_NAMESPACE_TAG_KEY}={namespace_id}'. A secret may only be used by the "
                    "namespace it is tagged for."
                )
            },
        )
    return None


def _validate_custom_connector_configuration(custom_connector_config: Any) -> dict[str, Any] | None:
    """Reject a connector ARN outside this deployment's region.

    Athena can technically invoke a connector in another region when given its
    full ARN, but we do not support that topology: the IAM grant on both the
    discovery and serve roles is region-pinned, so a cross-region connector
    registers fine — registration only records a name → ARN mapping — and then
    fails every ``SHOW``/``DESCRIBE``/``SELECT`` with an AccessDenied that says
    nothing about the region. Catching it here turns that into a 400 that names
    the problem.

    Returns an error response, or ``None`` when the configuration is usable.
    """
    for field, arn in (("connectorFunctionArn", custom_connector_config.connector_function_arn),):
        if not arn:
            continue
        region = _arn_region(arn)
        if region != _AWS_REGION:
            return api_response(
                400,
                {
                    "error": (
                        f"customConnectorConfiguration.{field} must be in region '{_AWS_REGION}' "
                        f"(got '{region or 'none'}'). Deploy the connector Lambda in "
                        f"'{_AWS_REGION}'; cross-region connectors are not supported."
                    )
                },
            )
    return None


def _resolve_query_engine(glue_config: Any, jdbc_config: Any) -> QueryEngine:
    """Preferred single-source execution engine for the source.

    ``JDBC`` (direct, low-latency) only when a direct dialect is implemented for
    the engine (today PostgreSQL/Redshift). ``REDSHIFT`` when a Glue/Iceberg
    source opts in to Redshift Serverless (`awsdatacatalog` auto-mount) via
    ``GlueConfiguration.executionEngine`` — else Glue defaults to Athena. JDBC
    engines without a direct path also go through Athena — so serve never routes
    to a path that doesn't exist.

    A custom-connector (``CUSTOM_CONNECTOR``) source passes neither config and
    falls through to ``ATHENA``, which is correct and the only option: there is no
    direct adapter for an arbitrary customer connector, and serve's own gate
    (``_fetch_jdbc_source`` requires ``queryEngine == "JDBC"``) keeps it off the
    direct route without a route-selection change.
    """
    if jdbc_config is not None:
        engine = getattr(jdbc_config.engine, "value", jdbc_config.engine) or ""
        if str(engine).upper() in DIRECT_QUERY_ENGINES:
            return QueryEngine.JDBC
        return QueryEngine.ATHENA
    if glue_config is not None:
        execution_engine = getattr(glue_config, "execution_engine", None)
        engine = getattr(execution_engine, "value", execution_engine) or ""
        if str(engine).upper() == GlueExecutionEngine.REDSHIFT.value:
            return QueryEngine.REDSHIFT
    return QueryEngine.ATHENA


def _rollback_unscanned_source(namespace_id: str, source_id: str) -> bool:
    """Remove a source row whose scan never started; mark it recoverable if that fails.

    The row is written with ``status=REGISTERED``, which is one of
    ``SOURCE_ACTIVE_STATUSES``. So a row that survives a failed rollback is
    genuinely stuck: DELETE returns 409, re-scan is refused (it requires
    ``SCAN_FAILED``), namespace deletion counts it as blocking, and the scan
    reaper never fires because no Step Functions execution was ever started —
    leaving DynamoDB surgery as the only exit.

    Falling back to ``SCAN_FAILED`` costs one extra write and makes the row both
    deletable and re-scannable, which is the difference between a retryable
    failure and an operator ticket.

    Returns:
        Whether the row was actually removed. The caller uses this to keep the
        namespace source count honest: a row that survives was never counted, but
        its eventual delete will decrement.
    """
    try:
        _get_dao().delete({"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"})
        return True
    except ClientError:
        logger.exception("rollback_source_delete_failed", source_id=source_id)
    with contextlib.suppress(ClientError):
        _get_dao().update(
            key={"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
            update_fields={
                "status": SourceStatus.SCAN_FAILED,
                "errorMessage": "Source creation failed before the scan started; delete and re-create it.",
                "updatedAt": _now_iso(),
            },
        )
    return False


def _strip_external_id(config_dict: dict[str, Any], existing_config: Any = None) -> dict[str, Any]:
    """Drop a caller-supplied ``externalId`` from a source configuration.

    The ExternalId presented on a cross-account assume is derived from the
    namespace at discovery time (``discovery_handler._external_id``). Persisting
    one from the request would hand the caller back control of the value that
    binds the assume to its namespace, which is the whole control.

    LEGACY BRIDGE: an ``externalId`` already on the record predates that control
    and the customer's trust policy still pins it, so it is carried forward on
    update — updating a source must not break its next scan. Delete this
    alongside the stored branch in ``discovery_handler._external_id``.
    """
    config_dict.pop("externalId", None)
    # Stored configuration is a JSON string on records written by the API and a
    # plain dict on some older ones — discovery_handler tolerates both, so this
    # must too or a legacy ExternalId is dropped and the next scan breaks.
    if isinstance(existing_config, str):
        try:
            existing_config = json.loads(existing_config)
        except (json.JSONDecodeError, TypeError):
            existing_config = None
    if isinstance(existing_config, dict):
        stored = existing_config.get("externalId")
        if stored:
            config_dict["externalId"] = stored
    return config_dict


def _create_database_source(db_req: Any, namespace_id: str) -> dict[str, Any]:
    """Create a DATABASE source. db_req is a CreateDatabaseSourceInput model instance."""
    name: str = db_req.name.strip()
    if not name:
        return api_response(400, {"error": "name is required"})

    glue_config = db_req.glue_configuration
    jdbc_config = db_req.jdbc_configuration
    custom_connector_config = db_req.custom_connector_configuration
    supplied = [c for c in (glue_config, jdbc_config, custom_connector_config) if c is not None]
    if not supplied:
        return api_response(
            400,
            {"error": "glueConfiguration, jdbcConfiguration or customConnectorConfiguration is required"},
        )
    # The config member is what selects the sub-type, so more than one is
    # ambiguous rather than additive — silently preferring one would persist a
    # record whose stored blob does not match its sourceSubType.
    if len(supplied) > 1:
        return api_response(
            400,
            {"error": "Provide exactly one of glueConfiguration, jdbcConfiguration or customConnectorConfiguration"},
        )

    if custom_connector_config is not None:
        error = _validate_custom_connector_configuration(custom_connector_config)
        if error:
            return error

    # Bind a JDBC source's credential secret to this namespace before persisting.
    # An ARN not bound to this namespace never reaches the sources table, so
    # neither scan-time discovery nor serve-time query can read it.
    if jdbc_config is not None:
        error = _validate_credential_secret_binding(jdbc_config, namespace_id)
        if error:
            return error

    # A Glue source opting into Redshift execution must name the workgroup that
    # runs its queries — the backend cannot infer which Serverless workgroup to
    # use. Athena (the default) needs no workgroup.
    if glue_config is not None:
        execution_engine = getattr(glue_config, "execution_engine", None)
        engine_value = getattr(execution_engine, "value", execution_engine) or ""
        if str(engine_value).upper() == GlueExecutionEngine.REDSHIFT.value and not getattr(
            glue_config, "redshift_workgroup", None
        ):
            return api_response(
                400,
                {"error": "redshiftWorkgroup is required when executionEngine is REDSHIFT"},
            )

    if jdbc_config:
        sub_type = SourceSubType.JDBC_DATABASE
    elif custom_connector_config:
        sub_type = SourceSubType.CUSTOM_CONNECTOR
    else:
        sub_type = SourceSubType.GLUE_DATABASE
    source_id = str(uuid.uuid4())
    now = _now_iso()

    config_dict = _strip_external_id(supplied[0].to_dict())

    # We derive the Athena data-catalog name rather than letting the caller name
    # it: catalog names are account+region-global, so a caller-chosen name would
    # let two sources collide, and under the get-then-create guard the second
    # would bind to the first's connector Lambda. Deriving from the source id
    # guarantees a 1:1 source↔catalog mapping, so teardown removes only this
    # source's catalog.
    athena_catalog_name = derive_catalog_name(source_id) if custom_connector_config else ""

    item: dict[str, Any] = {
        "PK": f"NS#{namespace_id}",
        "SK": f"SRC#{source_id}",
        "sourceId": source_id,
        "namespaceId": namespace_id,
        "name": name,
        "sourceType": SourceType.DATABASE,
        "sourceSubType": sub_type,
        "status": SourceStatus.REGISTERED,
        "configuration": json.dumps(config_dict),
        # Query-layer metadata (read by serve to build the Athena request).
        # Catalog: native Glue resolves from catalogId (AwsDataCatalog for the
        # account-root catalog, else the nested catalog name); JDBC federated
        # catalogs are nested under AwsDataCatalog. A custom connector is neither
        # — its catalog is a top-level Lambda-backed catalog, so it names itself.
        # region is where Athena/the catalog live (Glue: source region; JDBC and
        # custom connector: deployment region).
        # queryable flips true once the source is actually queryable
        # (Glue: after first scan; JDBC and custom connector: after the
        # post-discovery federation step).
        "athenaCatalog": (
            athena_catalog_name
            if custom_connector_config
            else (_resolve_glue_athena_catalog(glue_config) if glue_config else _DEFAULT_ATHENA_CATALOG)
        ),
        # Preferred single-source execution engine. Direct JDBC only when a
        # direct dialect exists for the engine (PostgreSQL/Redshift today),
        # otherwise Athena.
        "queryEngine": _resolve_query_engine(glue_config, jdbc_config),
        "region": (glue_config.region if glue_config else _AWS_REGION),
        "queryable": False,
        "tablesDiscovered": 0,
        "tablesApproved": 0,
        "createdAt": now,
        "updatedAt": now,
        "sourceTypeCreatedAt": f"{SourceType.DATABASE.value}#{now}",
    }
    # For native Glue sources the Athena Database is the Glue database name.
    # JDBC federated sources resolve schema per-table, so no single database.
    if glue_config:
        item["athenaDatabase"] = glue_config.database_name
    # A custom-connector source is scoped to exactly ONE database inside its
    # catalog (`databaseName` is required), and serve pins the query Database to a
    # single value, so record it up front rather than waiting for discovery to
    # report it. That also gives serve the right namespace when a scan discovers
    # zero tables — otherwise it would fall back to a hardcoded default that has
    # nothing to do with this connector.
    if custom_connector_config:
        item["athenaDatabase"] = custom_connector_config.database_name
        # Serve routes on the PRESENCE of this attribute, so it is what makes the
        # source addressable at all. Written before the scan because `queryable`
        # is False until discovery succeeds, and serve skips a not-queryable
        # source's catalog — note it retargets the query at its default catalog
        # rather than refusing it, which is pre-existing behaviour shared by every
        # sub-type, so this attribute is not what gates the pre-scan window.
        item["athenaDataCatalogName"] = athena_catalog_name
    # Persist the metadata enrichment toggle whenever the caller specifies it
    # (true or false). Records that omit the field — legacy records, or
    # programmatic callers that don't set it — fall back to "enabled" at read
    # time in the enrichment handler.
    if db_req.metadata_enrichment_enabled is not None:
        item["metadataEnrichmentEnabled"] = db_req.metadata_enrichment_enabled
    # A Glue source's caller-declared nested catalog is NOT written to
    # `athenaDataCatalogName`, even though that is the attribute named after it.
    # That attribute is system-managed — DELETE derives the name it expects there
    # and runs a Lake-Formation-admin teardown (glue.delete_catalog,
    # lf.deregister_resource, glue.delete_connection) against it, under IAM scoped
    # to the deployment-wide `{prefix}ds_*` window rather than to one namespace. A
    # caller-supplied value in that field is therefore a request to tear down a
    # named catalog, which is not what declaring one's own catalog should mean.
    #
    # The value is not lost: `athenaCatalog` above carries it for serve (which
    # reads it to address the nested catalog), and the `configuration` blob
    # round-trips it back as `glueConfiguration.athenaDataCatalogName` on
    # GetSource. This also matches the Smithy contract, which documents the
    # top-level member as read-only, system-managed and populated for JDBC
    # sub-types only.
    #
    # Persist redshiftWorkgroup at top level ONLY for Glue sources that actually
    # execute via Redshift (queryEngine=REDSHIFT). Read by serve's Redshift
    # executor the way athenaCatalog/athenaDatabase are read by the Athena
    # executor. A workgroup on an ATHENA source is meaningless, so it is
    # not persisted — keeps the record honest and avoids a dead column.
    if glue_config and item["queryEngine"] == QueryEngine.REDSHIFT and getattr(glue_config, "redshift_workgroup", None):
        item["redshiftWorkgroup"] = glue_config.redshift_workgroup

    try:
        _get_dao().put(item)
    except ClientError:
        logger.exception("ddb_put_failed", source_id=source_id)
        return api_response(500, {"error": "Internal server error"})

    # Register the Athena data catalog for a custom-connector source.
    #
    # AFTER the DynamoDB put, deliberately: every delete path — source delete,
    # namespace-deletion cascade, the rollback below — keys off the source record,
    # so a catalog registered before the record would be orphaned with nothing
    # left able to find it. Registering after means the worst case is a recorded
    # source with no catalog, which the rollback here removes and a retry
    # re-creates cleanly.
    #
    # It has to happen at onboarding rather than in the scan pipeline: discovery
    # for this sub-type runs Athena SQL against the catalog, so the catalog must
    # already exist when the pipeline's first (discovery) step runs.
    if custom_connector_config:
        try:
            register_lambda_catalog(
                catalog_name=athena_catalog_name,
                connector_function_arn=custom_connector_config.connector_function_arn,
            )
        except AthenaCatalogError as exc:
            logger.exception(
                "athena_data_catalog_registration_failed",
                source_id=source_id,
                catalog_name=athena_catalog_name,
            )
            # A read timeout after Athena committed the create is
            # indistinguishable from a failure, so the catalog may exist. The
            # source row is about to go away and it is the only handle on that
            # name, so delete the catalog before dropping the row — otherwise it
            # is orphaned with nothing able to find it again.
            #
            # NOT on a conflict: that catalog demonstrably belongs to something
            # else, and deleting it would destroy a resource we did not create.
            if not isinstance(exc, AthenaCatalogConflictError):
                with contextlib.suppress(AthenaCatalogError):
                    delete_lambda_catalog(catalog_name=athena_catalog_name)
            _rollback_unscanned_source(namespace_id, source_id)
            return api_response(
                500,
                {"error": "Failed to register the Athena data catalog for the connector. Please retry."},
            )

    # Increment the namespace sourceCount. Best-effort: counter drift
    # is undesirable but must never block source creation.
    adjust_namespace_source_count(namespace_id, SourceType.DATABASE, 1)

    # Write initial scan job record — SK is the ISO timestamp (used as scanJobSK in SFN)
    scan_job_sk = now
    try:
        _get_scan_dao().put(
            {
                "PK": f"SRC#{source_id}",
                "SK": scan_job_sk,
                "sourceId": source_id,
                "namespaceId": namespace_id,
                "status": "IN_PROGRESS",
                "scanType": "full",
                "startedAt": now,
                "createdAt": now,
            }
        )
    except ClientError:
        logger.exception("ddb_put_scan_job_failed", source_id=source_id)

    # Enqueue to database scan queue — include scanJobPK/scanJobSK so the state machine
    # can update the correct source-scan-jobs record (PK=SRC#{sourceId}, SK=scanJobSK).
    # scanJobId uses a fresh UUID (matching old logic) so each execution has a unique name.
    if _SCAN_QUEUE_URL:
        try:
            _get_sqs().send_message(
                QueueUrl=_SCAN_QUEUE_URL,
                MessageBody=json.dumps(
                    {
                        "datasourceId": f"DS#{source_id}",
                        "sourceId": source_id,
                        "scanJobId": f"SCAN#{str(uuid.uuid4())}",
                        "scanJobPK": f"SRC#{source_id}",
                        "scanJobSK": scan_job_sk,
                        "namespaceId": namespace_id,
                        "scanType": "full",
                    }
                ),
            )
        except ClientError:
            # The scan never started, so the source must not be left orphaned
            # (a REGISTERED source that never scans) or mislabeled SCAN_FAILED
            # (which implies a scan ran and failed). Roll back the partial create
            # so DDB state matches the 500 the client receives and a retry is clean.
            logger.exception("sqs_send_failed", source_id=source_id)
            with contextlib.suppress(ClientError):
                _get_scan_dao().delete({"PK": f"SRC#{source_id}", "SK": scan_job_sk})
            # The catalog is registered by now, and the source row is about to go
            # away — which is what every delete path keys off — so remove it here
            # or it is orphaned for good. Best-effort: a failure is logged and the
            # 500 still returned, because failing the rollback must not turn a
            # retryable create failure into an unretryable one.
            if custom_connector_config:
                try:
                    delete_lambda_catalog(catalog_name=athena_catalog_name)
                except AthenaCatalogError:
                    logger.exception(
                        "rollback_athena_data_catalog_delete_failed",
                        source_id=source_id,
                        catalog_name=athena_catalog_name,
                    )
            source_deleted = _rollback_unscanned_source(namespace_id, source_id)
            # Only adjust the counter if the source row was actually removed, else
            # we'd drift the count. adjust_* is itself best-effort, so the 500 below
            # is always returned.
            if source_deleted:
                adjust_namespace_source_count(namespace_id, SourceType.DATABASE, -1)
            return api_response(
                500,
                {"error": "Failed to create source. Please retry."},
            )

    logger.info("database_source_created", source_id=source_id, namespace_id=namespace_id)
    return api_response(
        202,
        {
            "sourceId": source_id,
            "status": SourceStatus.REGISTERED,
            "scanJobId": scan_job_sk,
            "createdAt": iso_to_epoch(now),
        },
    )


# ---------------------------------------------------------------------------
# LIST TABLES
# ---------------------------------------------------------------------------


def _handle_list_tables(event: dict[str, Any], namespace_id: str, source_id: str) -> dict[str, Any]:
    """GET /namespaces/{namespaceId}/sources/{sourceId}/tables."""
    from coa_common.datazone_forms import FORM_TYPE_NAME
    from coa_control_plane_server.models.review_status import ReviewStatus
    from coa_control_plane_server.models.table_summary import TableSummary

    if not _SMUS_DOMAIN_ID:
        return api_response(500, {"error": "SMUS_DOMAIN_ID not configured"})

    qs = event.get("queryStringParameters") or {}
    review_status_filter = qs.get("reviewStatus")
    next_token = qs.get("nextToken")
    try:
        max_results = min(int(qs.get("maxResults", "50")), 50)  # DataZone Search API hard limit is 50
    except (ValueError, TypeError):
        return api_response(400, {"error": "maxResults must be a valid integer"})

    if review_status_filter:
        try:
            ReviewStatus(review_status_filter)
        except ValueError:
            return api_response(400, {"error": f"Invalid reviewStatus: {review_status_filter}"})

    project_id = _resolve_project_id(namespace_id)
    if not project_id:
        return api_response(404, {"error": f"Namespace {namespace_id} not found"})

    client = _get_smus_client()
    ds_key = f"DS#{source_id}"
    try:
        result = client.search_assets(
            project_id=project_id,
            search_text=ds_key,
            max_results=max_results,
            next_token=next_token,
        )
    except Exception:
        logger.exception("list_tables_search_failed", source_id=source_id, project_id=project_id)
        return api_response(500, {"error": "Failed to search tables"})

    items: list[dict[str, Any]] = []
    skipped_assets: int = 0
    for asset in result.items:
        if not asset.name.startswith(f"{ds_key}:"):
            continue
        try:
            detail = client.get_asset_forms(asset_id=asset.asset_id)
        except Exception:
            skipped_assets += 1
            logger.warning(
                "list_tables_asset_forms_failed",
                source_id=source_id,
                asset_id=asset.asset_id,
                asset_name=asset.name,
            )
            continue
        for form in detail.get("formsOutput", []):
            if form.get("formName") != FORM_TYPE_NAME:
                continue
            try:
                payload = json.loads(form["content"])
            except (json.JSONDecodeError, ValueError):
                skipped_assets += 1
                logger.warning(
                    "list_tables_form_parse_failed",
                    source_id=source_id,
                    asset_id=asset.asset_id,
                    asset_name=asset.name,
                    form_name=form.get("formName"),
                )
                continue
            table_id = f"{payload.get('databaseName', '')}.{payload.get('tableName', '')}"
            cols_field = payload.get("columns", "[]")
            try:
                columns_raw = json.loads(cols_field) if isinstance(cols_field, str) else (cols_field or [])
            except (json.JSONDecodeError, ValueError):
                skipped_assets += 1
                logger.warning(
                    "list_tables_columns_parse_failed",
                    source_id=source_id,
                    asset_id=asset.asset_id,
                    asset_name=asset.name,
                    table_id=table_id,
                )
                continue
            columns_approved = sum(
                1
                for c in columns_raw
                if isinstance(c, dict) and c.get("business_metadata", {}).get("review_status") == ReviewStatus.APPROVED
            )
            summary = TableSummary(
                tableId=table_id,
                name=payload.get("tableName", ""),
                database=payload.get("databaseName", ""),
                columnCount=payload.get("columnCount", 0),
                columnsApproved=columns_approved,
                reviewStatus=payload.get("reviewStatus", ReviewStatus.PENDING_REVIEW),
                enrichmentSource=payload.get("enrichmentSource"),
            )
            if review_status_filter and summary.review_status != review_status_filter:
                continue
            items.append(summary.to_dict())
            break

    response: dict[str, Any] = {"items": items}
    if skipped_assets > 0:
        response["skippedAssets"] = skipped_assets
    if result.next_token:
        response["nextToken"] = result.next_token
    return api_response(200, response)


# ---------------------------------------------------------------------------
# GET TABLE
# ---------------------------------------------------------------------------


def _handle_get_table(namespace_id: str, source_id: str, table_id: str) -> dict[str, Any]:
    """GET /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}."""
    from coa_common.datazone_forms import FORM_TYPE_NAME, deserialize_form
    from coa_control_plane_server.models.business_metadata_output import BusinessMetadataOutput
    from coa_control_plane_server.models.column_metadata import ColumnMetadata
    from coa_control_plane_server.models.foreign_key_output import ForeignKeyOutput
    from coa_control_plane_server.models.primary_key_output import PrimaryKeyOutput
    from coa_control_plane_server.models.review_status import ReviewStatus as RS
    from coa_control_plane_server.models.technical_metadata_output import TechnicalMetadataOutput

    if not _SMUS_DOMAIN_ID:
        return api_response(500, {"error": "SMUS_DOMAIN_ID not configured"})

    project_id = _resolve_project_id(namespace_id)
    if not project_id:
        return api_response(404, {"error": f"Namespace {namespace_id} not found"})

    client = _get_smus_client()
    ds_key = f"DS#{source_id}"
    asset_name = f"{ds_key}:{table_id}"
    try:
        result = client.search_assets(project_id=project_id, search_text=asset_name, max_results=1)
    except Exception:
        logger.exception("get_table_search_failed", source_id=source_id, table_id=table_id)
        return api_response(500, {"error": "Failed to search for table"})

    if not result.items or result.items[0].name != asset_name:
        return api_response(404, {"error": f"Table {table_id} not found"})

    try:
        detail = client.get_asset_forms(asset_id=result.items[0].asset_id)
    except Exception:
        logger.exception("get_table_forms_failed", source_id=source_id, table_id=table_id)
        return api_response(500, {"error": "Failed to retrieve table metadata"})

    for form in detail.get("formsOutput", []):
        if form.get("formName") != FORM_TYPE_NAME:
            continue
        try:
            payload = json.loads(form["content"])
            table = deserialize_form(payload, data_source_id=source_id)
        except Exception:
            logger.exception("get_table_deserialize_failed", source_id=source_id)
            return api_response(500, {"error": "Failed to parse table metadata"})

        biz = table.business_metadata
        pk = None
        if table.primary_key and table.primary_key.columns:
            pk = PrimaryKeyOutput(
                columns=table.primary_key.columns,
                source=table.primary_key.source or None,
                confidence=table.primary_key.confidence or None,
            ).to_dict()
        fks = [
            ForeignKeyOutput(
                column=fk.column,
                targetTable=fk.target_table,
                targetColumn=fk.target_column or None,
                source=fk.source or None,
                confidence=fk.confidence or None,
            ).to_dict()
            for fk in table.foreign_keys
        ] or None
        columns = []
        for col in table.columns:
            col_biz = None
            if col.business_metadata:
                col_biz = BusinessMetadataOutput(
                    description=col.business_metadata.description or None,
                    synonyms=col.business_metadata.synonyms or None,
                    glossaryTerms=col.business_metadata.glossary_terms or None,
                    tags=col.business_metadata.tags or None,
                    enrichmentSource=col.business_metadata.enrichment_source or None,
                    reviewStatus=col.business_metadata.review_status or None,
                    confidence=col.business_metadata.confidence or None,
                ).to_dict()
            columns.append(
                ColumnMetadata(
                    name=col.name,
                    dataType=col.data_type,
                    nullable=col.nullable,
                    isPartitionKey=col.is_partition_key or None,
                    # Source / curated description has a single home now
                    # (business_metadata.description). The Smithy
                    # ColumnMetadata type still exposes a top-level
                    # ``description`` for backward-compat with web UI; map
                    # it from business_metadata so callers that read either
                    # field see the same value.
                    description=col.business_metadata.description or None,
                    businessMetadata=col_biz,
                    distinctValues=col.distinct_values or None,
                ).to_dict()
            )
        tech = TechnicalMetadataOutput(
            columnCount=table.technical_metadata.column_count or None,
            partitionKeys=table.technical_metadata.partition_keys or None,
            format=table.technical_metadata.format or None,
            location=table.technical_metadata.location or None,
        ).to_dict()
        return api_response(
            200,
            {
                "tableId": table.table_id,
                "name": table.name,
                "database": table.database,
                "reviewStatus": biz.review_status or RS.PENDING_REVIEW,
                "businessMetadata": BusinessMetadataOutput(
                    description=biz.description or None,
                    synonyms=biz.synonyms or None,
                    glossaryTerms=biz.glossary_terms or None,
                    tags=biz.tags or None,
                    enrichmentSource=biz.enrichment_source or None,
                    reviewStatus=biz.review_status or None,
                ).to_dict(),
                "primaryKey": pk,
                "foreignKeys": fks,
                "columns": columns,
                "technicalMetadata": tech,
            },
        )

    return api_response(404, {"error": f"Table {table_id} not found"})


# ---------------------------------------------------------------------------
# REVIEW — Resource-oriented endpoints
# ---------------------------------------------------------------------------
#
# Replaces the former bulk POST /review god endpoint. Each handler operates on
# a single asset (one search + one get_asset_forms = ~400ms) and writes a
# DataZone revision only when the resulting state actually changes. This
# eliminates the N+1 pattern that caused the 29s API Gateway timeout for
# sources with 75+ tables.
#
# Cascade rules (match the legacy bulk semantics):
#   - APPROVE on a table cascades to PENDING columns; never overrides REJECTED.
#   - REJECT on a table sets all non-REJECTED columns to REJECTED.
#   - Edit endpoints (PATCH .../metadata) never change reviewStatus — they only
#     set business metadata fields and mark enrichmentSource = STEWARD_EDITED.
#
# Idempotency:
#   - Every review/edit is idempotent. If the resulting reviewStatus equals the
#     existing reviewStatus and no metadata changed, no DataZone revision is
#     written and the counter is not adjusted.
#
# Counter tracking:
#   - tablesApproved is bumped via DynamoDB ADD (atomic) only when a table
#     transitions in or out of APPROVED. Column-level review does not touch it.
#     Counter drift is logged but not raised — the DataZone asset is the canon.


def _smus_or_500() -> tuple[SMUSClient | None, dict[str, Any] | None]:
    """Build the SMUS client or return a 500 if domain not configured."""
    if not _SMUS_DOMAIN_ID:
        return None, api_response(500, {"error": "SMUS_DOMAIN_ID not configured"})
    return _get_smus_client(), None


def _project_or_404(namespace_id: str) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve namespace → DataZone project ID, or return 404."""
    project_id = _resolve_project_id(namespace_id)
    if not project_id:
        return None, api_response(404, {"error": f"Namespace {namespace_id} not found"})
    return project_id, None


def _load_single_asset(
    client: SMUSClient,
    project_id: str,
    source_id: str,
    table_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Search-and-load a single table asset by exact name match.

    Performs at most 2 DataZone API calls (search + get_asset_forms),
    independent of the total table count for the source. Returns
    (asset_dict, None) or (None, error_response).
    """
    from coa_common.datazone_forms import FORM_TYPE_NAME, deserialize_form

    ds_key = f"DS#{source_id}"
    asset_name = f"{ds_key}:{table_id}"
    try:
        # max_results=10 tolerates partial-prefix search hits; we filter to the exact name.
        result = client.search_assets(project_id=project_id, search_text=asset_name, max_results=10)
    except Exception:
        logger.exception("load_single_asset_search_failed", source_id=source_id, table_id=table_id)
        return None, api_response(500, {"error": "Failed to load table from catalog"})

    asset = next((a for a in result.items if a.name == asset_name), None)
    if asset is None:
        return None, api_response(404, {"error": f"Table '{table_id}' not found"})

    try:
        detail = client.get_asset_forms(asset_id=asset.asset_id)
    except Exception:
        logger.exception("load_single_asset_forms_failed", source_id=source_id, table_id=table_id)
        return None, api_response(500, {"error": "Failed to retrieve table metadata"})

    for form in detail.get("formsOutput", []):
        if form.get("formName") != FORM_TYPE_NAME:
            continue
        try:
            payload = json.loads(form["content"])
            table = deserialize_form(payload, data_source_id=source_id)
        except Exception:
            logger.exception("load_single_asset_deserialize_failed", source_id=source_id, table_id=table_id)
            return None, api_response(500, {"error": "Failed to parse table metadata"})
        return {"asset_id": asset.asset_id, "asset_name": asset.name, "table": table}, None

    return None, api_response(500, {"error": "Asset is missing CoaTableMetadata form"})


def _decision_to_status(decision: str) -> str:
    """Map a ReviewDecision value to its corresponding ReviewStatus value."""
    from coa_control_plane_server.models.review_decision import ReviewDecision
    from coa_control_plane_server.models.review_status import ReviewStatus

    return ReviewStatus.APPROVED if decision == ReviewDecision.APPROVED else ReviewStatus.REJECTED


def _approval_delta(old_status: str, new_status: str) -> int:
    """Compute the change to apply to tablesApproved when a table transitions.

    +1 when entering APPROVED, -1 when leaving APPROVED, 0 otherwise.
    """
    from coa_control_plane_server.models.review_status import ReviewStatus

    was_approved = old_status == ReviewStatus.APPROVED
    is_approved = new_status == ReviewStatus.APPROVED
    if was_approved == is_approved:
        return 0
    return 1 if is_approved else -1


def _adjust_approval_counter(namespace_id: str, source_id: str, delta: int) -> None:
    """Atomically adjust the tablesApproved counter on the source record.

    Failures are logged but not raised — the DataZone asset is the source of
    truth; counter drift can be reconciled by re-deriving the count from
    DataZone (re-aggregation). To make systematic drift observable, every
    failed increment emits an ``ApprovalCounterUpdateFailed`` metric so an
    alarm can fire when increments fail repeatedly (manual reconciliation:
    recompute tablesApproved from the APPROVED assets in DataZone and PUT it
    back on the source record).
    """
    if delta == 0:
        return
    try:
        _get_dao().atomic_increment(
            {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
            "tablesApproved",
            amount=delta,
        )
    except ClientError:
        logger.exception(
            "tables_approved_counter_failed",
            namespace_id=namespace_id,
            source_id=source_id,
            delta=delta,
        )
        # Emit a metric so ops can alarm on systematic counter drift.
        emit_metric("ApprovalCounterUpdateFailed", 1, "Count")


def _apply_business_overrides(biz: Any, overrides: dict[str, Any]) -> bool:
    """Apply MetadataOverrides fields to a BusinessMetadata object.

    Returns True if any field was changed (forces enrichmentSource to
    STEWARD_EDITED on change). Only non-None values mutate state — passing
    null/missing leaves the existing field untouched.
    """
    from coa_common.domain_models import EnrichmentSource

    field_map = {
        "description": "description",
        "synonyms": "synonyms",
        "glossaryTerms": "glossary_terms",
        "tags": "tags",
    }
    changed = False
    for in_field, out_attr in field_map.items():
        if in_field in overrides and overrides[in_field] is not None:
            new_value = overrides[in_field]
            if getattr(biz, out_attr) != new_value:
                setattr(biz, out_attr, new_value)
                changed = True
    if changed:
        biz.enrichment_source = EnrichmentSource.STEWARD_EDITED
    return changed


def _parse_decision(body: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """Validate and extract a ReviewDecision from a request body."""
    from coa_control_plane_server.models.review_decision import ReviewDecision

    decision = body.get("decision")
    if not decision:
        return None, api_response(400, {"error": "decision is required"})
    try:
        ReviewDecision(decision)
    except ValueError:
        return None, api_response(400, {"error": f"Invalid decision: {decision}"})
    return decision, None


# Source statuses where per-table/column review or edit is allowed.
# Excludes transient bulk states (APPROVING, REJECTING) where the worker
# owns the source, and pre-review states (REGISTERED, SCANNING, ENRICHING).
# After-review states (APPROVED, REJECTED) and failure states
# (APPROVAL_FAILED, REJECTION_FAILED) are reviewable so stewards can adjust
# decisions or recover from a failed bulk run.
_REVIEWABLE_STATES: frozenset[str] = frozenset(
    {
        # NOTE: REJECTED is intentionally absent. A successful bulk reject now
        # makes the source terminal-REJECTED (see worker _lifecycle); from
        # there no per-asset review/edit or bulk approve/reject is allowed —
        # the steward must re-onboard a new source. APPROVAL_FAILED /
        # REJECTION_FAILED remain reviewable so a failed bulk run can be retried.
        "PENDING_REVIEW",
        "APPROVED",
        "APPROVAL_FAILED",
        "REJECTION_FAILED",
    }
)


def _assert_source_reviewable(namespace_id: str, source_id: str) -> dict[str, Any] | None:
    """Return a 409 response if the source is in a non-reviewable state.

    Prevents per-table/column review/edit operations from racing with bulk
    approve/reject workers (or from running before discovery/enrichment is
    complete). One DDB read per call. Returns ``None`` when the operation
    may proceed.
    """
    try:
        item = _get_dao().get(
            {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
            projection=["status", "sourceType"],
        )
    except ClientError:
        logger.exception("assert_reviewable_get_failed", source_id=source_id)
        return api_response(500, {"error": "Internal server error"})

    if not item:
        return api_response(404, {"error": f"Source '{source_id}' not found"})
    if item.get("sourceType") != SourceType.DATABASE:
        return api_response(400, {"error": "Review operations only apply to DATABASE sources"})

    status = item.get("status")
    if status not in _REVIEWABLE_STATES:
        return api_response(
            409,
            {
                "error": (
                    f"Source is in '{status}' state; review operations require one of: {sorted(_REVIEWABLE_STATES)}"
                )
            },
        )
    return None


def _terminal_edit_block(review_status: str, kind: str) -> dict[str, Any] | None:
    """Return a 409 if the asset is in a terminal review state.

    Metadata and key edits are disabled once a table or column is REJECTED —
    the steward must re-onboard. PENDING_REVIEW and APPROVED items can still
    be edited (editing does not change the reviewStatus).
    """
    from coa_control_plane_server.models.review_status import ReviewStatus

    if review_status == ReviewStatus.REJECTED:
        return api_response(
            409,
            {
                "error": (
                    f"Cannot edit {kind} metadata: review status is '{review_status}' (terminal). "
                    f"Editing is only allowed while the {kind} is PENDING_REVIEW or APPROVED."
                )
            },
        )
    return None


def _handle_review_table(event: dict[str, Any], namespace_id: str, source_id: str, table_id: str) -> dict[str, Any]:
    """PUT /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/review.

    Apply a review decision to a single table. Cascades to columns per the
    shared rules in ``coa_common.review_logic``. Idempotent —
    only writes a DataZone revision if the table's reviewStatus changes.
    """
    from coa_common.datazone_forms import build_forms_input
    from coa_common.review_logic import apply_decision_to_table

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    decision, err = _parse_decision(body)
    if err:
        return err
    assert decision is not None

    err = _assert_source_reviewable(namespace_id, source_id)
    if err:
        return err

    client, err = _smus_or_500()
    if err:
        return err
    assert client is not None
    project_id, err = _project_or_404(namespace_id)
    if err:
        return err
    assert project_id is not None
    asset, err = _load_single_asset(client, project_id, source_id, table_id)
    if err:
        return err
    assert asset is not None

    table = asset["table"]
    old_table_status = table.business_metadata.review_status

    # Cascade to columns first, then the table (see review_logic). Per-asset
    # APPROVE flips PENDING columns to APPROVED; REJECT flips every
    # non-REJECTED column to REJECTED. Persisted atomically below.
    changed = apply_decision_to_table(table, decision)
    new_table_status = table.business_metadata.review_status

    if changed:
        # Write whenever the table OR any column changed — an aggressive reject
        # can flip a still-APPROVED column even when the table was already
        # REJECTED, and that column change must be persisted.
        try:
            client.create_asset_revision(
                asset_id=asset["asset_id"],
                name=asset["asset_name"],
                description=table.business_metadata.description or "",
                forms_input=build_forms_input(table),
            )
        except Exception:
            logger.exception("review_table_revision_failed", source_id=source_id, table_id=table_id)
            return api_response(500, {"error": "Failed to write review"})
        if old_table_status != new_table_status:
            _adjust_approval_counter(namespace_id, source_id, _approval_delta(old_table_status, new_table_status))
            # Acceptance rate, single-table half. Only transitions count — a
            # re-approve of an already-APPROVED table is not a new decision.
            from coa_control_plane_server.models.review_status import ReviewStatus

            if new_table_status == ReviewStatus.APPROVED:
                emit_metric("TablesApprovedByReview", 1, "Count", ReviewScope="Table")
            elif new_table_status == ReviewStatus.REJECTED:
                emit_metric("TablesRejectedByReview", 1, "Count", ReviewScope="Table")

    return api_response(200, {"tableId": table_id, "reviewStatus": new_table_status})


def _handle_update_table_metadata(
    event: dict[str, Any], namespace_id: str, source_id: str, table_id: str
) -> dict[str, Any]:
    """PATCH /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/metadata.

    Edit the table's business metadata fields. Sets enrichmentSource =
    STEWARD_EDITED. Does NOT change reviewStatus.
    """
    from coa_common.datazone_forms import build_forms_input

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    overrides = body.get("overrides")
    if not isinstance(overrides, dict) or not overrides:
        return api_response(400, {"error": "overrides is required"})

    err = _assert_source_reviewable(namespace_id, source_id)
    if err:
        return err

    client, err = _smus_or_500()
    if err:
        return err
    assert client is not None
    project_id, err = _project_or_404(namespace_id)
    if err:
        return err
    assert project_id is not None
    asset, err = _load_single_asset(client, project_id, source_id, table_id)
    if err:
        return err
    assert asset is not None

    table = asset["table"]
    if (blocked := _terminal_edit_block(table.business_metadata.review_status, "table")) is not None:
        return blocked
    if _apply_business_overrides(table.business_metadata, overrides):
        try:
            client.create_asset_revision(
                asset_id=asset["asset_id"],
                name=asset["asset_name"],
                description=table.business_metadata.description or "",
                forms_input=build_forms_input(table),
            )
        except Exception:
            logger.exception("update_table_metadata_revision_failed", source_id=source_id, table_id=table_id)
            return api_response(500, {"error": "Failed to write metadata update"})

    return api_response(
        200,
        {"tableId": table_id, "reviewStatus": table.business_metadata.review_status},
    )


def _handle_update_table_keys(
    event: dict[str, Any], namespace_id: str, source_id: str, table_id: str
) -> dict[str, Any]:
    """PATCH /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/keys.

    Replace the table's primary key and/or foreign keys. Steward edits are
    authoritative: keys that actually change are stored with
    source = STEWARD_SPECIFIED, overriding deterministic or AI-inferred keys.
    Keys that are unchanged keep their original provenance, so editing one key
    never re-stamps the others. An omitted field is left unchanged; an empty
    list clears it. Key columns are validated against the table's own columns.
    Does NOT change reviewStatus.
    """
    from coa_common.datazone_forms import build_forms_input
    from coa_common.domain_models import EnrichmentSource, ForeignKey, PrimaryKey

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    pk_in = body.get("primaryKey")
    fks_in = body.get("foreignKeys")
    if pk_in is None and fks_in is None:
        return api_response(400, {"error": "primaryKey or foreignKeys is required"})

    err = _assert_source_reviewable(namespace_id, source_id)
    if err:
        return err
    client, err = _smus_or_500()
    if err:
        return err
    assert client is not None
    project_id, err = _project_or_404(namespace_id)
    if err:
        return err
    assert project_id is not None
    asset, err = _load_single_asset(client, project_id, source_id, table_id)
    if err:
        return err
    assert asset is not None

    table = asset["table"]
    if (blocked := _terminal_edit_block(table.business_metadata.review_status, "table")) is not None:
        return blocked
    valid_cols = {c.name for c in table.columns}

    if pk_in is not None:
        cols = pk_in.get("columns")
        if not isinstance(cols, list):
            return api_response(400, {"error": "primaryKey.columns must be a list"})
        unknown = [c for c in cols if c not in valid_cols]
        if unknown:
            return api_response(400, {"error": f"Unknown primary key column(s): {unknown}"})
        # De-duplicate while preserving composite-key order.
        cols = list(dict.fromkeys(cols))
        existing_pk = table.primary_key
        # Only stamp STEWARD_SPECIFIED when the key actually changed; an
        # unchanged key keeps its original provenance (DETERMINISTIC / AI_INFERRED).
        if existing_pk and cols == existing_pk.columns:
            table.primary_key = PrimaryKey(columns=cols, source=existing_pk.source, confidence=existing_pk.confidence)
        else:
            table.primary_key = PrimaryKey(columns=cols, source=EnrichmentSource.STEWARD_SPECIFIED)

    if fks_in is not None:
        if not isinstance(fks_in, list):
            return api_response(400, {"error": "foreignKeys must be a list"})
        # Index existing FKs so unchanged rows retain their original provenance.
        existing_fk_by_key = {(fk.column, fk.target_table, fk.target_column or ""): fk for fk in table.foreign_keys}
        new_fks: list[ForeignKey] = []
        for fk in fks_in:
            col = fk.get("column") if isinstance(fk, dict) else None
            target = fk.get("targetTable") if isinstance(fk, dict) else None
            if not col or not target:
                return api_response(400, {"error": "Each foreign key requires column and targetTable"})
            if col not in valid_cols:
                return api_response(400, {"error": f"Unknown foreign key column: {col}"})
            tcol = fk.get("targetColumn") or ""
            existing = existing_fk_by_key.get((col, target, tcol))
            new_fks.append(
                existing
                if existing is not None
                else ForeignKey(
                    column=col, target_table=target, target_column=tcol, source=EnrichmentSource.STEWARD_SPECIFIED
                )
            )
        table.foreign_keys = new_fks

    try:
        client.create_asset_revision(
            asset_id=asset["asset_id"],
            name=asset["asset_name"],
            description=table.business_metadata.description or "",
            forms_input=build_forms_input(table),
        )
    except Exception:
        logger.exception("update_table_keys_revision_failed", source_id=source_id, table_id=table_id)
        return api_response(500, {"error": "Failed to write keys update"})

    return api_response(
        200,
        {"tableId": table_id, "reviewStatus": table.business_metadata.review_status},
    )


def _handle_review_column(
    event: dict[str, Any],
    namespace_id: str,
    source_id: str,
    table_id: str,
    column_name: str,
) -> dict[str, Any]:
    """PUT /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/review.

    Apply a review decision to a single column. Idempotent — only writes a
    DataZone revision if the column's reviewStatus changes. Does NOT change
    table-level review status or the tablesApproved counter.
    """
    from coa_common.datazone_forms import build_forms_input

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    decision, err = _parse_decision(body)
    if err:
        return err
    assert decision is not None

    err = _assert_source_reviewable(namespace_id, source_id)
    if err:
        return err

    client, err = _smus_or_500()
    if err:
        return err
    assert client is not None
    project_id, err = _project_or_404(namespace_id)
    if err:
        return err
    assert project_id is not None
    asset, err = _load_single_asset(client, project_id, source_id, table_id)
    if err:
        return err
    assert asset is not None

    table = asset["table"]
    column = next((c for c in table.columns if c.name == column_name), None)
    if column is None:
        return api_response(404, {"error": f"Column '{column_name}' not found on table '{table_id}'"})

    old_col_status = column.business_metadata.review_status
    new_col_status = _decision_to_status(decision)

    if old_col_status != new_col_status:
        column.business_metadata.review_status = new_col_status
        try:
            client.create_asset_revision(
                asset_id=asset["asset_id"],
                name=asset["asset_name"],
                description=table.business_metadata.description or "",
                forms_input=build_forms_input(table),
            )
        except Exception:
            logger.exception(
                "review_column_revision_failed",
                source_id=source_id,
                table_id=table_id,
                column_name=column_name,
            )
            return api_response(500, {"error": "Failed to write column review"})

    return api_response(
        200,
        {
            "tableId": table_id,
            "columnName": column_name,
            "reviewStatus": new_col_status,
        },
    )


def _handle_update_column_metadata(
    event: dict[str, Any],
    namespace_id: str,
    source_id: str,
    table_id: str,
    column_name: str,
) -> dict[str, Any]:
    """PATCH /namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/metadata.

    Edit a single column's business metadata. Sets enrichmentSource =
    STEWARD_EDITED. Does NOT change reviewStatus.
    """
    from coa_common.datazone_forms import build_forms_input

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    overrides = body.get("overrides")
    if not isinstance(overrides, dict) or not overrides:
        return api_response(400, {"error": "overrides is required"})

    err = _assert_source_reviewable(namespace_id, source_id)
    if err:
        return err

    client, err = _smus_or_500()
    if err:
        return err
    assert client is not None
    project_id, err = _project_or_404(namespace_id)
    if err:
        return err
    assert project_id is not None
    asset, err = _load_single_asset(client, project_id, source_id, table_id)
    if err:
        return err
    assert asset is not None

    table = asset["table"]
    column = next((c for c in table.columns if c.name == column_name), None)
    if column is None:
        return api_response(404, {"error": f"Column '{column_name}' not found on table '{table_id}'"})

    if (blocked := _terminal_edit_block(column.business_metadata.review_status, "column")) is not None:
        return blocked

    if _apply_business_overrides(column.business_metadata, overrides):
        try:
            client.create_asset_revision(
                asset_id=asset["asset_id"],
                name=asset["asset_name"],
                description=table.business_metadata.description or "",
                forms_input=build_forms_input(table),
            )
        except Exception:
            logger.exception(
                "update_column_metadata_revision_failed",
                source_id=source_id,
                table_id=table_id,
                column_name=column_name,
            )
            return api_response(500, {"error": "Failed to write metadata update"})

    return api_response(
        200,
        {
            "tableId": table_id,
            "columnName": column_name,
            "reviewStatus": column.business_metadata.review_status,
        },
    )


# ---------------------------------------------------------------------------
# BULK APPROVE / REJECT — async via SQS worker
# ---------------------------------------------------------------------------
#
# Bulk operations cannot run inline within the 29s API Gateway timeout for
# sources with 75+ tables (each table requires search + get_asset_forms +
# create_asset_revision = 3 DataZone API calls). Instead the request is
# enqueued and a worker Lambda processes it asynchronously.
#
# The status transition (PENDING_REVIEW or *_FAILED) → APPROVING/REJECTING
# happens via a conditional DynamoDB update, which gives us idempotency: a
# duplicate API call (or replayed SQS message at the worker layer) finds the
# source already in the transient state and is rejected as a 409, preventing
# two workers from racing on the same source.
#
# Frontend polls GetSource for the source's `status` field. Terminal states:
#   - APPROVE flow:  APPROVED        (or APPROVAL_FAILED on worker error)
#   - REJECT flow:   PENDING_REVIEW  (rejection puts the source back into
#                                     review; tables marked REJECTED retain
#                                     that status — REJECTION_FAILED on error)


# Per-decision lifecycle: maps a ReviewDecision value to the
# (transient_status, failure_status, allowed_entry_states) triple.
def _bulk_lifecycle(decision: str) -> tuple[str, str, tuple[str, str]]:
    """Return (transient, failed, allowed_entry_states) for a decision."""
    from coa_control_plane_server.models.review_decision import ReviewDecision

    if decision == ReviewDecision.APPROVED:
        return (
            SourceStatus.APPROVING,
            SourceStatus.APPROVAL_FAILED,
            (SourceStatus.PENDING_REVIEW, SourceStatus.APPROVAL_FAILED),
        )
    if decision == ReviewDecision.REJECTED:
        return (
            SourceStatus.REJECTING,
            SourceStatus.REJECTION_FAILED,
            (SourceStatus.PENDING_REVIEW, SourceStatus.REJECTION_FAILED),
        )
    raise ValueError(f"Unsupported decision: {decision}")


def _enqueue_bulk_review(namespace_id: str, source_id: str, decision: str) -> dict[str, Any] | None:
    """Send a bulk-review SQS message. Returns an error response on failure."""
    if not _REVIEW_QUEUE_URL:
        return api_response(500, {"error": "REVIEW_QUEUE_URL not configured"})
    payload = {
        "namespaceId": namespace_id,
        "sourceId": source_id,
        "decision": decision,
    }
    try:
        _get_sqs().send_message(
            QueueUrl=_REVIEW_QUEUE_URL,
            MessageBody=json.dumps(payload),
            # Best-effort dedup: SQS Standard doesn't enforce it, but the conditional
            # status transition above is the real guard. The MessageGroupId is unused
            # for Standard queues; for FIFO we'd set it to source_id.
        )
    except ClientError:
        logger.exception("enqueue_bulk_review_failed", source_id=source_id)
        return api_response(500, {"error": "Failed to enqueue bulk review"})
    return None


def _transition_to_transient(
    namespace_id: str,
    source_id: str,
    transient_status: str,
    allowed_entry_states: tuple[str, ...],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Atomically transition source status to a bulk transient state.

    The conditional update only fires if the source is currently in one of
    ``allowed_entry_states`` (typically PENDING_REVIEW or the matching
    *_FAILED). On condition failure we return 409 with the actual current
    status. Returns (source_item, None) on success, (None, error_response)
    on failure.
    """
    try:
        item = _get_dao().get({"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"})
    except ClientError:
        logger.exception("ddb_get_for_transition_failed", source_id=source_id)
        return None, api_response(500, {"error": "Internal server error"})
    if not item:
        return None, api_response(404, {"error": f"Source '{source_id}' not found"})
    if item.get("sourceType") != SourceType.DATABASE:
        return None, api_response(400, {"error": "Bulk review only applies to DATABASE sources"})

    # Bulk approve / reject is meaningless when no tables have been discovered
    # yet (the worker would no-op against an empty asset set). Reject the
    # request rather than transition the source into a transient state and
    # leave the user staring at an APPROVING / REJECTING badge that never
    # resolves. The UI also disables the buttons in this case but we enforce
    # here to prevent direct API misuse.
    tables_discovered = int(item.get("tablesDiscovered") or 0)
    if tables_discovered <= 0:
        return None, api_response(
            400,
            {
                "error": (
                    "Bulk review requires at least one discovered table; "
                    "the source has 0 tables. Wait for the scan to complete or re-scan the source first."
                ),
                "sourceId": source_id,
                "tablesDiscovered": tables_discovered,
            },
        )

    # Build IN-clause placeholders dynamically so the helper supports any tuple
    # of allowed entry states.
    placeholders = [f":entry{i}" for i in range(len(allowed_entry_states))]
    condition = f"#status IN ({', '.join(placeholders)})"
    condition_values = dict(zip(placeholders, allowed_entry_states, strict=True))

    try:
        _get_dao().update(
            {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
            {"status": transient_status},
            condition=condition,
            condition_names={"#status": "status"},
            condition_values=condition_values,
            raise_on_error=True,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None, api_response(
                409,
                {
                    "error": (
                        f"Source is in '{item.get('status')}' state; "
                        f"bulk review requires one of: {list(allowed_entry_states)}"
                    )
                },
            )
        logger.exception("transition_to_transient_failed", source_id=source_id)
        return None, api_response(500, {"error": "Internal server error"})

    return item, None


def _handle_bulk_review(
    event: dict[str, Any],  # noqa: ARG001 — body is unused; decision is path-derived
    namespace_id: str,
    source_id: str,
    decision: str,
) -> dict[str, Any]:
    """Shared implementation for ApproveSource / RejectSource.

    Action endpoints — no body is consumed. Returns 202 with the new
    transient status, leaving the actual DataZone writes to the worker.
    """
    transient, failed, allowed_entry = _bulk_lifecycle(decision)

    _, err = _transition_to_transient(namespace_id, source_id, transient, allowed_entry)
    if err:
        return err

    err = _enqueue_bulk_review(namespace_id, source_id, decision)
    if err:
        # Best-effort rollback: if SQS enqueue failed, restore status so the user
        # can retry. We don't fail-fast on rollback errors; the worker has its
        # own status check and the source can be manually un-stuck.
        try:
            _get_dao().update(
                {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
                {"status": failed},
                raise_on_error=False,
            )
        except Exception:
            logger.exception("rollback_to_failed_state_failed", source_id=source_id)
        return err

    return api_response(202, {"sourceId": source_id, "status": transient})


def _handle_approve_source(event: dict[str, Any], namespace_id: str, source_id: str) -> dict[str, Any]:
    """POST /namespaces/{namespaceId}/sources/{sourceId}/approve (202)."""
    from coa_control_plane_server.models.review_decision import ReviewDecision

    return _handle_bulk_review(event, namespace_id, source_id, ReviewDecision.APPROVED)


def _handle_reject_source(event: dict[str, Any], namespace_id: str, source_id: str) -> dict[str, Any]:
    """POST /namespaces/{namespaceId}/sources/{sourceId}/reject (202)."""
    from coa_control_plane_server.models.review_decision import ReviewDecision

    return _handle_bulk_review(event, namespace_id, source_id, ReviewDecision.REJECTED)


# ---------------------------------------------------------------------------
# GET SCAN JOB
# ---------------------------------------------------------------------------


def _handle_get_scan_job(namespace_id: str, source_id: str, job_id: str) -> dict[str, Any]:
    """GET /namespaces/{namespaceId}/sources/{sourceId}/scan/{jobId}.

    Reads from source-scan-jobs table.
    PK = SRC#{sourceId}, SK = <ISO timestamp stored at job creation>
    The jobId path param is the ISO timestamp SK.
    """
    job_id = unquote(job_id)
    try:
        source_item = _get_dao().get({"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"})
    except ClientError:
        logger.exception("ddb_get_failed", source_id=source_id)
        return api_response(500, {"error": "Internal server error"})

    if not source_item:
        return api_response(404, {"error": f"Source '{source_id}' not found"})

    try:
        scan_item = _get_scan_dao().get({"PK": f"SRC#{source_id}", "SK": job_id})
    except ClientError:
        logger.exception("ddb_get_scan_job_failed", source_id=source_id, job_id=job_id)
        return api_response(500, {"error": "Internal server error"})

    if not scan_item:
        return api_response(404, {"error": f"Scan job '{job_id}' not found"})

    response: dict[str, Any] = {
        "scanJobId": job_id,
        "sourceId": source_id,
        "status": scan_item.get("status"),
        "scanType": scan_item.get("scanType"),
        # Coerced to int, because this response is assembled as a raw dict with no
        # Smithy model in the path to do it for us. The DAO reads through boto3's
        # resource interface, so a DynamoDB number arrives as a Decimal, and
        # api_response serialises with `json.dumps(..., default=str)` — which turns
        # Decimal("2") into the JSON STRING "2". A typed client then deserialises a
        # numeric member from a string and hands the consumer something that fails
        # `typeof x === "number"`. Every other numeric field on this response has
        # always had that defect; it was invisible only because nothing type-checked
        # them. See _item_to_summary, which gets this right via the same int() call.
        "tablesDiscovered": _optional_int(scan_item.get("tablesDiscovered")),
        "columnsDiscovered": _optional_int(scan_item.get("columnsDiscovered")),
        "startedAt": iso_to_epoch(scan_item.get("startedAt")),
        "completedAt": iso_to_epoch(scan_item.get("completedAt")),
        "errorMessage": scan_item.get("errorMessage"),
        # A scan can SUCCEED while individual tables were unreadable, leaving those
        # tables with no columns, no comments, and no declared keys — and enrichment
        # then generates descriptions over the gap. Unsurfaced, the result is
        # indistinguishable from a complete one, so a reviewer would approve a
        # silently incomplete ontology. Both keys drop out below when absent, so a
        # clean scan's response is unchanged.
        "tablesFailed": _optional_int(scan_item.get("tablesFailed")),
        "failedTables": scan_item.get("failedTables"),
    }
    return api_response(200, {k: v for k, v in response.items() if v is not None})


# ---------------------------------------------------------------------------
# UPDATE METADATA
# ---------------------------------------------------------------------------


def _validate_configuration_blob(config_key: str, blob: Any) -> dict[str, Any] | None:
    """Validate a configuration blob against its Smithy model.

    Returns an error response, or ``None`` when the blob is valid. Validating here
    rather than trusting the caller is what keeps a stored blob loadable by the GET
    path, which reconstructs it through the same model.
    """
    models = {
        "glueConfiguration": GlueConfiguration,
        "jdbcConfiguration": JdbcConfiguration,
        "customConnectorConfiguration": CustomConnectorConfiguration,
    }
    try:
        models[config_key].model_validate(blob)
    except PydanticValidationError as exc:
        return api_response(400, {"error": f"Invalid {config_key}: {exc.errors()[0]['msg']}"})
    return None


def _apply_custom_connector_configuration_update(
    item: dict[str, Any],
    new_config: dict[str, Any],
    update_fields: dict[str, Any],
) -> dict[str, Any] | None:
    """Guard and extend an ``customConnectorConfiguration`` update.

    Two things in this blob are not free to change, because the stored blob is not
    the only place their value lives:

    * **The connector Lambda ARNs.** They are baked into the Athena data catalog's
      ``Parameters`` at source-create, and nothing re-registers it afterwards — the
      post-discovery step only flips ``queryable``. Accepting a new ARN would
      return 200, report the new ARN on every read path, and leave the catalog
      still invoking the OLD Lambda. Discovery and serve would then keep answering
      from the previous connector with no error anywhere, which is the worst shape
      a wrong answer can take: every surface agrees, and all of them are wrong.
      Rejected rather than re-bound, because re-binding is delete-then-create with
      no transaction around it — a failure between the two would leave the source
      with no catalog at all, which is worse than refusing the edit.
    * **``databaseName``.** Serve does not read ``athenaDatabase`` first — it prefers
      ``discoveredSchemas[0]`` and falls back to ``athenaDatabase`` only when that
      list is empty (``coa_serve.clients.athena._resolve_catalog_and_database``).
      Discovery writes ``discoveredSchemas`` on every successful scan, so for a
      scanned source the fallback is dead and writing ``athenaDatabase`` retargets
      nothing: the update returns 200 while serve keeps querying the old database.
      A re-scan cannot reconcile it either — ``_handle_rescan`` 409s any ``DATABASE``
      source that is not ``SCAN_FAILED``.
      Rejected rather than made to work, because the stored tables, their approved
      metadata and the induced ontology all describe the OLD database. Retargeting
      the query would leave every one of them describing something this source no
      longer points at.

    Both are rejected only when the value CHANGES; an update echoing the stored value
    is allowed, so a client may PUT the whole configuration back to edit another field
    in it. The members stay in the request shape because ``CustomConnectorConfiguration`` is
    shared with create, where they are required — a member cannot be dropped for one
    operation only.

    Returns an error response, or ``None`` when the update may proceed.
    """
    try:
        stored = json.loads(item.get("configuration") or "{}")
    except (json.JSONDecodeError, TypeError):
        stored = {}

    for field in ("connectorFunctionArn",):
        stored_arn = stored.get(field) or None
        new_arn = new_config.get(field) or None
        if stored_arn != new_arn:
            return api_response(
                400,
                {
                    "error": (
                        f"customConnectorConfiguration.{field} cannot be changed after creation: the connector "
                        f"Lambda is bound into the registered Athena data catalog, which this update does not "
                        f"re-register. Delete the source and create it again with the new ARN."
                    )
                },
            )

    # Compare against the blob as well as the top-level attribute: a source created
    # before athenaDatabase was mirrored, or one whose mirror write was lost, would
    # otherwise read as "changed" for an unchanged value and reject a valid edit.
    stored_database = stored.get("databaseName") or item.get("athenaDatabase") or None
    new_database = new_config.get("databaseName") or None
    if new_database and stored_database and new_database != stored_database:
        return api_response(
            400,
            {
                "error": (
                    "customConnectorConfiguration.databaseName cannot be changed after creation: the discovered "
                    "tables, their approved metadata and the induced ontology all describe the current database, "
                    "and this update does not re-discover them. Delete the source and create it again against "
                    f"{new_database!r}."
                )
            },
        )
    # Still mirror the value when the attribute is absent, so a source whose
    # athenaDatabase was never written gets one without changing where it points.
    if new_database and not item.get("athenaDatabase"):
        update_fields["athenaDatabase"] = new_database
    return None


def _handle_update_metadata(event: dict[str, Any], namespace_id: str, source_id: str) -> dict[str, Any]:
    """PUT /namespaces/{namespaceId}/sources/{sourceId}/metadata.

    Updates mutable fields on a DATABASE source: name, configuration,
    metadataEnrichmentEnabled.
    """
    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return api_response(400, {"error": "Invalid JSON body"})

    if not body:
        return api_response(400, {"error": "Request body must not be empty"})

    try:
        item = _get_dao().get({"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"})
    except ClientError:
        logger.exception("ddb_get_failed", source_id=source_id)
        return api_response(500, {"error": "Internal server error"})

    if not item:
        return api_response(404, {"error": f"Source '{source_id}' not found"})

    if item.get("sourceType") != SourceType.DATABASE:
        return api_response(400, {"error": "Metadata update is only supported for DATABASE sources"})

    update_fields: dict[str, Any] = {}

    if "name" in body and body["name"] is not None:
        name = str(body["name"]).strip()
        if not name:
            return api_response(400, {"error": "name must not be blank"})
        update_fields["name"] = name

    if "metadataEnrichmentEnabled" in body and body["metadataEnrichmentEnabled"] is not None:
        update_fields["metadataEnrichmentEnabled"] = bool(body["metadataEnrichmentEnabled"])

    # The stored `configuration` blob is untyped, so sourceSubType is the only
    # thing that says which shape it holds — which means an update must write the
    # shape the row's own sub-type declares. Writing a Glue config onto an
    # CUSTOM_CONNECTOR row (or vice versa) leaves a record whose blob and sub-type
    # disagree, and GET then 500s on the mismatched required members rather than
    # mislabelling anything.
    config_key_for_sub_type = {
        SourceSubType.GLUE_DATABASE.value: "glueConfiguration",
        SourceSubType.JDBC_DATABASE.value: "jdbcConfiguration",
        SourceSubType.CUSTOM_CONNECTOR.value: "customConnectorConfiguration",
    }
    supplied_config_keys = [k for k in config_key_for_sub_type.values() if body.get(k)]
    if len(supplied_config_keys) > 1:
        return api_response(
            400,
            {"error": f"Cannot update more than one configuration at a time: {', '.join(supplied_config_keys)}"},
        )
    if supplied_config_keys:
        supplied_key = supplied_config_keys[0]
        stored_sub_type = item.get("sourceSubType", "")
        expected_key = config_key_for_sub_type.get(stored_sub_type)
        if expected_key is None and stored_sub_type:
            # Recognised-but-unmapped, or an unknown value: there is no shape to
            # check against, and guessing would be how the blob and the sub-type
            # come apart.
            return api_response(
                400,
                {"error": f"Configuration update is not supported for sub-type '{stored_sub_type}'"},
            )
        if expected_key is None:
            # No sub-type at all — a legacy row. It already fails GET validation on
            # that member alone, so refusing the update here would only remove a
            # way to work with it. Accept whatever was supplied, as before.
            logger.warning("update_configuration_without_sub_type", source_id=source_id, config_key=supplied_key)
            expected_key = supplied_key
        if supplied_key != expected_key:
            return api_response(
                400,
                {
                    "error": (
                        f"This source is a {item['sourceSubType']} source, so its configuration must be "
                        f"supplied as '{expected_key}', not '{supplied_key}'."
                    )
                },
            )
        error = _validate_configuration_blob(supplied_key, body[supplied_key])
        if error:
            return error
        # Re-run the namespace binding on an update, so a JDBC source's
        # credentialSecretArn cannot be repointed at a secret bound to a
        # different namespace after creation (the create-time check alone would
        # leave this path open).
        if supplied_key == "jdbcConfiguration":
            error = _validate_credential_secret_binding(
                JdbcConfiguration.model_validate(body[supplied_key]), namespace_id
            )
            if error:
                return error
        if supplied_key == "customConnectorConfiguration":
            error = _apply_custom_connector_configuration_update(item, body[supplied_key], update_fields)
            if error:
                return error
        update_fields["configuration"] = json.dumps(_strip_external_id(body[supplied_key], item.get("configuration")))

    if not update_fields:
        return api_response(400, {"error": "No updatable fields provided"})

    try:
        _get_dao().update(
            {"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"},
            update_fields,
        )
    except ClientError:
        logger.exception("ddb_update_failed", source_id=source_id)
        return api_response(500, {"error": "Internal server error"})

    updated = _get_dao().get({"PK": f"NS#{namespace_id}", "SK": f"SRC#{source_id}"})
    return api_response(
        200,
        {
            "sourceId": source_id,
            "status": (updated or item).get("status"),
            "updatedAt": iso_to_epoch((updated or item).get("updatedAt")),
        },
    )
