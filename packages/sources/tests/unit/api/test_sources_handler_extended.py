# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extended unit tests for sources_handler.py — GET, DELETE, RESCAN, CREATE routes.

Supplements test_sources_handler.py which covers LIST.
"""

from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from coa_common.constants import RESOURCE_PREFIX
from coa_control_plane_server.models.source_type import SourceType

os.environ.setdefault("SOURCES_TABLE", "test-sources")
os.environ.setdefault("SOURCE_SCAN_JOBS_TABLE", "test-scan-jobs")
os.environ.setdefault("NAMESPACES_TABLE", "test-namespaces")
os.environ.setdefault("SCAN_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/scan-queue")
os.environ.setdefault("INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue")
os.environ.setdefault("DELETION_STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine/delete")
os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

_NAMESPACE_ID = "550e8400-e29b-41d4-a716-446655440000"
_SOURCE_ID = "src-001"

import coa_sources.api.sources_handler as _sh  # noqa: E402

_SH = "coa_sources.api.sources_handler"


def _current_sh():
    """Get the current sources_handler module (handles reimports from test_sources_handler.py)."""
    return sys.modules.get("coa_sources.api.sources_handler", _sh)


def _parse(result):
    return result["statusCode"], json.loads(result["body"]) if result.get("body") else {}


def _make_event(method, resource, path_params=None, body=None, qs=None):
    return {
        "httpMethod": method,
        "resource": resource,
        "pathParameters": path_params or {"namespaceId": _NAMESPACE_ID},
        "queryStringParameters": qs or {},
        "body": json.dumps(body) if body else None,
        "requestContext": {},
    }


def _db_source_item(status="APPROVED"):
    return {
        "PK": f"NS#{_NAMESPACE_ID}",
        "SK": f"SRC#{_SOURCE_ID}",
        "sourceId": _SOURCE_ID,
        "namespaceId": _NAMESPACE_ID,
        "name": "my-db",
        "sourceType": "DATABASE",
        "sourceSubType": "GLUE_DATABASE",
        "status": status,
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
    }


def _doc_source_item(status="COMPLETED"):
    return {
        "PK": f"NS#{_NAMESPACE_ID}",
        "SK": f"SRC#{_SOURCE_ID}",
        "sourceId": _SOURCE_ID,
        "namespaceId": _NAMESPACE_ID,
        "name": "my-docs",
        "sourceType": "DOCUMENTS",
        "sourceSubType": "LOCAL_UPLOAD",
        "docSourceType": "upload",
        "status": status,
        "tenantId": "tenant-123",
        "s3Prefixes": ["uploads/"],
        "extractionConfig": {},
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
    }


@pytest.fixture(autouse=True)
def reset_lazy_clients():
    """Reset lazy clients on the current module instance (handles reimports from test_sources_handler.py)."""
    import coa_sources.api.namespace_counters as nc

    sh = sys.modules.get("coa_sources.api.sources_handler", _sh)
    sh._dao = None
    sh._sqs = None
    sh._s3 = None
    sh._sfn = None
    sh._scan_dao = None
    sh._ns_dao = None
    nc._ns_dao = None
    with patch("coa_sources.api.sources_handler.adjust_namespace_source_count"):
        yield
    sh = sys.modules.get("coa_sources.api.sources_handler", _sh)
    sh._dao = None
    sh._sqs = None
    sh._s3 = None
    sh._sfn = None
    sh._scan_dao = None
    sh._ns_dao = None
    nc._ns_dao = None


# ===================================================================
# _handle_get
# ===================================================================


@pytest.mark.unit
class TestHandleGet:
    def test_get_source_happy_path_database(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item()

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_get(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["sourceId"] == _SOURCE_ID
        assert body["sourceType"] == "DATABASE"

    def test_get_source_happy_path_documents(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item()

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_get(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["sourceType"] == "DOCUMENTS"

    def test_get_source_not_found_returns_404(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = None

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_get(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 404
        assert _SOURCE_ID in body["error"]

    def test_get_source_ddb_error_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "GetItem")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_get(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500


# ===================================================================
# _handle_delete
# ===================================================================


@pytest.mark.unit
class TestHandleDelete:
    @pytest.fixture(autouse=True)
    def _mock_database_cleanup_helpers(self):
        """Mock the per-source cleanup helpers by default so tests don't make
        real AWS calls. Tests that need to assert specific behavior on these
        helpers should patch them again locally to override.
        """
        with (
            patch(f"{_SH}._delete_source_datazone_assets", return_value=0) as mock_assets,
            patch(f"{_SH}._delete_source_scan_jobs", return_value=0) as mock_scans,
        ):
            yield mock_assets, mock_scans

    def test_delete_database_source_happy_path(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["status"] == "DELETED"
        mock_dao.delete.assert_called_once()

    def test_delete_database_source_cleans_up_athena_federation(self):
        """If a DATABASE source has Glue Connection / Athena catalog refs,
        they must be cleaned up before the DDB row is deleted so we don't
        leave orphaned cloud resources."""
        item = _db_source_item("APPROVED")
        item["glueConnectionName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"
        item["athenaDataCatalogName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {"AccessKeyId": "k", "SecretAccessKey": "s", "SessionToken": "t"}
        }

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
            patch(f"{_SH}.boto3.Session") as mock_session,
            patch(f"{_SH}.cleanup_federated_resources") as mock_cleanup,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_sts.assume_role.assert_called_once()
        assert mock_sts.assume_role.call_args.kwargs["RoleArn"] == "arn:aws:iam::123:role/fed"
        mock_cleanup.assert_called_once_with(
            glue_connection_name=f"{RESOURCE_PREFIX}-dev-ds-abc",
            athena_catalog_name=f"{RESOURCE_PREFIX}-dev-ds-abc",
            session=mock_session.return_value,
        )
        mock_dao.delete.assert_called_once()

    def test_delete_database_source_skips_athena_cleanup_when_no_refs(self):
        """Sources without Athena federation (S3/Iceberg, or never scanned)
        should not assume the role or run cleanup at all."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")  # no glueConnectionName
        mock_sts = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_sts.assume_role.assert_not_called()
        mock_dao.delete.assert_called_once()

    def test_delete_database_source_blocks_when_athena_cleanup_fails(self):
        """Federation cleanup failures must block the DDB delete (HTTP 500) so
        the source row remains and the delete can be retried — federated
        catalogs/connections are billable and have no automatic recovery path."""
        item = _db_source_item("APPROVED")
        item["glueConnectionName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = RuntimeError("AssumeRole denied")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500
        mock_dao.delete.assert_not_called()

    def test_delete_database_source_blocks_when_assume_role_returns_no_credentials(self):
        """A malformed AssumeRole response (missing Credentials) must be treated
        as a cleanup failure and block the delete rather than crashing."""
        item = _db_source_item("APPROVED")
        item["glueConnectionName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {}  # no Credentials

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500
        mock_dao.delete.assert_not_called()

    def test_delete_athena_connector_source_removes_the_data_catalog(self):
        """A custom-connector source owns a top-level LAMBDA-type Athena data
        catalog. That is a plain athena:DeleteDataCatalog on this role — no Glue
        object and no Lake Formation grants, so nothing to assume the federation
        provisioner's admin role for."""
        item = _db_source_item("APPROVED")
        item["sourceSubType"] = "ATHENA_CONNECTOR"
        item["athenaDataCatalogName"] = f"{RESOURCE_PREFIX}devds_abc123"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
            patch(f"{_SH}.delete_lambda_catalog") as mock_delete_catalog,
            patch(f"{_SH}.cleanup_federated_resources") as mock_cleanup,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_delete_catalog.assert_called_once_with(catalog_name=f"{RESOURCE_PREFIX}devds_abc123")
        # A Lambda catalog also populates athenaDataCatalogName, so without the
        # sub-type gate it would match the federated-teardown block, assume the
        # LF-admin role, and call glue.delete_catalog — a no-op for a Lambda
        # catalog — reporting success while leaking the registration.
        mock_cleanup.assert_not_called()
        mock_sts.assume_role.assert_not_called()
        mock_dao.delete.assert_called_once()

    def test_delete_athena_connector_source_derives_the_catalog_name_when_absent(self):
        """Defence in depth. The attribute is written in the same put as the row,
        so no row this service creates can lack it — but the name is derivable from
        the source id alone, so a hand-written or migrated row is still cleaned up
        rather than silently leaving its catalog behind."""
        item = _db_source_item("APPROVED")
        item["sourceSubType"] = "ATHENA_CONNECTOR"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}.delete_lambda_catalog") as mock_delete_catalog,
            patch(f"{_SH}.derive_catalog_name", return_value="derived-name") as mock_derive,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_derive.assert_called_once_with(_SOURCE_ID)
        mock_delete_catalog.assert_called_once_with(catalog_name="derived-name")

    def test_delete_athena_connector_source_blocks_when_catalog_delete_fails(self):
        """The source row is the only handle on the catalog, so dropping the row
        after a failed teardown orphans it permanently."""
        from coa_sources.database.connectors.athena_catalog import AthenaCatalogError

        item = _db_source_item("APPROVED")
        item["sourceSubType"] = "ATHENA_CONNECTOR"
        item["athenaDataCatalogName"] = f"{RESOURCE_PREFIX}devds_abc123"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}.delete_lambda_catalog", side_effect=AthenaCatalogError("denied")),
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500
        mock_dao.delete.assert_not_called()

    def test_delete_jdbc_source_still_runs_federated_teardown(self):
        """The sub-type gate must not divert a real federated JDBC source away
        from the Glue/Lake Formation teardown it does need."""
        item = _db_source_item("APPROVED")
        item["sourceSubType"] = "JDBC_DATABASE"
        item["glueConnectionName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"
        item["athenaDataCatalogName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {"AccessKeyId": "k", "SecretAccessKey": "s", "SessionToken": "t"}
        }

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
            patch(f"{_SH}.boto3.Session"),
            patch(f"{_SH}.delete_lambda_catalog") as mock_delete_catalog,
            patch(f"{_SH}.cleanup_federated_resources") as mock_cleanup,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_cleanup.assert_called_once()
        mock_delete_catalog.assert_not_called()

    def test_delete_database_source_cleans_up_datazone_assets_and_scan_jobs(self):
        """DATABASE delete must invoke DataZone asset cleanup and scan-job
        cleanup before deleting the sources-table row."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._delete_source_datazone_assets", return_value=3) as mock_assets,
            patch(f"{_SH}._delete_source_scan_jobs", return_value=2) as mock_scans,
        ):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["status"] == "DELETED"
        mock_assets.assert_called_once_with(_NAMESPACE_ID, _SOURCE_ID)
        mock_scans.assert_called_once_with(_SOURCE_ID)
        mock_dao.delete.assert_called_once()

    def test_delete_database_source_proceeds_when_datazone_cleanup_fails(self):
        """DataZone asset cleanup failures are logged but must not block
        the DDB delete — they are best-effort and the namespace-level
        cleanup will sweep any leftovers."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(
                f"{_SH}._delete_source_datazone_assets",
                side_effect=RuntimeError("DataZone throttled"),
            ),
            patch(f"{_SH}._delete_source_scan_jobs", return_value=0),
        ):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["status"] == "DELETED"
        mock_dao.delete.assert_called_once()

    def test_delete_database_source_proceeds_when_scan_job_cleanup_fails(self):
        """Scan-job cleanup failures are logged but must not block the DDB
        delete; the rows are cleaned up by namespace-level deletion."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._delete_source_datazone_assets", return_value=0),
            patch(
                f"{_SH}._delete_source_scan_jobs",
                side_effect=RuntimeError("DDB throttled"),
            ),
        ):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        assert body["status"] == "DELETED"
        mock_dao.delete.assert_called_once()

    def test_delete_document_source_happy_path(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("COMPLETED")
        mock_sfn = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sfn", return_value=mock_sfn),
            patch(f"{_SH}._DELETION_STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine/delete"),
        ):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 202
        assert body["status"] == "DELETING"
        mock_sfn.start_execution.assert_called_once()

    def test_delete_source_not_found_returns_404(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = None

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 404

    def test_delete_document_already_deleting_returns_202(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("DELETING")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 202
        assert body["status"] == "DELETING"

    def test_delete_document_active_status_returns_409(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("REGISTERED")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 409

    def test_delete_database_active_status_returns_409(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("SCANNING")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 409

    def test_delete_ddb_error_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "GetItem")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500

    def test_delete_document_sfn_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("COMPLETED")
        mock_sfn = MagicMock()
        mock_sfn.start_execution.side_effect = ClientError({"Error": {"Code": "SFNError"}}, "StartExecution")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sfn", return_value=mock_sfn),
            patch(f"{_SH}._DELETION_STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine/delete"),
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500

    def test_delete_database_ddb_delete_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")
        mock_dao.delete.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "DeleteItem")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500


# ===================================================================
# _delete_source_datazone_assets and _delete_source_scan_jobs helpers
# ===================================================================


@pytest.mark.unit
class TestDeleteSourceDatazoneAssets:
    def test_returns_zero_when_no_smus_domain(self):
        with patch(f"{_SH}._SMUS_DOMAIN_ID", ""):
            result = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)
        assert result == 0

    def test_returns_zero_when_namespace_has_no_project_id(self):
        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value=None,
            ),
        ):
            result = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)
        assert result == 0

    def test_deletes_only_assets_with_matching_prefix(self):
        """search_assets is fuzzy; only assets whose name starts with
        ``DS#{sourceId}:`` should be deleted. A foreign asset that happens
        to surface in the search results must be skipped."""
        ds_key = f"DS#{_SOURCE_ID}"

        # MagicMock(name=...) sets the mock's repr name, not the .name attr —
        # we have to assign .name explicitly for the prefix check to see it.
        match_a = MagicMock(asset_id="a1")
        match_a.name = f"{ds_key}:public.users"
        match_b = MagicMock(asset_id="b2")
        match_b.name = f"{ds_key}:public.orders"
        unrelated = MagicMock(asset_id="x9")
        unrelated.name = "DS#other:public.invoices"

        page_one = MagicMock(items=[match_a, unrelated], next_token="tok-2")
        page_two = MagicMock(items=[match_b], next_token=None)

        mock_client = MagicMock()
        mock_client.search_assets.side_effect = [page_one, page_two]

        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value="proj-1",
            ),
            patch(
                "coa_sources.api.database_routes._get_smus_client",
                return_value=mock_client,
            ),
        ):
            removed = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)

        assert removed == 2
        assert mock_client.search_assets.call_count == 2
        deleted_ids = [c.kwargs["asset_id"] for c in mock_client.delete_asset.call_args_list]
        assert deleted_ids == ["a1", "b2"]

    def test_continues_when_individual_delete_fails(self):
        """A single asset delete failure must not prevent later deletes —
        we want maximum cleanup on a best-effort basis."""
        ds_key = f"DS#{_SOURCE_ID}"
        a = MagicMock(asset_id="a1")
        a.name = f"{ds_key}:t1"
        b = MagicMock(asset_id="b2")
        b.name = f"{ds_key}:t2"

        mock_client = MagicMock()
        mock_client.search_assets.return_value = MagicMock(items=[a, b], next_token=None)
        mock_client.delete_asset.side_effect = [RuntimeError("boom"), None]

        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value="proj-1",
            ),
            patch(
                "coa_sources.api.database_routes._get_smus_client",
                return_value=mock_client,
            ),
        ):
            removed = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)

        # b succeeded, a failed → 1 removed
        assert removed == 1
        assert mock_client.delete_asset.call_count == 2

    def test_collects_all_pages_before_deleting(self):
        """Regression: deleting assets must not perturb search pagination.

        DataZone search pagination is over a live result set; if the function
        deletes assets while iterating, the deletions shrink that set and the
        next-page token skips past the remaining matches (observed: 75 assets,
        page size 50 → only 50 deleted, 25 orphaned). This models a paginator
        that ONLY yields the second page while no delete has occurred yet —
        i.e. all search_assets calls must complete before any delete_asset.
        """
        ds_key = f"DS#{_SOURCE_ID}"

        def _mk(n):
            m = MagicMock(asset_id=n)
            m.name = f"{ds_key}:{n}"
            return m

        page_one = MagicMock(items=[_mk(f"p1-{i}") for i in range(50)], next_token="tok-2")
        page_two = MagicMock(items=[_mk(f"p2-{i}") for i in range(25)], next_token=None)

        mock_client = MagicMock()
        search_calls = {"n": 0}

        def _search(**_kwargs):
            # Fail loudly if a delete happened before pagination finished:
            # collect-then-delete guarantees zero deletes during search.
            assert mock_client.delete_asset.call_count == 0, (
                "delete_asset was called before pagination completed — "
                "assets are being deleted mid-search (the pagination bug)."
            )
            search_calls["n"] += 1
            return page_one if search_calls["n"] == 1 else page_two

        mock_client.search_assets.side_effect = _search

        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value="proj-1",
            ),
            patch(
                "coa_sources.api.database_routes._get_smus_client",
                return_value=mock_client,
            ),
        ):
            removed = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)

        # All 75 across both pages must be deleted — none orphaned.
        assert removed == 75
        assert mock_client.search_assets.call_count == 2
        assert mock_client.delete_asset.call_count == 75

    def test_delete_phase_stops_at_wall_clock_budget(self):
        """The synchronous delete loop must bail out once the time budget is spent.

        Pagination (read-only) always completes; only the delete phase is
        budget-bound. With the budget pinned to 0, the deadline is already past
        on entry to the delete loop, so zero assets are deleted and the function
        returns cleanly (the namespace-deletion sweep finishes the rest) rather
        than running thousands of serial deletes past the Lambda timeout.
        """
        ds_key = f"DS#{_SOURCE_ID}"
        items = []
        for i in range(10):
            m = MagicMock(asset_id=f"a{i}")
            m.name = f"{ds_key}:t{i}"
            items.append(m)

        mock_client = MagicMock()
        mock_client.search_assets.return_value = MagicMock(items=items, next_token=None)

        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(f"{_SH}._DATAZONE_CLEANUP_BUDGET_S", 0),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value="proj-1",
            ),
            patch(
                "coa_sources.api.database_routes._get_smus_client",
                return_value=mock_client,
            ),
        ):
            removed = _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)

        # Pagination still ran; the delete loop exited on the spent budget.
        assert removed == 0
        assert mock_client.search_assets.call_count == 1
        assert mock_client.delete_asset.call_count == 0

    @staticmethod
    def _run_with_client(mock_client):
        """Invoke _delete_source_datazone_assets with the SMUS client patched."""
        with (
            patch(f"{_SH}._SMUS_DOMAIN_ID", "dz-domain-1"),
            patch(
                "coa_sources.api.database_routes._resolve_project_id",
                return_value="proj-1",
            ),
            patch(
                "coa_sources.api.database_routes._get_smus_client",
                return_value=mock_client,
            ),
        ):
            return _current_sh()._delete_source_datazone_assets(_NAMESPACE_ID, _SOURCE_ID)

    def test_empty_search_results(self):
        """No matching assets → one search call, zero deletes, returns 0."""
        mock_client = MagicMock()
        mock_client.search_assets.return_value = MagicMock(items=[], next_token=None)

        removed = self._run_with_client(mock_client)

        assert removed == 0
        assert mock_client.search_assets.call_count == 1
        assert mock_client.delete_asset.call_count == 0

    def test_single_page_results(self):
        """A single page (next_token=None on first call) deletes all matches and stops."""
        ds_key = f"DS#{_SOURCE_ID}"
        items = []
        for i in range(3):
            m = MagicMock(asset_id=f"a{i}")
            m.name = f"{ds_key}:t{i}"
            items.append(m)
        mock_client = MagicMock()
        mock_client.search_assets.return_value = MagicMock(items=items, next_token=None)

        removed = self._run_with_client(mock_client)

        assert removed == 3
        assert mock_client.search_assets.call_count == 1
        assert mock_client.delete_asset.call_count == 3

    def test_pagination_limit_hit_stops_at_max_pages(self):
        """A paginator that never returns a falsy next_token stops at max_pages (100).

        Every page yields a fresh token, so the loop is bounded only by the
        max_pages guard. Assets collected across those pages are still deleted.
        """
        ds_key = f"DS#{_SOURCE_ID}"

        def _page(**_kwargs):
            m = MagicMock(asset_id="dup")
            m.name = f"{ds_key}:t"
            # Always returns a non-empty token → next_token never falsy.
            return MagicMock(items=[m], next_token="more")

        mock_client = MagicMock()
        mock_client.search_assets.side_effect = _page

        removed = self._run_with_client(mock_client)

        # Bounded by the 100-page guard (the loop's else-branch logs the limit).
        assert mock_client.search_assets.call_count == 100
        # 100 collected asset ids → 100 delete attempts.
        assert mock_client.delete_asset.call_count == 100
        assert removed == 100

    def test_search_assets_exception_propagates(self):
        """An exception from search_assets (collection phase) is not swallowed.

        Only per-asset delete failures are best-effort; a failure to enumerate
        the assets must surface so the caller (best-effort in _handle_delete)
        logs it rather than silently reporting a clean cleanup.
        """
        mock_client = MagicMock()
        mock_client.search_assets.side_effect = RuntimeError("search boom")

        with pytest.raises(RuntimeError, match="search boom"):
            self._run_with_client(mock_client)

        assert mock_client.delete_asset.call_count == 0


@pytest.mark.unit
class TestDeleteSourceScanJobs:
    def test_paginates_and_batch_deletes_all_rows(self):
        page_one = MagicMock(
            items=[
                {"PK": f"SRC#{_SOURCE_ID}", "SK": "2026-01-01T00:00:00Z"},
                {"PK": f"SRC#{_SOURCE_ID}", "SK": "2026-01-02T00:00:00Z"},
            ],
            last_evaluated_key={"PK": f"SRC#{_SOURCE_ID}", "SK": "2026-01-02T00:00:00Z"},
        )
        page_two = MagicMock(
            items=[{"PK": f"SRC#{_SOURCE_ID}", "SK": "2026-01-03T00:00:00Z"}],
            last_evaluated_key=None,
        )
        mock_dao = MagicMock()
        mock_dao.query.side_effect = [page_one, page_two]

        with patch(f"{_SH}._get_scan_dao", return_value=mock_dao):
            removed = _current_sh()._delete_source_scan_jobs(_SOURCE_ID)

        assert removed == 3
        assert mock_dao.query.call_count == 2
        mock_dao.batch_delete.assert_called_once()
        deleted_keys = mock_dao.batch_delete.call_args[0][0]
        assert len(deleted_keys) == 3
        assert all(k["PK"] == f"SRC#{_SOURCE_ID}" for k in deleted_keys)

    def test_no_rows_skips_batch_delete(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)

        with patch(f"{_SH}._get_scan_dao", return_value=mock_dao):
            removed = _current_sh()._delete_source_scan_jobs(_SOURCE_ID)

        assert removed == 0
        mock_dao.batch_delete.assert_not_called()


# ===================================================================
# _handle_delete — namespace sourceCount maintenance
# ===================================================================


@pytest.mark.unit
class TestHandleDeleteCounter:
    """Deleting a source of any type must decrement the namespace
    ``sourceCount`` exactly once, and must not touch the counter when the
    delete is a no-op (source missing, or a document delete that is already
    in progress).

    The autouse ``reset_lazy_clients`` fixture patches
    ``adjust_namespace_source_count`` globally; each test re-patches it with a
    local handle so the call can be asserted.
    """

    @pytest.fixture(autouse=True)
    def _mock_database_cleanup_helpers(self):
        with (
            patch(f"{_SH}._delete_source_datazone_assets", return_value=0),
            patch(f"{_SH}._delete_source_scan_jobs", return_value=0),
        ):
            yield

    def test_delete_database_source_decrements_count(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_dao.delete.assert_called_once()
        mock_counter.assert_called_once_with(_NAMESPACE_ID, SourceType.DATABASE, -1)

    def test_delete_document_source_decrements_count(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("COMPLETED")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sfn", return_value=MagicMock()),
            patch(f"{_SH}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 202
        mock_counter.assert_called_once_with(_NAMESPACE_ID, SourceType.DOCUMENTS, -1)

    def test_delete_missing_source_does_not_touch_count(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = None

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 404
        mock_counter.assert_not_called()

    def test_delete_document_already_deleting_does_not_double_decrement(self):
        """A document source already transitioning to DELETING returns 202
        early without decrementing again, so concurrent deletes can't drive
        the counter below the true total."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("DELETING")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sfn", return_value=MagicMock()),
            patch(f"{_SH}.adjust_namespace_source_count") as mock_counter,
        ):
            status, body = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 202
        assert body["status"] == "DELETING"
        mock_counter.assert_not_called()

    def test_delete_database_failed_federation_cleanup_does_not_decrement(self):
        """When federation teardown fails the DDB row is intentionally left in
        place (HTTP 500) so the delete can be retried; the counter must not be
        decremented for a source that still exists."""
        item = _db_source_item("APPROVED")
        item["glueConnectionName"] = f"{RESOURCE_PREFIX}-dev-ds-abc"

        mock_dao = MagicMock()
        mock_dao.get.return_value = item
        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = RuntimeError("AssumeRole denied")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._FEDERATION_PROVISIONER_ROLE_ARN", "arn:aws:iam::123:role/fed"),
            patch(f"{_SH}._get_sts", return_value=mock_sts),
            patch(f"{_SH}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_current_sh()._handle_delete(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500
        mock_dao.delete.assert_not_called()
        mock_counter.assert_not_called()


# ===================================================================
# _handle_rescan
# ===================================================================


@pytest.mark.unit
class TestHandleRescan:
    def test_rescan_database_source_happy_path(self):
        mock_dao = MagicMock()
        # Only SCAN_FAILED is allowed: re-scan is a recovery action, not a
        # generic re-trigger. Any other entry status should be rejected.
        mock_dao.get.return_value = _db_source_item("SCAN_FAILED")
        mock_scan_dao = MagicMock()
        mock_sqs = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_scan_dao", return_value=mock_scan_dao),
            patch(f"{_SH}._get_sqs", return_value=mock_sqs),
            patch(f"{_SH}._SCAN_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/scan-queue"),
        ):
            status, body = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 202
        assert body["status"] == "IN_PROGRESS"
        mock_sqs.send_message.assert_called_once()

    def test_rescan_document_source_happy_path(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("SCAN_FAILED")
        mock_sqs = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sqs", return_value=mock_sqs),
            patch(f"{_SH}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, body = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_sqs.send_message.assert_called_once()

    def test_rescan_source_not_found_returns_404(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = None

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 404

    @pytest.mark.parametrize(
        "status",
        [
            "REGISTERED",
            "SCANNING",
            "ENRICHING",
            "PENDING_REVIEW",
            "APPROVING",
            "REJECTING",
            "APPROVED",
            "COMPLETED",
            "DELETING",
        ],
    )
    def test_rescan_database_rejects_non_scan_failed_status(self, status):
        # Schema-drift re-scans aren't supported yet; the API must reject
        # any entry status other than SCAN_FAILED to prevent destructive
        # re-enrichment of already-approved tables.
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item(status)

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            resp_status, body = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert resp_status == 409
        assert "SCAN_FAILED" in body["error"]

    def test_rescan_document_completed_is_allowed(self):
        """Documents can re-scan from COMPLETED (re-ingest) unlike DATABASE sources."""
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("COMPLETED")
        mock_sqs = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sqs", return_value=mock_sqs),
            patch(f"{_SH}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, body = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 200
        mock_sqs.send_message.assert_called_once()

    @pytest.mark.parametrize(
        "status",
        ["REGISTERED", "SCANNING"],
    )
    def test_rescan_document_rejects_active_statuses(self, status):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item(status)

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            resp_status, body = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert resp_status == 409

    def test_rescan_ddb_error_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "GetItem")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500

    def test_rescan_document_sqs_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.get.return_value = _doc_source_item("SCAN_FAILED")
        mock_sqs = MagicMock()
        mock_sqs.send_message.side_effect = ClientError({"Error": {"Code": "SQSError"}}, "SendMessage")

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_sqs", return_value=mock_sqs),
            patch(f"{_SH}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, _ = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500

    def test_rescan_document_missing_tenant_id_returns_500(self):
        item = _doc_source_item("SCAN_FAILED")
        item.pop("tenantId")

        mock_dao = MagicMock()
        mock_dao.get.return_value = item

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            status, _ = _parse(_current_sh()._handle_rescan(_NAMESPACE_ID, _SOURCE_ID))

        assert status == 500


# ===================================================================
# _handle_create
# ===================================================================


@pytest.mark.unit
class TestHandleCreate:
    def test_create_invalid_json_returns_400(self):
        mock_ns_dao = MagicMock()

        with patch(f"{_SH}._get_ns_dao", return_value=mock_ns_dao):
            event = _make_event("POST", "/namespaces/{namespaceId}/sources", body=None)
            event["body"] = "not-json"
            status, body = _parse(_current_sh()._handle_create(event, _NAMESPACE_ID))

        assert status == 400

    def test_create_namespace_not_found_returns_404(self):
        mock_ns_dao = MagicMock()
        mock_ns_dao.get.return_value = None

        with patch(f"{_SH}._get_ns_dao", return_value=mock_ns_dao):
            event = _make_event(
                "POST",
                "/namespaces/{namespaceId}/sources",
                body={
                    "sourceType": "DATABASE",
                    "databaseSource": {
                        "name": "my-db",
                        "glueConfiguration": {
                            "databaseName": "mydb",
                            "region": "us-east-1",
                            "catalogId": "123456789012",
                        },
                    },
                },
            )
            status, body = _parse(_current_sh()._handle_create(event, _NAMESPACE_ID))

        assert status == 404

    def test_create_database_missing_database_source_returns_400(self):
        mock_ns_dao = MagicMock()
        mock_ns_dao.get.return_value = {"PK": f"NS#{_NAMESPACE_ID}"}

        with patch(f"{_SH}._get_ns_dao", return_value=mock_ns_dao):
            event = _make_event(
                "POST",
                "/namespaces/{namespaceId}/sources",
                body={"sourceType": "DATABASE"},
            )
            status, body = _parse(_current_sh()._handle_create(event, _NAMESPACE_ID))

        assert status == 400

    def test_create_documents_missing_document_source_returns_400(self):
        mock_ns_dao = MagicMock()
        mock_ns_dao.get.return_value = {"PK": f"NS#{_NAMESPACE_ID}"}

        with patch(f"{_SH}._get_ns_dao", return_value=mock_ns_dao):
            event = _make_event(
                "POST",
                "/namespaces/{namespaceId}/sources",
                body={"sourceType": "DOCUMENTS"},
            )
            status, body = _parse(_current_sh()._handle_create(event, _NAMESPACE_ID))

        assert status == 400


# ===================================================================
# handler (Lambda entry point) routing
# ===================================================================


@pytest.mark.unit
class TestHandlerRouting:
    def test_handler_routes_get_source(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item()

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            event = _make_event(
                "GET",
                "/namespaces/{namespaceId}/sources/{sourceId}",
                path_params={"namespaceId": _NAMESPACE_ID, "sourceId": _SOURCE_ID},
            )
            status, body = _parse(_current_sh().handler(event, None))

        assert status == 200

    def test_handler_routes_delete_source(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item("APPROVED")

        with patch(f"{_SH}._get_dao", return_value=mock_dao):
            event = _make_event(
                "DELETE",
                "/namespaces/{namespaceId}/sources/{sourceId}",
                path_params={"namespaceId": _NAMESPACE_ID, "sourceId": _SOURCE_ID},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200

    def test_handler_routes_rescan(self):
        mock_dao = MagicMock()
        # Re-scan now requires the source to be in SCAN_FAILED.
        mock_dao.get.return_value = _db_source_item("SCAN_FAILED")
        mock_scan_dao = MagicMock()
        mock_sqs = MagicMock()

        with (
            patch(f"{_SH}._get_dao", return_value=mock_dao),
            patch(f"{_SH}._get_scan_dao", return_value=mock_scan_dao),
            patch(f"{_SH}._get_sqs", return_value=mock_sqs),
            patch(f"{_SH}._SCAN_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/scan-queue"),
        ):
            event = _make_event(
                "POST",
                "/namespaces/{namespaceId}/sources/{sourceId}/rescan",
                path_params={"namespaceId": _NAMESPACE_ID, "sourceId": _SOURCE_ID},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 202

    def test_handler_missing_namespace_id_returns_400(self):
        event = _make_event("GET", "/namespaces/{namespaceId}/sources", path_params={"namespaceId": ""})
        status, _ = _parse(_current_sh().handler(event, None))
        assert status == 400

    def test_handler_invalid_namespace_id_returns_400(self):
        event = _make_event(
            "GET",
            "/namespaces/{namespaceId}/sources",
            path_params={"namespaceId": "invalid@ns"},
        )
        status, _ = _parse(_current_sh().handler(event, None))
        assert status == 400

    def test_handler_unknown_route_returns_404(self):
        event = _make_event(
            "PATCH",
            "/namespaces/{namespaceId}/sources",
            path_params={"namespaceId": _NAMESPACE_ID},
        )
        status, _ = _parse(_current_sh().handler(event, None))
        assert status == 404

    def test_handler_get_scan_job_route(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item()
        mock_scan_dao = MagicMock()
        mock_scan_dao.get.return_value = {
            "status": "COMPLETED",
            "scanType": "full",
            "startedAt": "2026-01-01T00:00:00Z",
        }

        _DR = "coa_sources.api.database_routes"
        with (
            patch(f"{_DR}._get_dao", return_value=mock_dao),
            patch(f"{_DR}._get_scan_dao", return_value=mock_scan_dao),
        ):
            event = _make_event(
                "GET",
                "/namespaces/{namespaceId}/sources/{sourceId}/scan/{jobId}",
                path_params={"namespaceId": _NAMESPACE_ID, "sourceId": _SOURCE_ID, "jobId": "job-123"},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200

    def test_handler_update_metadata_route(self):
        mock_dao = MagicMock()
        mock_dao.get.return_value = _db_source_item()

        _DR = "coa_sources.api.database_routes"
        with patch(f"{_DR}._get_dao", return_value=mock_dao):
            event = _make_event(
                "PUT",
                "/namespaces/{namespaceId}/sources/{sourceId}/metadata",
                path_params={"namespaceId": _NAMESPACE_ID, "sourceId": _SOURCE_ID},
                body={"name": "new-name"},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200

    def test_handler_missing_source_id_returns_400(self):
        event = _make_event(
            "GET",
            "/namespaces/{namespaceId}/sources/{sourceId}",
            path_params={"namespaceId": _NAMESPACE_ID, "sourceId": ""},
        )
        status, _ = _parse(_current_sh().handler(event, None))
        assert status == 400


class TestPathParameterDecoding:
    """The router must percent-decode path parameters before dispatching.

    REST API Gateway proxy integration passes ``pathParameters`` exactly as
    they appear in the URL — still percent-encoded. Smithy-generated clients
    encode every httpLabel per RFC 3986, so a non-ASCII table name reaches the
    Lambda as ``db.%E5%95%86%E5%93%81...`` and, without decoding, never
    matches the stored asset name (observed as a 404 whose message echoes the
    still-encoded id). These tests pin the single decoding point in
    ``_route`` through representative routes.
    """

    _TABLE = "coa_blog_ja.商品マスタ"

    @staticmethod
    def _encoded(value):
        from urllib.parse import quote

        return quote(value, safe="")

    def test_get_table_receives_decoded_table_id(self):
        handler_mock = MagicMock(return_value={"statusCode": 200, "body": json.dumps({})})

        with patch(f"{_SH}._handle_get_table", handler_mock):
            event = _make_event(
                "GET",
                "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}",
                path_params={
                    "namespaceId": _NAMESPACE_ID,
                    "sourceId": _SOURCE_ID,
                    "tableId": self._encoded(self._TABLE),
                },
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200
        handler_mock.assert_called_once_with(_NAMESPACE_ID, _SOURCE_ID, self._TABLE)

    def test_review_table_receives_decoded_table_id(self):
        handler_mock = MagicMock(return_value={"statusCode": 200, "body": json.dumps({})})

        with patch(f"{_SH}._handle_review_table", handler_mock):
            event = _make_event(
                "PUT",
                "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/review",
                path_params={
                    "namespaceId": _NAMESPACE_ID,
                    "sourceId": _SOURCE_ID,
                    "tableId": self._encoded(self._TABLE),
                },
                body={"reviewStatus": "APPROVED"},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200
        assert handler_mock.call_args.args[1:] == (_NAMESPACE_ID, _SOURCE_ID, self._TABLE)

    def test_column_route_receives_decoded_column_name(self):
        handler_mock = MagicMock(return_value={"statusCode": 200, "body": json.dumps({})})
        column = "商品コード"

        with patch(f"{_SH}._handle_review_column", handler_mock):
            event = _make_event(
                "PUT",
                "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/review",
                path_params={
                    "namespaceId": _NAMESPACE_ID,
                    "sourceId": _SOURCE_ID,
                    "tableId": self._encoded(self._TABLE),
                    "columnName": self._encoded(column),
                },
                body={"reviewStatus": "APPROVED"},
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200
        assert handler_mock.call_args.args[1:] == (_NAMESPACE_ID, _SOURCE_ID, self._TABLE, column)

    def test_plain_ascii_table_id_passes_through_unchanged(self):
        handler_mock = MagicMock(return_value={"statusCode": 200, "body": json.dumps({})})

        with patch(f"{_SH}._handle_get_table", handler_mock):
            event = _make_event(
                "GET",
                "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}",
                path_params={
                    "namespaceId": _NAMESPACE_ID,
                    "sourceId": _SOURCE_ID,
                    "tableId": "sales.orders",
                },
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200
        handler_mock.assert_called_once_with(_NAMESPACE_ID, _SOURCE_ID, "sales.orders")

    def test_literal_percent_in_table_id_decodes_exactly_once(self):
        # A client must send a literal "%" as "%25"; one decode restores it.
        handler_mock = MagicMock(return_value={"statusCode": 200, "body": json.dumps({})})

        with patch(f"{_SH}._handle_get_table", handler_mock):
            event = _make_event(
                "GET",
                "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}",
                path_params={
                    "namespaceId": _NAMESPACE_ID,
                    "sourceId": _SOURCE_ID,
                    "tableId": "sales.discount%2550",
                },
            )
            status, _ = _parse(_current_sh().handler(event, None))

        assert status == 200
        handler_mock.assert_called_once_with(_NAMESPACE_ID, _SOURCE_ID, "sales.discount%50")
