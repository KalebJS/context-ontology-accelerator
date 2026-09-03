# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the discovery handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from coa_common.domain_models import (
    Column,
    DiscoveredMetadata,
    Table,
)
from coa_control_plane_server.models.source_status import SourceStatus
from coa_sources.database.connectors import get_connector
from coa_sources.database.connectors.base import (
    ConnectionTestResult,
)

MODULE = "coa_sources.database.pipeline.discovery_handler"


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv("SOURCES_TABLE", "test-datasources")
    monkeypatch.setenv("SOURCE_SCAN_JOBS_TABLE", "test-scan-jobs")
    monkeypatch.setenv("SMUS_DOMAIN_ID", "dz-test-domain")
    monkeypatch.setenv("NAMESPACES_TABLE", "test-namespaces")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset module-level DAO singletons between tests."""
    import coa_sources.database.pipeline.discovery_handler as mod

    mod._ds_dao = None
    mod._scan_dao = None
    mod._ns_dao = None
    yield
    mod._ds_dao = None
    mod._scan_dao = None
    mod._ns_dao = None


class TestDiscoveryHandler:
    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_glue_database_discovery(self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write):
        from coa_sources.database.pipeline.discovery_handler import (
            handler,
        )

        # Mock DAOs
        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {
                "databaseName": "analytics_db",
                "catalogId": "123456789012",
                "region": "us-east-1",
            },
        }
        mock_get_ds.return_value = mock_ds_dao

        mock_scan_dao = MagicMock()
        mock_get_scan.return_value = mock_scan_dao

        mock_ns_dao = MagicMock()
        mock_ns_dao.get.return_value = {"dataZoneProjectId": "proj-123"}
        mock_get_ns.return_value = mock_ns_dao

        # Mock connector
        mock_connector = MagicMock()
        mock_connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        test_table = Table(
            name="events",
            database="analytics_db",
            columns=[Column(name="id", data_type="bigint")],
        )
        mock_connector.discover_metadata.return_value = DiscoveredMetadata(tables=[test_table])
        mock_get_connector.return_value = mock_connector

        mock_write.return_value = {"assets_created": 1, "assets_revised": 0}

        result = handler(
            {
                "datasourceId": "DS#ds-123",
                "scanJobId": "SCAN#scan-456",
                "namespaceId": "ns-test",
                "scanType": "full",
            },
            None,
        )

        assert result["tablesDiscovered"] == 1
        assert result["columnsDiscovered"] == 1
        assert result["assetsCreated"] == 1
        mock_get_connector.assert_called_once_with("GLUE_DATABASE")
        mock_scan_dao.update.assert_called_once()
        # The source-record update persists the distinct discovered schemas
        # (used by the federation step to scope Lake Formation grants).
        schema_updates = [
            c.kwargs["update_fields"]["discoveredSchemas"]
            for c in mock_ds_dao.update.call_args_list
            if "discoveredSchemas" in c.kwargs.get("update_fields", {})
        ]
        assert schema_updates == [["analytics_db"]]
        # Native Glue sources are marked queryable after a successful scan.
        queryable_updates = [
            c.kwargs["update_fields"]["queryable"]
            for c in mock_ds_dao.update.call_args_list
            if "queryable" in c.kwargs.get("update_fields", {})
        ]
        assert queryable_updates == [True]

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_snowflake_discovers_via_driver_with_warehouse(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        from coa_sources.database.connectors.base import ConnectionTestResult
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "JDBC_DATABASE",
            "configuration": {
                "databaseName": "db",
                "engine": "SNOWFLAKE",
                "host": "acme.snowflakecomputing.com",
                "warehouse": "WH",
                "role": "R",
            },
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_get_scan.return_value = MagicMock()
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-123"}))
        mock_write.return_value = {"assets_created": 1}
        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name="t", database="public", columns=[Column(name="c", data_type="text")])]
        )
        mock_get_connector.return_value = connector

        result = handler(
            {"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-1", "scanType": "full"}, None
        )

        # The handler is engine-agnostic: it routes every JDBC sub-type to the same
        # connector. The connector is mocked here, which is what keeps this covering
        # the handler's warehouse/role pass-through regardless of the engine.
        mock_get_connector.assert_called_once_with("JDBC_DATABASE")
        # warehouse/role are passed through to the connector for the Snowflake dialect.
        cfg = connector.discover_metadata.call_args[0][0]
        assert cfg["warehouse"] == "WH"
        assert cfg["role"] == "R"
        assert result["tablesDiscovered"] == 1

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_cross_account_role_and_external_id_passthrough(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """LEGACY BRIDGE: an externalId already in the stored config still wins.

        Those roles were onboarded before the derived ExternalId and their trust
        policy pins the old value, so honouring it is what keeps their next scan
        working. A caller can no longer get a value into storage — see
        ``database_routes._strip_external_id``."""
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {
                "databaseName": "analytics_db",
                "catalogId": "123456789012",
                "region": "us-east-1",
                "crossAccountRoleArn": "-dev-datasource-access-acme",
                "externalId": "ext-abc",
            },
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_get_scan.return_value = MagicMock()
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-123"}))
        mock_write.return_value = {"assets_created": 1}
        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name="t", database="analytics_db", columns=[Column(name="c", data_type="text")])]
        )
        mock_get_connector.return_value = connector

        handler({"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-1", "scanType": "full"}, None)

        # Both test_connection and discover_metadata receive the same config.
        test_cfg = connector.test_connection.call_args[0][0]
        disc_cfg = connector.discover_metadata.call_args[0][0]
        for cfg in (test_cfg, disc_cfg):
            assert cfg["cross_account_role_arn"] == ("-dev-datasource-access-acme")
            assert cfg["external_id"] == "ext-abc"

    @patch.dict("os.environ", {"RESOURCE_PREFIX": "coa-dev-"})
    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_external_id_is_derived_from_namespace_not_the_request(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """The ExternalId presented on the assume comes from the namespace.

        The role ARN is caller-supplied, so this binding is what stops a caller
        with manageSource on one namespace from pointing a source at another
        tenant's datasource-access role and reading it (confused deputy).
        """
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {
                "databaseName": "analytics_db",
                "catalogId": "123456789012",
                "region": "us-east-1",
                "crossAccountRoleArn": "arn:aws:iam::999999999999:role/coa-dev-datasource-access-acme",
            },
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_get_scan.return_value = MagicMock()
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-123"}))
        mock_write.return_value = {"assets_created": 1}
        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name="t", database="analytics_db", columns=[Column(name="c", data_type="text")])]
        )
        mock_get_connector.return_value = connector

        handler({"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-9", "scanType": "full"}, None)

        for cfg in (
            connector.test_connection.call_args[0][0],
            connector.discover_metadata.call_args[0][0],
        ):
            assert cfg["external_id"] == "coa-dev-ns-9"

    @patch.dict("os.environ", {"RESOURCE_PREFIX": "coa-dev-"})
    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_two_namespaces_get_different_external_ids(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """The same role ARN in two namespaces must present different ExternalIds.

        This is the property that makes the trust policy an authorization list:
        tenant B pins its own namespace's value, so tenant A's scan of B's role
        is denied by STS.
        """
        from coa_sources.database.pipeline.discovery_handler import handler

        shared_role = "arn:aws:iam::999999999999:role/coa-dev-datasource-access-acme"
        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {
                "databaseName": "analytics_db",
                "catalogId": "123456789012",
                "region": "us-east-1",
                "crossAccountRoleArn": shared_role,
            },
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_get_scan.return_value = MagicMock()
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-123"}))
        mock_write.return_value = {"assets_created": 1}
        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name="t", database="analytics_db", columns=[Column(name="c", data_type="text")])]
        )
        mock_get_connector.return_value = connector

        seen = []
        for namespace_id in ("ns-tenant-a", "ns-tenant-b"):
            handler(
                {
                    "datasourceId": "DS#ds-1",
                    "scanJobId": "SCAN#s",
                    "namespaceId": namespace_id,
                    "scanType": "full",
                },
                None,
            )
            seen.append(connector.discover_metadata.call_args[0][0]["external_id"])

        assert seen == ["coa-dev-ns-tenant-a", "coa-dev-ns-tenant-b"]
        assert len(set(seen)) == 2

    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    def test_datasource_not_found(self, mock_get_ds, mock_get_scan):
        from coa_sources.database.pipeline.discovery_handler import (
            handler,
        )

        mock_dao = MagicMock()
        mock_dao.get.return_value = None
        mock_get_ds.return_value = mock_dao
        mock_get_scan.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="Data source not found"):
            handler(
                {
                    "datasourceId": "DS#nonexistent",
                    "scanJobId": "SCAN#x",
                    "namespaceId": "ns-test",
                },
                None,
            )

    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    def test_unsupported_source_type(self, mock_get_ds, mock_get_scan):
        from coa_sources.database.pipeline.discovery_handler import (
            handler,
        )

        mock_dao = MagicMock()
        mock_dao.get.return_value = {
            "sourceSubType": "UNKNOWN_TYPE",
            "configuration": {},
        }
        mock_get_ds.return_value = mock_dao
        mock_get_scan.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="Unsupported source type"):
            handler(
                {
                    "datasourceId": "DS#unknown-1",
                    "scanJobId": "SCAN#x",
                    "namespaceId": "ns-test",
                },
                None,
            )

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_too_many_tables_fails_fast(self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write):
        """When discovery yields more tables than MAX_TABLES_PER_SOURCE, the
        scan must fail fast with an actionable message and NOT attempt the
        DataZone write (which would otherwise hit the Lambda timeout)."""
        from coa_sources.database.pipeline import discovery_handler as mod
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {"databaseName": "db", "catalogId": "123456789012", "region": "us-east-1"},
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_scan_dao = MagicMock()
        mock_get_scan.return_value = mock_scan_dao
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-1"}))

        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name=f"t{i}", database="db", columns=[]) for i in range(3)]
        )
        mock_get_connector.return_value = connector

        with (
            patch.object(mod, "MAX_TABLES_PER_SOURCE", 2),
            pytest.raises(RuntimeError, match="exceeding the limit"),
        ):
            handler(
                {"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-1", "scanType": "full"},
                None,
            )

        # Fail-fast: no DataZone write attempted.
        mock_write.assert_not_called()
        # Source marked SCAN_FAILED via the error path.
        statuses = [
            c.kwargs["update_fields"].get("status")
            for c in mock_ds_dao.update.call_args_list
            if "status" in c.kwargs.get("update_fields", {})
        ]
        assert SourceStatus.SCAN_FAILED in statuses


class TestConnectorRegistry:
    def test_get_connector_glue(self):
        from coa_sources.database.connectors.glue_catalog import GlueCatalogConnector

        connector = get_connector("GLUE_DATABASE")
        assert isinstance(connector, GlueCatalogConnector)

    def test_get_connector_unknown_raises(self):
        with pytest.raises(ValueError, match="Unsupported source type"):
            get_connector("UNKNOWN")


class TestDiscoveryHandlerStatusLifecycle:
    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_sets_scanning_status_at_start(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {"databaseName": "db", "catalogId": "123456789012", "region": "us-east-1"},
        }
        mock_get_ds.return_value = mock_ds_dao

        mock_scan_dao = MagicMock()
        mock_get_scan.return_value = mock_scan_dao

        mock_ns_dao = MagicMock()
        mock_ns_dao.get.return_value = {"dataZoneProjectId": "proj-1"}
        mock_get_ns.return_value = mock_ns_dao

        mock_connector = MagicMock()
        mock_connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        mock_connector.discover_metadata.return_value = DiscoveredMetadata(tables=[])
        mock_get_connector.return_value = mock_connector

        mock_write.return_value = {"assets_created": 0}

        handler({"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s-1", "namespaceId": "ns-1", "scanType": "full"}, None)

        # First update call should set SCANNING
        first_update = mock_ds_dao.update.call_args_list[0]
        assert first_update[1]["update_fields"]["status"] == SourceStatus.SCANNING

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_datazone_write_exhaustion_writes_terminal_status(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """If DataZone asset writes exhaust their retries and the writer raises
        (e.g. sustained TooManyRequestsException, issue #857), discovery must
        still write a TERMINAL source status (SCAN_FAILED) — never leave the
        source stuck in SCANNING."""
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = {
            "sourceSubType": "GLUE_DATABASE",
            "configuration": {"databaseName": "db", "catalogId": "123456789012", "region": "us-east-1"},
        }
        mock_get_ds.return_value = mock_ds_dao
        mock_scan_dao = MagicMock()
        mock_get_scan.return_value = mock_scan_dao
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-1"}))

        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = DiscoveredMetadata(
            tables=[Table(name="t", database="db", columns=[Column(name="c", data_type="text")])]
        )
        mock_get_connector.return_value = connector

        # Writer exhausts retries under throttling and fails loud.
        mock_write.side_effect = RuntimeError(
            "Failed to write 1 asset(s): TooManyRequestsException (reached max retries: 10)"
        )

        with pytest.raises(RuntimeError):
            handler(
                {"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-1", "scanType": "full"},
                None,
            )

        # Source must be driven to a terminal status, not left SCANNING.
        statuses = [
            c.kwargs["update_fields"].get("status")
            for c in mock_ds_dao.update.call_args_list
            if "status" in c.kwargs.get("update_fields", {})
        ]
        assert SourceStatus.SCAN_FAILED in statuses
        assert statuses[-1] == SourceStatus.SCAN_FAILED, "final status write must be terminal, not SCANNING"

    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    def test_sets_scan_failed_on_error(self, mock_get_ds, mock_get_scan):
        from coa_sources.database.pipeline.discovery_handler import handler

        mock_ds_dao = MagicMock()
        mock_ds_dao.get.return_value = None  # Will cause ValueError
        mock_get_ds.return_value = mock_ds_dao

        mock_scan_dao = MagicMock()
        mock_get_scan.return_value = mock_scan_dao

        with pytest.raises(RuntimeError):
            handler({"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s-1", "namespaceId": "ns-1"}, None)

        # Data source status set to SCAN_FAILED
        ds_update = mock_ds_dao.update.call_args_list
        assert len(ds_update) == 1
        assert ds_update[0][1]["update_fields"]["status"] == SourceStatus.SCAN_FAILED

        # errorMessage set on scan job
        scan_update = mock_scan_dao.update.call_args_list
        assert len(scan_update) == 1
        assert "errorMessage" in scan_update[0][1]["update_fields"]


# ═══════════════════════════════════════════════════════════════════
# _provision_athena_federation — error isolation + rollback
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
# MAX_TABLES_PER_SOURCE env-var parsing — cold-start safety
# ═══════════════════════════════════════════════════════════════════


class TestMaxTablesEnvParsing:
    def test_invalid_value_falls_back_to_zero(self, monkeypatch):
        """A misconfigured (non-integer) MAX_TABLES_PER_SOURCE must not crash
        the Lambda on cold start; it falls back to 0 (cap disabled)."""
        import importlib

        import coa_sources.database.pipeline.discovery_handler as mod

        monkeypatch.setenv("MAX_TABLES_PER_SOURCE", "abc")
        try:
            reloaded = importlib.reload(mod)
            assert reloaded.MAX_TABLES_PER_SOURCE == 0
        finally:
            # Restore a clean module state for other tests.
            monkeypatch.delenv("MAX_TABLES_PER_SOURCE", raising=False)
            importlib.reload(mod)

    def test_valid_value_is_parsed(self, monkeypatch):
        import importlib

        import coa_sources.database.pipeline.discovery_handler as mod

        monkeypatch.setenv("MAX_TABLES_PER_SOURCE", "250")
        try:
            reloaded = importlib.reload(mod)
            assert reloaded.MAX_TABLES_PER_SOURCE == 250
        finally:
            monkeypatch.delenv("MAX_TABLES_PER_SOURCE", raising=False)
            importlib.reload(mod)


class TestCustomConnectorDiscovery:
    """Threading the custom-connector config, and surfacing a degraded scan.

    The catalog name is the interesting part: it is derived by the control plane
    and stored as a TOP-LEVEL attribute on the source record, not inside the
    caller-supplied ``configuration`` blob, so the handler has to read it from the
    item rather than from the config.
    """

    @staticmethod
    def _item(**overrides):
        item = {
            "sourceSubType": "CUSTOM_CONNECTOR",
            "athenaDataCatalogName": "coadevds_abc123",
            "configuration": {"databaseName": "widgets", "tableFilter": "dim_*"},
        }
        item.update(overrides)
        return item

    @staticmethod
    def _wire(mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, item):
        ds_dao = MagicMock()
        ds_dao.get.return_value = item
        mock_get_ds.return_value = ds_dao
        scan_dao = MagicMock()
        mock_get_scan.return_value = scan_dao
        mock_get_ns.return_value = MagicMock(get=MagicMock(return_value={"dataZoneProjectId": "proj-123"}))
        mock_write.return_value = {"assets_created": 1}
        connector = MagicMock()
        connector.test_connection.return_value = ConnectionTestResult(success=True, message="OK", checks=[])
        connector.discover_metadata.return_value = metadata
        mock_get_connector.return_value = connector
        return ds_dao, scan_dao, connector

    _EVENT = {"datasourceId": "DS#ds-1", "scanJobId": "SCAN#s", "namespaceId": "ns-1", "scanType": "full"}

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_threads_the_derived_catalog_name_from_the_source_record(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        from coa_sources.database.pipeline.discovery_handler import handler

        metadata = DiscoveredMetadata(
            tables=[Table(name="dim_a", database="widgets", columns=[Column(name="c", data_type="int")])]
        )
        _, _, connector = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, self._item()
        )
        handler(self._EVENT, None)

        for cfg in (connector.test_connection.call_args[0][0], connector.discover_metadata.call_args[0][0]):
            # Read from the item, not the config blob — the caller never supplies it.
            assert cfg["athena_data_catalog_name"] == "coadevds_abc123"
            assert cfg["database_name"] == "widgets"
            assert cfg["table_filter"] == "dim_*"

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_a_source_without_the_attribute_threads_an_empty_catalog_name(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """The connector then fails its own connection test with an actionable
        message, rather than the handler raising a KeyError here."""
        from coa_sources.database.pipeline.discovery_handler import handler

        item = self._item()
        del item["athenaDataCatalogName"]
        metadata = DiscoveredMetadata(
            tables=[Table(name="t", database="widgets", columns=[Column(name="c", data_type="int")])]
        )
        _, _, connector = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, item
        )
        handler(self._EVENT, None)
        assert connector.discover_metadata.call_args[0][0]["athena_data_catalog_name"] == ""

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_records_unreadable_tables_on_the_scan_job(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """A table that fails to read reaches review with no columns and no keys
        while enrichment fills AI descriptions over the gap, so the count has to
        leave the logs and land on the scan job the steward sees."""
        from coa_sources.database.pipeline.discovery_handler import handler

        metadata = DiscoveredMetadata(
            tables=[Table(name="ok", database="widgets", columns=[Column(name="c", data_type="int")])],
            failed_tables=["widgets.bad", "widgets.worse"],
        )
        _, scan_dao, _ = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, self._item()
        )
        handler(self._EVENT, None)

        fields = scan_dao.update.call_args.kwargs["update_fields"]
        assert fields["tablesFailed"] == 2
        assert fields["failedTables"] == ["widgets.bad", "widgets.worse"]
        assert fields["tablesDiscovered"] == 1

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_a_clean_scan_writes_no_failure_fields(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """Absent rather than zero: a `tablesFailed: 0` on every scan job would
        make the field useless as a filter for the degraded ones."""
        from coa_sources.database.pipeline.discovery_handler import handler

        metadata = DiscoveredMetadata(
            tables=[Table(name="ok", database="widgets", columns=[Column(name="c", data_type="int")])]
        )
        _, scan_dao, _ = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, self._item()
        )
        handler(self._EVENT, None)

        fields = scan_dao.update.call_args.kwargs["update_fields"]
        assert "tablesFailed" not in fields
        assert "failedTables" not in fields

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_the_stored_failed_table_list_is_capped(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """A DynamoDB item is limited to 400 KB, so the list is a signal and the
        count beside it is the exact figure."""
        from coa_sources.database.pipeline.discovery_handler import (
            _MAX_REPORTED_FAILED_TABLES,
            handler,
        )

        failed = [f"widgets.t{i}" for i in range(_MAX_REPORTED_FAILED_TABLES + 25)]
        metadata = DiscoveredMetadata(
            tables=[Table(name="ok", database="widgets", columns=[Column(name="c", data_type="int")])],
            failed_tables=failed,
        )
        _, scan_dao, _ = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, self._item()
        )
        handler(self._EVENT, None)

        fields = scan_dao.update.call_args.kwargs["update_fields"]
        assert fields["tablesFailed"] == len(failed)
        assert len(fields["failedTables"]) == _MAX_REPORTED_FAILED_TABLES

    @patch(f"{MODULE}.write_to_datazone")
    @patch(f"{MODULE}._get_ns_dao")
    @patch(f"{MODULE}._get_scan_dao")
    @patch(f"{MODULE}._get_ds_dao")
    @patch(f"{MODULE}.get_connector")
    def test_discovery_does_not_mark_the_source_queryable(
        self, mock_get_connector, mock_get_ds, mock_get_scan, mock_get_ns, mock_write
    ):
        """The post-discovery federation step owns that flip, as it does for JDBC."""
        from coa_sources.database.pipeline.discovery_handler import handler

        metadata = DiscoveredMetadata(
            tables=[Table(name="ok", database="widgets", columns=[Column(name="c", data_type="int")])]
        )
        ds_dao, _, _ = self._wire(
            mock_get_ds, mock_get_scan, mock_get_ns, mock_write, mock_get_connector, metadata, self._item()
        )
        handler(self._EVENT, None)

        source_fields = ds_dao.update.call_args.kwargs["update_fields"]
        assert "queryable" not in source_fields
        # discoveredSchemas is what serve pins the query Database to.
        assert source_fields["discoveredSchemas"] == ["widgets"]
