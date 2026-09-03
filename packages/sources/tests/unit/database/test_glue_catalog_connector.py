# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for GlueCatalogConnector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from coa_common.domain_models import Column, Table
from coa_sources.database.connectors.glue_catalog import (
    GlueCatalogConnector,
)


@pytest.fixture
def connector():
    return GlueCatalogConnector()


class TestTestConnection:
    def test_missing_database_name(self, connector):
        result = connector.test_connection({})
        assert not result.success
        assert "database_name" in result.message

    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_database_not_found(self, mock_boto3, connector):
        mock_client = MagicMock()
        not_found = type("EntityNotFoundException", (Exception,), {})
        mock_client.exceptions.EntityNotFoundException = not_found
        mock_client.get_database.side_effect = not_found()
        mock_boto3.client.return_value = mock_client

        result = connector.test_connection({"database_name": "nonexistent", "region": "us-east-1"})
        assert not result.success
        assert "does not exist" in result.message

    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_successful_connection(self, mock_boto3, connector):
        mock_client = MagicMock()
        mock_client.get_database.return_value = {"Database": {"Name": "test-db", "Description": "Test database"}}
        mock_client.get_tables.return_value = {"TableList": [{"Name": "table1"}, {"Name": "table2"}]}
        mock_boto3.client.return_value = mock_client

        result = connector.test_connection({"database_name": "test-db", "region": "us-east-1"})
        assert result.success
        assert len(result.checks) == 2
        assert result.checks[0].status == "ok"
        assert result.checks[1].status == "ok"

    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_empty_database_warning(self, mock_boto3, connector):
        mock_client = MagicMock()
        mock_client.get_database.return_value = {"Database": {"Name": "empty-db"}}
        mock_client.get_tables.return_value = {"TableList": []}
        mock_boto3.client.return_value = mock_client

        result = connector.test_connection({"database_name": "empty-db", "region": "us-east-1"})
        assert result.success
        assert result.checks[1].status == "warning"


class TestDiscoverMetadata:
    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_discovers_typed_tables_and_columns(self, mock_boto3, connector):
        mock_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "TableList": [
                    {
                        "Name": "orders",
                        "Description": "Order records",
                        "Parameters": {"classification": "parquet"},
                        "StorageDescriptor": {
                            "Location": "s3://bucket/orders/",
                            "Columns": [
                                {"Name": "order_id", "Type": "bigint", "Comment": "PK"},
                                {"Name": "customer_id", "Type": "bigint", "Comment": ""},
                                {"Name": "amount", "Type": "decimal(10,2)", "Comment": ""},
                            ],
                        },
                        "PartitionKeys": [{"Name": "order_date", "Type": "date", "Comment": ""}],
                    }
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        mock_boto3.client.return_value = mock_client

        result = connector.discover_metadata(
            {
                "database_name": "sales_db",
                "data_source_id": "DS#123",
                "namespace_id": "ns-test",
                "region": "us-east-1",
            }
        )

        assert len(result.tables) == 1
        table = result.tables[0]
        assert isinstance(table, Table)
        assert table.name == "orders"
        assert table.table_id == "sales_db.orders"
        assert table.technical_metadata_hash != ""
        assert table.technical_metadata.column_count == 4
        assert table.technical_metadata.format == "parquet"
        assert table.technical_metadata.location == "s3://bucket/orders/"
        assert table.technical_metadata.partition_keys == ["order_date"]

        assert len(table.columns) == 4
        assert all(isinstance(c, Column) for c in table.columns)

        partition_cols = [c for c in table.columns if c.is_partition_key]
        assert len(partition_cols) == 1
        assert partition_cols[0].name == "order_date"
        assert partition_cols[0].nullable is False

        assert result.total_columns == 4

    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_table_exclude_filter(self, mock_boto3, connector):
        mock_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "TableList": [
                    {
                        "Name": "orders",
                        "StorageDescriptor": {"Columns": [], "Location": ""},
                        "PartitionKeys": [],
                    },
                    {
                        "Name": "orders_staging",
                        "StorageDescriptor": {"Columns": [], "Location": ""},
                        "PartitionKeys": [],
                    },
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        mock_boto3.client.return_value = mock_client

        result = connector.discover_metadata(
            {
                "database_name": "db",
                "table_exclude_filter": "*_staging",
                "region": "us-east-1",
            }
        )

        assert len(result.tables) == 1
        assert result.tables[0].name == "orders"

    @patch("coa_sources.database.connectors.glue_catalog.boto3")
    def test_technical_metadata_hash_deterministic(self, mock_boto3, connector):
        """Same columns in different order produce the same hash."""
        mock_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "TableList": [
                    {
                        "Name": "t1",
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "b", "Type": "int"},
                                {"Name": "a", "Type": "string"},
                            ],
                            "Location": "",
                        },
                        "PartitionKeys": [],
                    }
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        mock_boto3.client.return_value = mock_client

        r1 = connector.discover_metadata({"database_name": "db", "region": "us-east-1"})

        paginator.paginate.return_value = [
            {
                "TableList": [
                    {
                        "Name": "t1",
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "a", "Type": "string"},
                                {"Name": "b", "Type": "int"},
                            ],
                            "Location": "",
                        },
                        "PartitionKeys": [],
                    }
                ]
            }
        ]
        r2 = connector.discover_metadata({"database_name": "db", "region": "us-east-1"})

        assert r1.tables[0].technical_metadata_hash == r2.tables[0].technical_metadata_hash

    @patch("coa_sources.database.connectors.sts_assume.boto3")
    def test_cross_account_role(self, mock_boto3, connector):
        """Verify STS AssumeRole is called for cross-account access."""
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }

        mock_session = MagicMock()
        mock_glue = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableList": []}]
        mock_glue.get_paginator.return_value = paginator
        mock_session.client.return_value = mock_glue

        mock_boto3.client.return_value = mock_sts
        mock_boto3.Session.return_value = mock_session

        connector.discover_metadata(
            {
                "database_name": "db",
                "cross_account_role_arn": "arn:aws:iam::999999999999:role/test",
                "external_id": "coa-dev-ns-1",
                "region": "us-east-1",
            }
        )

        mock_sts.assume_role.assert_called_once()
        mock_boto3.Session.assert_called_once()

    @patch("coa_sources.database.connectors.sts_assume.boto3")
    def test_cross_account_role_passes_external_id(self, mock_boto3, connector):
        """The configured external_id must reach AssumeRole."""
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        mock_session = MagicMock()
        mock_glue = MagicMock()
        mock_glue.get_database.return_value = {"Database": {"Name": "db"}}
        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableList": []}]
        mock_glue.get_paginator.return_value = paginator
        mock_session.client.return_value = mock_glue
        mock_boto3.client.return_value = mock_sts
        mock_boto3.Session.return_value = mock_session

        connector.discover_metadata(
            {
                "database_name": "db",
                "cross_account_role_arn": "arn:aws:iam::999999999999:role/test",
                "external_id": "ext-xyz",
                "region": "us-east-1",
            }
        )

        assert mock_sts.assume_role.call_args.kwargs["ExternalId"] == "ext-xyz"
        assert mock_sts.assume_role.call_args.kwargs["RoleArn"] == "arn:aws:iam::999999999999:role/test"

    @patch("coa_sources.database.connectors.sts_assume.boto3")
    def test_cross_account_role_without_external_id_is_refused(self, mock_boto3, connector):
        """No external_id → refuse to assume at all (confused-deputy guard).

        An assume with no ExternalId carries no evidence of which namespace asked
        for it, so a caller could point a source at another tenant's role and read
        it. The connector must raise rather than fall back to an unconditioned
        assume, which is what it used to do.
        """
        mock_sts = MagicMock()
        mock_boto3.client.return_value = mock_sts

        with pytest.raises(ValueError, match="external_id is required"):
            connector.discover_metadata(
                {
                    "database_name": "db",
                    "cross_account_role_arn": "arn:aws:iam::999999999999:role/test",
                    "region": "us-east-1",
                }
            )

        mock_sts.assume_role.assert_not_called()

    @patch("coa_sources.database.connectors.sts_assume.boto3")
    def test_role_session_name_carries_namespace(self, mock_boto3, connector):
        """RoleSessionName reaches the data owner's CloudTrail — it must name the namespace."""
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {"AccessKeyId": "A", "SecretAccessKey": "S", "SessionToken": "T"}
        }
        mock_session = MagicMock()
        mock_glue = MagicMock()
        mock_glue.get_database.return_value = {"Database": {"Name": "db"}}
        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableList": []}]
        mock_glue.get_paginator.return_value = paginator
        mock_session.client.return_value = mock_glue
        mock_boto3.client.return_value = mock_sts
        mock_boto3.Session.return_value = mock_session

        connector.discover_metadata(
            {
                "database_name": "db",
                "cross_account_role_arn": "arn:aws:iam::999999999999:role/test",
                "external_id": "coa-dev-ns-42",
                "namespace_id": "ns-42",
                "region": "us-east-1",
            }
        )

        session_name = mock_sts.assume_role.call_args.kwargs["RoleSessionName"]
        assert "ns-42" in session_name
        assert len(session_name) <= 64
