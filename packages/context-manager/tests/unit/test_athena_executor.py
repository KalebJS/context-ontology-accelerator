# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for AthenaQueryExecutor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from coa_common.constants import RESOURCE_PREFIX
from coa_serve.clients.athena import AthenaQueryExecutor
from coa_serve.tier2.sql_firewall import UnsafeSQLError


@pytest.mark.unit
class TestAthenaValidation:
    """Test namespace validation and SQL safety (via centralized firewall)."""

    def _make_executor(self):
        with patch("boto3.client"), patch("boto3.resource"):
            return AthenaQueryExecutor(region="us-east-1", sources_table="test-sources")

    def test_validates_namespace(self):
        from coa_serve.query_utils import validate_namespace

        validate_namespace("my-namespace")
        validate_namespace("test_ns")
        validate_namespace("demo123")

        with pytest.raises(ValueError):
            validate_namespace("invalid namespace!")
        with pytest.raises(ValueError):
            validate_namespace("ns with spaces")

    def test_workgroup_prefix(self):
        with patch("boto3.client"), patch("boto3.resource"):
            executor = AthenaQueryExecutor(workgroup_prefix="coa-", sources_table="t")
        assert executor._workgroup_prefix == "coa-"


@pytest.mark.unit
class TestAthenaExecution:
    """Test query execution (mocked boto3)."""

    async def test_execute_success(self):
        with patch("boto3.client") as mock_boto:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena

            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-123"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "id"},
                            {"Name": "name"},
                        ]
                    },
                    "Rows": [
                        {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},  # header
                        {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
                        {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "Bob"}]},
                    ],
                }
            }

            with patch("boto3.resource"):
                executor = AthenaQueryExecutor(region="us-east-1", sources_table="t")
            result = await executor.execute(
                "SELECT id, name FROM users",
                namespace="demo",
                database="mydb",
            )

            assert result.row_count == 2
            assert result.columns == ["id", "name"]
            assert result.rows[0]["id"] == "1"
            assert result.rows[0]["name"] == "Alice"
            assert result.truncated is False

    async def test_invalid_timeout_raises(self):
        with patch("boto3.client"), patch("boto3.resource"):
            executor = AthenaQueryExecutor(region="us-east-1", sources_table="t")

        with pytest.raises(ValueError, match="timeout_seconds must be 1-300"):
            await executor.execute(
                "SELECT 1",
                namespace="demo",
                timeout_seconds=0,
            )

        with pytest.raises(ValueError, match="timeout_seconds must be 1-300"):
            await executor.execute(
                "SELECT 1",
                namespace="demo",
                timeout_seconds=400,
            )

    async def test_invalid_sql_raises(self):
        with patch("boto3.client"), patch("boto3.resource"):
            executor = AthenaQueryExecutor(region="us-east-1", sources_table="t")

        with pytest.raises(UnsafeSQLError):
            await executor.execute(
                "DROP TABLE users",
                namespace="demo",
            )


@pytest.mark.unit
class TestTableNameRewrite:
    """Test _rewrite_table_names_for_federation."""

    def test_strips_schema_prefix_and_quotes(self):
        result = AthenaQueryExecutor._rewrite_table_names_for_federation(
            "SELECT COUNT(*) AS v0 FROM BIRD_PUBLIC_INCOME AS V1", "public"
        )
        assert '"income"' in result
        assert "BIRD_PUBLIC_INCOME" not in result

    def test_handles_multiple_tables(self):
        sql = "SELECT a.id, b.name FROM BIRD_PUBLIC_INCOME AS a JOIN BIRD_PUBLIC_MAJOR AS b ON a.id = b.id"
        result = AthenaQueryExecutor._rewrite_table_names_for_federation(sql, "public")
        assert '"income"' in result
        assert '"major"' in result
        assert "BIRD_PUBLIC" not in result

    def test_no_match_leaves_sql_unchanged(self):
        sql = "SELECT 1 FROM some_table"
        result = AthenaQueryExecutor._rewrite_table_names_for_federation(sql, "public")
        assert "some_table" in result

    def test_preserves_column_references(self):
        sql = "SELECT V1.AMOUNT FROM BIRD_PUBLIC_INCOME AS V1 WHERE V1.AMOUNT > 100"
        result = AthenaQueryExecutor._rewrite_table_names_for_federation(sql, "public")
        assert '"income"' in result
        assert "AMOUNT" in result or "amount" in result.lower()

    def test_invalid_sql_returns_original(self):
        sql = "NOT VALID SQL {{{"
        result = AthenaQueryExecutor._rewrite_table_names_for_federation(sql, "public")
        assert result == sql


@pytest.mark.unit
class TestFederatedCatalogResolution:
    """Test that federated catalog sources route correctly."""

    async def test_federated_source_uses_discovered_schemas(self):
        """When athenaDataCatalogName + discoveredSchemas set, uses first discovered schema."""
        import json

        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_table.query.return_value = {
                "Items": [
                    {
                        "sourceType": "DATABASE",
                        "athenaDataCatalogName": "scldevds_abc123",
                        "discoveredSchemas": ["public", "analytics"],
                        "queryable": True,
                        "configuration": json.dumps({"schemaName": "ignored"}),
                    }
                ]
            }
            mock_table.get_item.return_value = {"Item": {"athenaWorkgroupName": f"{RESOURCE_PREFIX}-dev-ns-123"}}
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-fed"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "cnt"}]},
                    "Rows": [
                        {"Data": [{"VarCharValue": "cnt"}]},
                        {"Data": [{"VarCharValue": "42"}]},
                    ],
                }
            }

            executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources", sources_registry=None)
            executor._sources._table_name = "coa-sources"

        result = await executor.execute(
            "SELECT COUNT(*) FROM BIRD_PUBLIC_INCOME",
            namespace="ns-123",
        )

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Catalog"] == "scldevds_abc123"
        assert call_kwargs["QueryExecutionContext"]["Database"] == "public"
        assert '"income"' in call_kwargs["QueryString"]
        assert result.row_count == 1

    async def test_not_queryable_source_falls_back(self):
        """When queryable is explicitly False, falls back to default database."""
        import json

        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_table.query.return_value = {
                "Items": [
                    {
                        "sourceType": "DATABASE",
                        "athenaDataCatalogName": "scldevds_abc123",
                        "queryable": False,
                        "configuration": json.dumps({}),
                    }
                ]
            }
            mock_table.get_item.return_value = {"Item": None}
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-nq"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "x"}]},
                    "Rows": [{"Data": [{"VarCharValue": "x"}]}],
                }
            }

            with patch.dict("os.environ", {"ATHENA_DATABASE": "fallback_db"}):
                executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources")

        await executor.execute("SELECT 1 as x", namespace="ns-123")

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "fallback_db"
        assert call_kwargs["QueryExecutionContext"]["Catalog"] == "AwsDataCatalog"

    async def test_glue_source_uses_athena_database(self):
        """Glue native source uses athenaDatabase field."""
        import json

        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_table.get_item.return_value = {
                "Item": {
                    "sourceType": "DATABASE",
                    "sourceSubType": "GLUE_DATABASE",
                    "athenaDatabase": "my_glue_db",
                    "queryable": True,
                    "configuration": json.dumps({"databaseName": "ignored"}),
                }
            }
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-g"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "x"}]},
                    "Rows": [{"Data": [{"VarCharValue": "x"}]}],
                }
            }

            executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources")

        await executor.execute("SELECT 1", namespace="ns-123", data_source_id="src-glue")

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "my_glue_db"
        assert call_kwargs["QueryExecutionContext"]["Catalog"] == "AwsDataCatalog"


@pytest.mark.unit
class TestAthenaDatabaseResolution:
    """Test per-namespace database resolution via SourcesRegistry."""

    async def test_uses_resolved_database_from_ddb(self):
        """When data_source_id resolves to a database, that database is used in the query."""
        import json

        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_table.get_item.return_value = {
                "Item": {"configuration": json.dumps({"databaseName": "bird_test_db_catalog"})}
            }
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "count"}]},
                    "Rows": [
                        {"Data": [{"VarCharValue": "count"}]},
                        {"Data": [{"VarCharValue": "42"}]},
                    ],
                }
            }

            executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources")

        await executor.execute(
            "SELECT count(*) as count FROM bird_public_claim",
            namespace="ns-123",
            data_source_id="src-456",
        )

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "bird_test_db_catalog"

    async def test_falls_back_to_env_var_when_ddb_empty(self):
        """When DDB lookup returns nothing, falls back to ATHENA_DATABASE env var."""
        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_table.get_item.return_value = {"Item": None}
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-2"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "x"}]},
                    "Rows": [{"Data": [{"VarCharValue": "x"}]}],
                }
            }

            with patch.dict("os.environ", {"ATHENA_DATABASE": "fallback_db"}):
                executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources")

            await executor.execute(
                "SELECT 1 as x",
                namespace="ns-123",
                data_source_id="missing-source",
            )

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "fallback_db"

    async def test_explicit_database_param_skips_ddb(self):
        """When database param is passed explicitly, DDB is not queried."""
        with patch("boto3.client") as mock_boto, patch("boto3.resource") as mock_res:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_table = MagicMock()
            mock_res.return_value.Table.return_value = mock_table

            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-3"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.return_value = {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "x"}]},
                    "Rows": [{"Data": [{"VarCharValue": "x"}]}],
                }
            }

            executor = AthenaQueryExecutor(region="us-west-2", sources_table="coa-sources")

        await executor.execute(
            "SELECT 1 as x",
            namespace="ns-123",
            database="explicit_db",
        )

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == "explicit_db"
        mock_table.get_item.assert_not_called()


@pytest.mark.unit
class TestResultPagination:
    """Athena returns at most 1000 rows per call; results must page via NextToken.

    Regression coverage for the silent-truncation bug: `_get_results` issued a
    single GetQueryResults call and discarded `NextToken`, so every result set
    was capped at 999 rows (1000 minus the header) regardless of max_rows. It
    surfaced in benchmarking as four TPC-H queries returning exactly 999 rows.
    """

    @staticmethod
    def _page(start: int, count: int, *, header: bool, token: str | None):
        """Build one GetQueryResults page. The header row appears ONLY on page 1."""
        rows = []
        if header:
            rows.append({"Data": [{"VarCharValue": "id"}]})
        rows += [{"Data": [{"VarCharValue": str(i)}]} for i in range(start, start + count)]
        page = {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}]}, "Rows": rows}}
        if token:
            page["NextToken"] = token
        return page

    async def _run(self, pages, max_rows):
        with patch("boto3.client") as mock_boto:
            mock_athena = MagicMock()
            mock_boto.return_value = mock_athena
            mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
            mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
            mock_athena.get_query_results.side_effect = pages
            with patch("boto3.resource"):
                ex = AthenaQueryExecutor(region="us-east-1", sources_table="t")
            res = await ex.execute("SELECT id FROM t", namespace="demo", database="db", max_rows=max_rows)
            return res, mock_athena

    async def test_follows_next_token_across_pages(self):
        """2500 rows across 3 pages must all be returned, not truncated at 999."""
        pages = [
            self._page(0, 1000, header=True, token="t1"),
            self._page(1000, 1000, header=False, token="t2"),
            self._page(2000, 500, header=False, token=None),
        ]
        res, mock_athena = await self._run(pages, 5000)

        assert res.row_count == 2500, f"expected all 2500 rows, got {res.row_count}"
        assert mock_athena.get_query_results.call_count == 3
        assert res.truncated is False
        # No row lost or duplicated at page boundaries.
        assert res.rows[0]["id"] == "0"
        assert res.rows[999]["id"] == "999"
        assert res.rows[1000]["id"] == "1000"
        assert res.rows[-1]["id"] == "2499"

    async def test_header_stripped_only_on_first_page(self):
        """Stripping rows[1:] on every page would drop one real row per page."""
        pages = [
            self._page(0, 3, header=True, token="t1"),
            self._page(3, 3, header=False, token=None),
        ]
        res, _ = await self._run(pages, 100)
        assert [r["id"] for r in res.rows] == ["0", "1", "2", "3", "4", "5"]

    async def test_stops_at_max_rows_and_flags_truncated(self):
        pages = [
            self._page(0, 1000, header=True, token="t1"),
            self._page(1000, 1000, header=False, token="t2"),
        ]
        res, mock_athena = await self._run(pages, 1500)
        assert res.row_count == 1500
        assert res.truncated is True
        assert mock_athena.get_query_results.call_count == 2

    async def test_single_page_needs_one_call(self):
        pages = [self._page(0, 5, header=True, token=None)]
        res, mock_athena = await self._run(pages, 1000)
        assert res.row_count == 5
        assert res.truncated is False
        assert mock_athena.get_query_results.call_count == 1

    async def test_empty_result_set(self):
        pages = [self._page(0, 0, header=True, token=None)]
        res, _ = await self._run(pages, 1000)
        assert res.row_count == 0
        assert res.truncated is False


@pytest.mark.unit
class TestInjectLimitTrailingComment:
    """Regression: a trailing ``-- comment`` line must not swallow the appended LIMIT.

    Every textual-append fallback in ``_inject_limit`` used to append
    `` LIMIT n`` to the last line. LLM-generated SQL frequently ends with a
    ``-- comment`` line, so the cap landed inside the comment: the query
    parsed fine server-side and executed WITHOUT the scan cap.
    """

    def test_no_limit_with_trailing_comment_appends_on_new_line(self):
        sql = "SELECT a FROM t\n-- assumption: default scope"
        out = AthenaQueryExecutor._inject_limit(sql, 1000)
        assert out.splitlines()[-1].strip() == "LIMIT 1000", out

    def test_parse_failure_with_trailing_comment_appends_on_new_line(self):
        sql = "NOT VALID SQL AT ALL\n-- trailing"
        out = AthenaQueryExecutor._inject_limit(sql, 500)
        assert out.splitlines()[-1].strip() == "LIMIT 500", out

    def test_normal_append_still_caps(self):
        out = AthenaQueryExecutor._inject_limit("SELECT a FROM t", 100)
        assert "LIMIT 100" in out

    def test_existing_outer_limit_within_cap_unchanged(self):
        sql = "SELECT a FROM t LIMIT 5"
        assert AthenaQueryExecutor._inject_limit(sql, 100) == sql
