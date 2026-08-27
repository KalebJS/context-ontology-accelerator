# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Athena query executor — cross-source SQL execution via AWS Athena.

Executes SQL via Athena (Trino engine) against data sources registered in
the AWS Glue Data Catalog. Used for cross-source federation when VKG-translated
SQL references tables from multiple catalogs.

Security:
- Only SELECT statements allowed (validated via sqlglot AST).
- SQL comments stripped before validation to prevent bypass.
- Workgroup: coa-{namespace} (isolation per namespace).
- Timeout: 120s max poll.
"""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import boto3
import sqlglot
import structlog
from coa_common import resolve_region, sync_boto_config

from ..query_utils import validate_namespace
from ..tier2.sql_firewall import SQLFirewall
from .base import QueryResult, instrumented
from .sources_registry import SourcesRegistry

logger = structlog.get_logger(__name__)


def _append_limit(sql: str, max_rows: int) -> str:
    """Textual-append fallback for ``_inject_limit``.

    The LIMIT goes on its OWN line: LLM-generated SQL frequently ends with a
    ``-- comment`` line (e.g. a stated assumption), and a same-line append
    would land inside that comment — silently removing the scan cap while the
    query still executes.
    """
    return f"{sql.rstrip().rstrip(';')}\nLIMIT {int(max_rows)}"


_POOL_SIZE = int(os.environ.get("ATHENA_THREAD_POOL_SIZE", "3"))
_EXECUTOR = ThreadPoolExecutor(max_workers=_POOL_SIZE, thread_name_prefix="athena")

_POLL_INITIAL_DELAY = 0.5
_POLL_MAX_DELAY = 5.0
_POLL_BACKOFF = 2.0
_DEFAULT_TIMEOUT = 120


class AthenaQueryError(RuntimeError):
    """Raised when Athena query execution fails."""


class AthenaTimeoutError(TimeoutError):
    """Raised when Athena query exceeds timeout."""


_firewall = SQLFirewall()


class AthenaQueryExecutor:
    """Executes SQL via Athena against Glue-cataloged data sources.

    Used for cross-source queries when VKG translation produces SQL
    referencing tables from multiple data sources.

    Database resolution (in priority order):
    1. Explicit ``database`` param passed to execute()
    2. DDB source record lookup (reads glueDatabaseName from the source's row)
    3. ATHENA_DATABASE env var fallback
    """

    def __init__(
        self,
        region: str | None = None,
        workgroup_prefix: str | None = None,
        workgroup: str | None = None,
        output_s3: str | None = None,
        sources_table: str | None = None,
        sources_registry: SourcesRegistry | None = None,
    ):
        """Configure Athena region, workgroup, output location, and sources registry.

        Args:
            region: AWS region. Defaults to the resolved region.
            workgroup_prefix: Prefix for per-namespace workgroups. Defaults to
                ``ATHENA_WORKGROUP_PREFIX`` then ``"coa-"``.
            workgroup: Fixed workgroup that overrides per-namespace resolution.
                Defaults to the ``ATHENA_WORKGROUP`` env var.
            output_s3: Default S3 output location. Defaults to ``ATHENA_OUTPUT_S3``.
            sources_table: DynamoDB sources table name for database lookups.
            sources_registry: Pre-built registry; created from ``sources_table`` when omitted.
        """
        self._region = region or resolve_region()
        self._fixed_workgroup = workgroup or os.environ.get("ATHENA_WORKGROUP")
        self._workgroup_prefix = workgroup_prefix or os.environ.get("ATHENA_WORKGROUP_PREFIX", "coa-")
        self._default_output_s3 = output_s3 or os.environ.get("ATHENA_OUTPUT_S3", "")
        self._default_database = os.environ.get("ATHENA_DATABASE", "default")
        self._athena = boto3.client("athena", region_name=self._region, config=sync_boto_config())
        self._sources = sources_registry or SourcesRegistry(
            table_name=sources_table, region=self._region, executor=_EXECUTOR
        )

    async def close(self) -> None:
        """Release boto3 client resources."""
        self._athena.close()

    @instrumented("athena")
    async def execute(
        self,
        sql: str,
        *,
        namespace: str,
        data_source_id: str = "",
        database: str = "",
        max_rows: int = 1000,
        timeout_seconds: int = _DEFAULT_TIMEOUT,
    ) -> QueryResult:
        """Execute SQL via Athena.

        Args:
            sql: The SQL to execute.
            namespace: Namespace (used for workgroup: coa-{namespace}).
            data_source_id: Data source identifier (used for DDB database lookup).
            database: Glue database name (explicit override; skips DDB lookup).
            max_rows: Maximum rows to return.
            timeout_seconds: Max time to wait for completion (default 120s).

        Returns:
            QueryResult with rows, columns, and metadata.

        Raises:
            AthenaQueryError: On execution failure.
            AthenaTimeoutError: If query exceeds timeout.
            UnsafeSQLError: If SQL contains non-SELECT statements.
        """
        start = time.perf_counter()

        if not isinstance(timeout_seconds, int) or timeout_seconds <= 0 or timeout_seconds > 300:
            raise ValueError(f"timeout_seconds must be 1-300, got {timeout_seconds}")

        validate_namespace(namespace)
        _firewall.validate(sql)

        # inject a dialect-aware LIMIT into the SQL itself (not just the
        # result-fetch cap) so large scans don't execute fully on the Athena side.
        # Mirrors the JDBC path's sqlglot LIMIT injection (source_db._inject_limit).
        sql = self._inject_limit(sql, max_rows)

        resolved_catalog, resolved_db = await self._resolve_catalog_and_database(namespace, data_source_id, database)
        if resolved_catalog:
            original_sql = sql
            sql = self._rewrite_table_names_for_federation(sql, resolved_db)
            logger.info(
                "athena_federation_resolved",
                catalog=resolved_catalog,
                database=resolved_db,
                rewritten=sql != original_sql,
            )
        workgroup = self._fixed_workgroup or await self._resolve_workgroup(namespace)
        query_id = await self._start_query(sql, resolved_db, workgroup, catalog=resolved_catalog)
        await self._wait_for_completion(query_id, timeout_seconds)
        rows, columns, has_more = await self._get_results(query_id, max_rows)

        duration_ms = int((time.perf_counter() - start) * 1000)

        logger.info(
            "athena_query_executed",
            namespace=namespace,
            query_id=query_id,
            duration_ms=duration_ms,
            row_count=len(rows),
            truncated=has_more,
        )

        return QueryResult(
            rows=rows,
            columns=columns,
            row_count=len(rows),
            truncated=has_more,
            duration_ms=duration_ms,
            engine="athena",
        )

    async def health_check(self) -> dict[str, Any]:
        """Probe Athena reachability; returns a status dict, never raises."""
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                _EXECUTOR,
                lambda: self._athena.list_work_groups(MaxResults=1),
            )
            return {"status": "ok", "backend": "athena"}
        except Exception:
            return {"status": "error", "detail": "Athena health check failed"}

    async def _start_query(self, sql: str, database: str, workgroup: str, catalog: str = "") -> str:
        loop = asyncio.get_running_loop()
        context: dict[str, str] = {"Database": database, "Catalog": catalog or "AwsDataCatalog"}
        kwargs: dict[str, Any] = {
            "QueryString": sql,
            "QueryExecutionContext": context,
            "WorkGroup": workgroup,
        }
        if self._default_output_s3:
            kwargs["ResultConfiguration"] = {"OutputLocation": self._default_output_s3}

        response = await loop.run_in_executor(
            _EXECUTOR,
            lambda: self._athena.start_query_execution(**kwargs),
        )
        return response["QueryExecutionId"]

    async def _wait_for_completion(self, query_id: str, timeout_seconds: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = time.perf_counter() + timeout_seconds
        delay = _POLL_INITIAL_DELAY

        while True:
            response = await loop.run_in_executor(
                _EXECUTOR,
                lambda qid=query_id: self._athena.get_query_execution(QueryExecutionId=qid),
            )
            state = response["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return
            if state in ("FAILED", "CANCELLED"):
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                raise AthenaQueryError(f"Athena query {state}: {reason}")

            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                raise AthenaTimeoutError(f"Athena query {query_id} timed out after {timeout_seconds}s")

            await asyncio.sleep(min(delay, max(0, remaining)))
            delay = min(delay * _POLL_BACKOFF, _POLL_MAX_DELAY)

    _ATHENA_PAGE_MAX = 1000
    """Athena GetQueryResults hard limit on rows per call."""

    async def _get_results(self, query_id: str, max_rows: int) -> tuple[list[dict[str, Any]], list[str], bool]:
        """Fetch up to ``max_rows`` result rows, following NextToken across pages.

        Athena caps GetQueryResults at 1000 rows per call and returns a
        ``NextToken`` for the remainder. The previous implementation issued a
        single call and discarded the token, so every result set was silently
        truncated to 999 rows (1000 minus the header) no matter how large
        ``max_rows`` was — ``has_more`` was computed and then ignored by callers.

        Note the Athena quirk this loop depends on: the header row is present
        ONLY on the first page. Stripping ``rows[1:]`` unconditionally would eat
        one real data row per subsequent page.
        """
        loop = asyncio.get_running_loop()
        rows: list[dict[str, Any]] = []
        columns: list[str] = []
        next_token: str | None = None
        first_page = True
        truncated = False

        while True:
            # Request only what is still needed; +1 on the first page for the header.
            want = max_rows - len(rows) + (1 if first_page else 0)
            page_size = max(1, min(want, self._ATHENA_PAGE_MAX))
            kwargs: dict[str, Any] = {"QueryExecutionId": query_id, "MaxResults": page_size}
            if next_token:
                kwargs["NextToken"] = next_token

            # Bind kwargs as a default arg: a bare closure would late-bind and
            # every iteration would re-request the same page.
            response = await loop.run_in_executor(
                _EXECUTOR,
                lambda kw=kwargs: self._athena.get_query_results(**kw),
            )

            result_set = response["ResultSet"]
            page_rows = result_set.get("Rows", [])
            if first_page:
                columns = [col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]
                page_rows = page_rows[1:] if page_rows else []
                first_page = False

            for row in page_rows:
                if len(rows) >= max_rows:
                    truncated = True
                    break
                row_dict = {}
                for i, datum in enumerate(row.get("Data", [])):
                    if i < len(columns):
                        row_dict[columns[i]] = datum.get("VarCharValue")
                rows.append(row_dict)

            next_token = response.get("NextToken")
            if not next_token or len(rows) >= max_rows:
                break

        return rows, columns, bool(next_token) or truncated

    @staticmethod
    def _inject_limit(sql: str, max_rows: int) -> str:
        """Inject/cap the OUTER-query LIMIT for the Trino/Athena dialect.

        the JDBC path injects LIMIT via sqlglot; the Athena path
        previously relied only on the result-fetch cap, so a query without a
        LIMIT still scanned the full table server-side. This caps the scan.

        Operates ONLY on the top-level SELECT's own LIMIT (``parsed.args["limit"]``)
        — NEVER a descendant/subquery LIMIT. Mutating an inner LIMIT would change
        results (e.g. capping ``(SELECT * FROM t LIMIT 2000)`` feeding a COUNT),
        and a small inner LIMIT must not be mistaken for an outer cap.

        - Outer query has no LIMIT → append ``LIMIT {max_rows}``.
        - Outer integer LIMIT > max_rows → lower it to the cap.
        - Outer non-integer LIMIT (param/expr) → replace with the cap.
        - Outer integer LIMIT ≤ max_rows → unchanged.
        Falls back to a textual append if parsing fails or the parsed root is
        not a simple SELECT we can reason about (never raises).
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="trino")
        except Exception:
            return _append_limit(sql, max_rows)

        # Only the OUTER query's own limit — args.get("limit"), not find() which
        # descends into subqueries. Set-ops (UNION/EXCEPT) and non-Select roots
        # don't carry an outer .args["limit"] we can safely edit → textual append.
        if not isinstance(parsed, sqlglot.exp.Select):
            return _append_limit(sql, max_rows)

        limit_node = parsed.args.get("limit")
        if limit_node is None:
            return _append_limit(sql, max_rows)

        existing = limit_node.expression
        # Replace the outer LIMIT when it is non-integer (param/expr) OR an integer
        # above the cap; leave an integer LIMIT already within the cap unchanged.
        non_integer = not existing or not getattr(existing, "is_int", False)
        if non_integer or int(existing.this) > max_rows:
            limit_node.set("expression", sqlglot.exp.Literal.number(max_rows))
        else:
            return sql  # outer LIMIT already within cap — unchanged

        try:
            return parsed.sql(dialect="trino")
        except Exception:
            return _append_limit(sql, max_rows)

    @staticmethod
    def _rewrite_table_names_for_federation(sql: str, schema: str) -> str:
        """Rewrite VKG-generated table names for federated catalog queries.

        Fallback for when R2RML uses Glue-crawled names (e.g.,
        BIRD_PUBLIC_INCOME) but the federated PostgreSQL connector expects
        the actual PG table name (income). When VKG provides physical names
        via datasourceRouting, this becomes a no-op.
        Only called for federated JDBC sources (athenaDataCatalogName set).
        """
        if not schema:
            return sql

        try:
            parsed = sqlglot.parse_one(sql, dialect="trino")
        except Exception:
            return sql

        schema_upper = schema.upper() + "_"
        for table in parsed.find_all(sqlglot.exp.Table):
            name = table.name
            upper_name = name.upper()
            idx = upper_name.find(schema_upper)
            if idx >= 0:
                raw_table = name[idx + len(schema_upper) :]
                table.set("this", sqlglot.exp.to_identifier(raw_table.lower(), quoted=True))

        return parsed.sql(dialect="trino")

    async def _resolve_workgroup(self, namespace: str) -> str:
        """Resolve workgroup: DDB namespace record > prefix-based fallback."""
        workgroup = await self._sources.resolve_workgroup(namespace)
        if workgroup:
            return workgroup
        return f"{self._workgroup_prefix}{namespace}"

    async def _resolve_catalog_and_database(
        self, namespace: str, data_source_id: str, explicit_database: str
    ) -> tuple[str, str]:
        """Resolve Athena catalog and database for query execution.

        For federated JDBC sources (athenaDataCatalogName set), the catalog is
        the nested Glue catalog name and database is the first discovered schema.
        For native Glue sources, catalog is from athenaCatalog (usually
        AwsDataCatalog) and database is athenaDatabase.

        Returns ("", db) when no federated catalog is needed (Glue-native path).
        Returns (catalog, schema) for federated JDBC sources.
        Skips sources where queryable is explicitly False.
        """
        if explicit_database:
            return "", explicit_database

        if not self._sources.available:
            return "", self._default_database

        if not data_source_id or data_source_id == "default":
            # Fallback: no routing info available — scan DDB for any DATABASE source.
            # Works for single-source namespaces; ambiguous for multi-source.
            source = await self._sources.find_database_source(namespace)
        else:
            # Targeted lookup by source ID (from VKG datasourceRouting or caller).
            source = await self._sources.get_source(namespace, data_source_id)

        if not source:
            return "", self._default_database

        if source.get("queryable") is False:
            logger.warning("source_not_queryable", namespace=namespace, source_id=data_source_id)
            return "", self._default_database

        # JDBC federated path: Catalog=nested catalog, Database=schema
        federated_catalog = source.get("athenaDataCatalogName") or ""
        if federated_catalog:
            discovered = source.get("discoveredSchemas") or []
            schema = discovered[0] if discovered else "public"
            logger.info(
                "catalog_resolution",
                path="federated",
                catalog=federated_catalog,
                schema=schema,
                schema_source="discoveredSchemas" if discovered else "default",
                namespace=namespace,
            )
            return federated_catalog, schema

        # Glue-native path: use athenaCatalog/athenaDatabase if available
        athena_db = source.get("athenaDatabase") or source.get("glueDatabaseName") or ""
        if athena_db:
            logger.info("catalog_resolution", path="glue_native", database=athena_db, namespace=namespace)
            return "", athena_db

        config = self._sources.parse_configuration(source)
        db = config.get("databaseName", "") or self._default_database
        logger.info("catalog_resolution", path="config_fallback", database=db, namespace=namespace)
        return "", db
