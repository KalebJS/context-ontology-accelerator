# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for AthenaConnector — discovery of a Lambda-backed Athena catalog.

The statement runner is stubbed at its ``run`` boundary, so these tests pin the
connector's own behaviour: the DESCRIBE row shape (three logical fields packed
into one tab-separated cell, verified live), tag extraction, filtering, and the
fail-soft-per-table accounting that keeps a degraded scan from looking complete.
"""

from __future__ import annotations

import pytest
from coa_common.domain_models import EnrichmentSource
from coa_sources.database.connectors import CONNECTOR_REGISTRY, get_connector
from coa_sources.database.connectors.athena_connector import (
    AthenaConnector,
    _parse_describe_rows,
)
from coa_sources.database.connectors.athena_statement import (
    AthenaStatementFailed,
    AthenaStatementTimeout,
)

pytestmark = pytest.mark.unit

_CATALOG = "coadevds_abc123"
_DATABASE = "widgets"


def _packed(name: str, data_type: str, comment: str = "") -> list[str]:
    """A DESCRIBE row as Athena actually returns it: one tab-separated cell."""
    return [f"{name}\t{data_type}\t{comment}"]


class FakeRunner:
    """Stubs AthenaStatementRunner.run, keyed on the statement's shape."""

    def __init__(
        self,
        *,
        databases: list[str] | None = None,
        tables: list[str] | None = None,
        describes: dict[str, list[list[str]]] | None = None,
        describe_errors: dict[str, Exception] | None = None,
        databases_error: Exception | None = None,
        tables_error: Exception | None = None,
    ) -> None:
        self.databases = databases if databases is not None else [_DATABASE]
        self.tables = tables if tables is not None else []
        self.describes = describes or {}
        self.describe_errors = describe_errors or {}
        self.databases_error = databases_error
        self.tables_error = tables_error
        self.statements: list[str] = []
        self.header_modes: list[str] = []

    def run(self, statement, *, header_row="auto", **_kwargs):
        self.statements.append(statement)
        self.header_modes.append(header_row)
        if statement.startswith("SHOW DATABASES"):
            if self.databases_error:
                raise self.databases_error
            return [[d] for d in self.databases]
        if statement.startswith("SHOW TABLES"):
            if self.tables_error:
                raise self.tables_error
            return [[t] for t in self.tables]
        if statement.startswith("DESCRIBE"):
            table = statement.rsplit(".", 1)[-1]
            if table in self.describe_errors:
                raise self.describe_errors[table]
            return self.describes.get(table, [])
        raise AssertionError(f"unexpected statement: {statement}")


def _config(**overrides) -> dict:
    base = {
        "athena_data_catalog_name": _CATALOG,
        "database_name": _DATABASE,
        "data_source_id": "DS#src-1",
        "namespace_id": "ns-1",
    }
    base.update(overrides)
    return base


def _statement_error(kind: str = "failed") -> Exception:
    if kind == "timeout":
        return AthenaStatementTimeout("DESCRIBE x", 120.0)
    return AthenaStatementFailed("DESCRIBE x", "FAILED", "connector exploded")


class TestRegistry:
    def test_athena_connector_is_registered_for_the_sub_type(self):
        assert CONNECTOR_REGISTRY["ATHENA_CONNECTOR"] is AthenaConnector

    def test_get_connector_returns_an_instance(self):
        assert isinstance(get_connector("ATHENA_CONNECTOR"), AthenaConnector)


class TestParseDescribeRows:
    # The shape verified live: three advertised columns, one packed cell.
    def test_splits_a_tab_packed_cell(self):
        rows = [_packed("customer_id", "bigint", "Surrogate key")]
        assert [(n, t, c.description) for n, t, c in _parse_describe_rows(rows)] == [
            ("customer_id", "bigint", "Surrogate key")
        ]

    # The output shape is connector- and Athena-version dependent, so a genuinely
    # three-celled row must read positionally rather than being tab-split.
    def test_reads_a_multi_cell_row_positionally(self):
        rows = [["customer_id", "bigint", "Surrogate key"]]
        assert [(n, t, c.description) for n, t, c in _parse_describe_rows(rows)] == [
            ("customer_id", "bigint", "Surrogate key")
        ]

    def test_handles_a_missing_comment(self):
        assert _parse_describe_rows([["email", "varchar"]])[0][2].description == ""
        assert _parse_describe_rows([_packed("email", "varchar")])[0][2].description == ""

    # maxsplit=2, so a comment containing a tab is not truncated.
    def test_a_comment_containing_a_tab_survives(self):
        rows = [["order_id\tbigint\tid\tand more"]]
        assert _parse_describe_rows(rows)[0][2].description == "id\tand more"

    # Hive-style DESCRIBE can emit section markers and blanks. Parsing those into
    # columns would put a column named "#" into the ontology.
    @pytest.mark.parametrize(
        "row",
        [
            [],
            [None],
            [""],
            ["# Partition Information"],
            _packed("# col_name", "data_type"),
            ["just_a_name"],
        ],
    )
    def test_skips_rows_that_are_not_columns(self, row):
        assert _parse_describe_rows([row]) == []

    def test_strips_surrounding_whitespace_from_name_and_type(self):
        assert _parse_describe_rows([[" email ", " varchar "]])[0][:2] == ("email", "varchar")

    # One unparsable row must not cost the surrounding columns.
    def test_a_bad_row_does_not_drop_the_good_ones(self):
        rows = [_packed("a", "int"), ["garbage"], _packed("b", "int")]
        assert [n for n, _, _ in _parse_describe_rows(rows)] == ["a", "b"]


class TestDiscoverMetadata:
    def test_discovers_columns_types_and_comments(self):
        runner = FakeRunner(
            tables=["customers"],
            describes={
                "customers": [
                    _packed("customer_id", "bigint", "Surrogate key"),
                    _packed("email", "varchar", "Primary contact"),
                ]
            },
        )
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.name == "customers"
        assert table.database == _DATABASE
        assert table.data_source_id == "DS#src-1"
        assert table.namespace_id == "ns-1"
        assert [(c.name, c.data_type) for c in table.columns] == [("customer_id", "bigint"), ("email", "varchar")]
        assert table.technical_metadata.column_count == 2

    # A source-system comment is authoritative, so enrichment must not overwrite
    # it — which is what DETERMINISTIC signals to the enricher.
    def test_comments_become_deterministic_descriptions(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int", "The comment")]})
        column = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0].columns[0]
        assert column.business_metadata.description == "The comment"
        assert column.business_metadata.enrichment_source == EnrichmentSource.DETERMINISTIC
        assert column.business_metadata.confidence == 1.0

    # An absent comment must leave business_metadata untouched rather than
    # claiming a DETERMINISTIC empty string, which would block enrichment from
    # filling the gap it exists to fill.
    def test_a_missing_comment_leaves_no_enrichment_source(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int")]})
        column = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0].columns[0]
        assert column.business_metadata.description == ""
        assert column.business_metadata.enrichment_source == ""

    def test_extracts_a_primary_key_from_pk_tags(self):
        runner = FakeRunner(
            tables=["t"],
            describes={
                "t": [
                    _packed("a", "int", "First part @pk"),
                    _packed("b", "int", "Second part @pk"),
                    _packed("c", "int", "Not a key"),
                ]
            },
        )
        table = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0]
        # DESCRIBE order defines the composite key's column order.
        assert table.primary_key.columns == ["a", "b"]
        assert table.primary_key.source == EnrichmentSource.DETERMINISTIC
        # ...and the tag is stripped from what a steward reads.
        assert table.columns[0].business_metadata.description == "First part"

    def test_extracts_foreign_keys_from_fk_tags(self):
        runner = FakeRunner(
            tables=["orders"],
            describes={"orders": [_packed("customer_id", "bigint", "Owner @fk(customers.customer_id)")]},
        )
        table = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0]
        assert len(table.foreign_keys) == 1
        fk = table.foreign_keys[0]
        assert (fk.column, fk.target_table, fk.target_column) == ("customer_id", "customers", "customer_id")
        assert fk.source == EnrichmentSource.DETERMINISTIC
        assert table.columns[0].business_metadata.description == "Owner"

    def test_no_tags_yields_an_empty_primary_key_not_none(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int", "plain")]})
        table = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0]
        assert table.primary_key.columns == []
        assert table.foreign_keys == []

    # The bare DESCRIBE form returns column rows only; a table comment appears
    # solely under EXTENDED/FORMATTED, which federated catalogs reject.
    def test_no_table_level_description_is_claimed(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int", "col comment")]})
        table = AthenaConnector(runner=runner).discover_metadata(_config()).tables[0]
        assert table.business_metadata.description == ""

    def test_uses_unquoted_identifiers_in_every_statement(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int")]})
        AthenaConnector(runner=runner).discover_metadata(_config())
        assert runner.statements == [
            f"SHOW DATABASES IN {_CATALOG}",
            f"SHOW TABLES IN {_CATALOG}.{_DATABASE}",
            f"DESCRIBE {_CATALOG}.{_DATABASE}.t",
        ]
        assert '"' not in "".join(runner.statements)
        assert "`" not in "".join(runner.statements)

    # SHOW/DESCRIBE carry no header row, and the runner would otherwise have to
    # infer that from StatementType. Being explicit means a table never loses its
    # first column to a guess.
    def test_asks_for_no_header_row_on_every_statement(self):
        runner = FakeRunner(tables=["t"], describes={"t": [_packed("c", "int")]})
        AthenaConnector(runner=runner).discover_metadata(_config())
        assert set(runner.header_modes) == {"absent"}

    def test_hash_is_stable_across_runs_and_changes_with_the_schema(self):
        def run(cols):
            runner = FakeRunner(tables=["t"], describes={"t": cols})
            return AthenaConnector(runner=runner).discover_metadata(_config()).tables[0].technical_metadata_hash

        first = run([_packed("a", "int")])
        assert first == run([_packed("a", "int")])
        assert first != run([_packed("a", "bigint")])

    # Completion order from the thread pool is nondeterministic, so an unsorted
    # result would reorder between scans and churn downstream artifacts.
    def test_tables_are_returned_in_a_stable_order(self):
        runner = FakeRunner(
            tables=["zebra", "apple", "mango"],
            describes={t: [_packed("c", "int")] for t in ("zebra", "apple", "mango")},
        )
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert [t.name for t in result.tables] == ["apple", "mango", "zebra"]


class TestFiltering:
    def test_include_filter_keeps_only_matches(self):
        runner = FakeRunner(
            tables=["dim_a", "fact_b", "stg_c"],
            describes={t: [_packed("c", "int")] for t in ("dim_a", "fact_b", "stg_c")},
        )
        result = AthenaConnector(runner=runner).discover_metadata(_config(table_filter="dim_*|fact_*"))
        assert sorted(t.name for t in result.tables) == ["dim_a", "fact_b"]

    def test_exclude_filter_runs_after_the_include_filter(self):
        runner = FakeRunner(
            tables=["dim_a", "dim_tmp"],
            describes={t: [_packed("c", "int")] for t in ("dim_a", "dim_tmp")},
        )
        result = AthenaConnector(runner=runner).discover_metadata(
            _config(table_filter="dim_*", table_exclude_filter="*_tmp")
        )
        assert [t.name for t in result.tables] == ["dim_a"]

    # A filter that matches nothing is a configuration outcome the steward can
    # see and fix, not a scan failure.
    def test_a_filter_matching_nothing_yields_no_tables_and_no_error(self):
        runner = FakeRunner(tables=["a", "b"])
        result = AthenaConnector(runner=runner).discover_metadata(_config(table_filter="nope_*"))
        assert result.tables == []
        assert result.failed_tables == []

    def test_an_empty_database_yields_no_tables(self):
        runner = FakeRunner(tables=[])
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert result.tables == []

    # No DESCRIBE should be issued for a filtered-out table — each one costs the
    # customer a Lambda invocation.
    def test_filtered_out_tables_are_never_described(self):
        runner = FakeRunner(tables=["keep", "drop"], describes={"keep": [_packed("c", "int")]})
        AthenaConnector(runner=runner).discover_metadata(_config(table_filter="keep"))
        assert not any("drop" in s for s in runner.statements)


class TestDegradation:
    # Fail soft per table: one failure must not cost the whole scan...
    @pytest.mark.parametrize("kind", ["failed", "timeout"])
    def test_a_failed_describe_costs_only_that_table(self, kind):
        runner = FakeRunner(
            tables=["good", "bad"],
            describes={"good": [_packed("c", "int")]},
            describe_errors={"bad": _statement_error(kind)},
        )
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert [t.name for t in result.tables] == ["good"]
        # ...but it must be recorded, because the source still advances to review
        # and enrichment fills AI descriptions over the gap.
        assert result.failed_tables == [f"{_DATABASE}.bad"]

    # A table that describes to zero columns is indistinguishable from one we
    # failed to read, and shipping it would put an empty table into the ontology
    # for enrichment to invent descriptions for.
    def test_a_table_with_no_columns_counts_as_failed(self):
        runner = FakeRunner(tables=["ok", "empty"], describes={"ok": [_packed("c", "int")], "empty": []})
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert [t.name for t in result.tables] == ["ok"]
        assert result.failed_tables == [f"{_DATABASE}.empty"]

    # ...and when it is the ONLY table, that is "reachable but no readable
    # schema", which must fail rather than reach review looking empty.
    def test_a_sole_table_with_no_columns_raises(self):
        runner = FakeRunner(tables=["empty"], describes={"empty": []})
        with pytest.raises(RuntimeError, match="failed to describe"):
            AthenaConnector(runner=runner).discover_metadata(_config())

    # A source with tables but no readable schema is not a successful scan — it
    # would reach review looking empty rather than broken.
    def test_every_table_failing_raises(self):
        runner = FakeRunner(
            tables=["a", "b"],
            describe_errors={"a": _statement_error(), "b": _statement_error()},
        )
        with pytest.raises(RuntimeError, match="failed to describe"):
            AthenaConnector(runner=runner).discover_metadata(_config())

    def test_failed_table_names_are_qualified_and_sorted(self):
        runner = FakeRunner(
            tables=["z", "a", "ok"],
            describes={"ok": [_packed("c", "int")]},
            describe_errors={"z": _statement_error(), "a": _statement_error()},
        )
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert result.failed_tables == [f"{_DATABASE}.a", f"{_DATABASE}.z"]

    # An enumeration failure is not partial — nothing was discovered — so it must
    # fail the scan rather than report an empty database.
    def test_a_failure_listing_tables_propagates(self):
        runner = FakeRunner(tables_error=_statement_error())
        with pytest.raises(AthenaStatementFailed):
            AthenaConnector(runner=runner).discover_metadata(_config())

    def test_a_failure_listing_databases_propagates(self):
        runner = FakeRunner(databases_error=_statement_error())
        with pytest.raises(AthenaStatementFailed):
            AthenaConnector(runner=runner).discover_metadata(_config())


class TestConfigurationValidation:
    @pytest.mark.parametrize("missing", ["athena_data_catalog_name", "database_name"])
    def test_missing_required_config_raises(self, missing):
        runner = FakeRunner()
        with pytest.raises(ValueError, match="required"):
            AthenaConnector(runner=runner).discover_metadata(_config(**{missing: ""}))

    # The configured database is authoritative; a mismatch is a config error the
    # steward must see, not something to discover around.
    def test_a_database_the_connector_does_not_expose_raises(self):
        runner = FakeRunner(databases=["other"])
        with pytest.raises(ValueError, match="not found"):
            AthenaConnector(runner=runner).discover_metadata(_config())

    # Sending a quoted identifier would fail with a parser error from whichever
    # of Athena's two parsers rejects that quoting style, so it is caught here.
    @pytest.mark.parametrize("bad", ["my database", "1st", 'has"quote', "with-dash", ""])
    def test_a_database_name_needing_quoting_raises(self, bad):
        runner = FakeRunner(databases=[bad])
        with pytest.raises(ValueError):
            AthenaConnector(runner=runner).discover_metadata(_config(database_name=bad))

    # A table we cannot address is recorded as failed rather than skipped, so the
    # steward learns the connector exposes something out of reach.
    def test_a_table_name_needing_quoting_counts_as_failed(self):
        runner = FakeRunner(tables=["ok", "my table"], describes={"ok": [_packed("c", "int")]})
        result = AthenaConnector(runner=runner).discover_metadata(_config())
        assert [t.name for t in result.tables] == ["ok"]
        assert result.failed_tables == [f"{_DATABASE}.my table"]


class TestTestConnection:
    def test_succeeds_when_the_catalog_and_database_resolve(self):
        runner = FakeRunner()
        result = AthenaConnector(runner=runner).test_connection(_config())
        assert result.success is True
        assert {c.check for c in result.checks} == {"catalog_access", "database_exists"}

    def test_probes_with_show_databases_only(self):
        runner = FakeRunner()
        AthenaConnector(runner=runner).test_connection(_config())
        assert runner.statements == [f"SHOW DATABASES IN {_CATALOG}"]

    # The likeliest cause of this failure is the one thing this service cannot do
    # itself, so the message has to name it.
    def test_a_catalog_failure_reports_the_invoke_grant(self):
        runner = FakeRunner(databases_error=_statement_error())
        result = AthenaConnector(runner=runner).test_connection(_config())
        assert result.success is False
        assert "resource policy" in result.message
        assert "invoke" in result.message
        # Athena's own words are preserved so a connector fault stays
        # distinguishable from a wiring fault.
        assert "connector exploded" in result.message

    def test_a_missing_database_lists_what_the_connector_reports(self):
        runner = FakeRunner(databases=["alpha", "beta"])
        result = AthenaConnector(runner=runner).test_connection(_config(database_name="gamma"))
        assert result.success is False
        assert "alpha" in result.message and "beta" in result.message

    @pytest.mark.parametrize("missing", ["athena_data_catalog_name", "database_name"])
    def test_missing_required_config_fails_without_calling_athena(self, missing):
        runner = FakeRunner()
        result = AthenaConnector(runner=runner).test_connection(_config(**{missing: ""}))
        assert result.success is False
        assert runner.statements == []

    def test_an_unaddressable_name_fails_without_calling_athena(self):
        runner = FakeRunner()
        result = AthenaConnector(runner=runner).test_connection(_config(database_name="my database"))
        assert result.success is False
        assert runner.statements == []
