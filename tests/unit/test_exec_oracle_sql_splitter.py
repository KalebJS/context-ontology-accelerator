# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Oracle seed-script statement splitter.

The splitter decides which statements reach the database. A bug here does not
raise — it silently drops DDL, and the damage shows up much later as a scan that
discovers a partial schema. Cheap to pin, so it is pinned.

``oracledb`` is stubbed: the splitter is pure text handling, and the driver is a
Lambda-bundle dependency that the repo's dev env deliberately does not install
(the loader pulls it ephemerally via ``uv run --with oracledb``).
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "tests" / "cdk" / "scripts" / "exec_oracle_sql.py"
_SEED_SQL = _REPO_ROOT / "tests" / "cdk" / "oracle-seed" / "seed.sql"

# `tests/unit/` is published to the public mirror but `tests/cdk/` is not (see the
# allowlist in .npmignore). Without this guard the module-level load below raises
# FileNotFoundError during collection, which pytest reports as a collection error
# and aborts the WHOLE suite — the mirror's unit-test job fails with exit 2 having
# run nothing. Skip instead: there is no script to pin in that checkout.
if not _SCRIPT.exists() or not _SEED_SQL.exists():
    pytest.skip("tests/cdk/ is absent from this checkout (public mirror)", allow_module_level=True)


def _load() -> ModuleType:
    sys.modules.setdefault("oracledb", types.ModuleType("oracledb"))
    spec = importlib.util.spec_from_file_location("exec_oracle_sql", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_mod = _load()


def test_plsql_block_is_one_statement_terminated_by_slash() -> None:
    """A `;` inside BEGIN..END must not split the block."""
    script = "BEGIN\n  FOR t IN (SELECT 1 FROM dual) LOOP\n    NULL;\n  END LOOP;\nEND;\n/\n"
    statements = _mod.split_statements(script)
    assert len(statements) == 1
    assert statements[0].startswith("BEGIN")
    assert statements[0].endswith("END;")


def test_plain_statements_split_on_semicolon() -> None:
    script = "CREATE TABLE a (x NUMBER);\nCOMMENT ON TABLE a IS 'hi';\nINSERT INTO a VALUES (1);\n"
    assert _mod.split_statements(script) == [
        "CREATE TABLE a (x NUMBER)",
        "COMMENT ON TABLE a IS 'hi'",
        "INSERT INTO a VALUES (1)",
    ]


def test_sqlplus_directives_and_comments_are_dropped() -> None:
    """These have no server-side meaning; sending them would error."""
    script = "WHENEVER SQLERROR EXIT SQL.SQLCODE\n-- a comment\nSET ECHO OFF\nCREATE TABLE a (x NUMBER);\nEXIT\n"
    assert _mod.split_statements(script) == ["CREATE TABLE a (x NUMBER)"]


def test_multiline_statement_is_kept_whole() -> None:
    script = "CREATE TABLE a (\n  x NUMBER(10) NOT NULL,\n  y VARCHAR2(25)\n);\n"
    statements = _mod.split_statements(script)
    assert len(statements) == 1
    assert "y VARCHAR2(25)" in statements[0]


def test_empty_input_yields_nothing() -> None:
    assert _mod.split_statements("") == []
    assert _mod.split_statements("-- only a comment\n\n") == []


def test_real_seed_script_yields_the_expected_ddl() -> None:
    """End-to-end over the actual seed SQL: every table and comment survives.

    Guards the case that matters — a splitter change that quietly drops half the
    schema would otherwise only show up as an Oracle scan finding fewer tables.

    Reads oracle-seed/seed.sql directly (the single source of truth both the
    manual load-oracle-data.sh path and the container's first-boot seed use) —
    not extracted from a heredoc in the shell script, which no longer exists.
    """
    body = _SEED_SQL.read_text()
    statements = _mod.split_statements(body)

    creates = [s for s in statements if s.upper().startswith("CREATE TABLE")]
    comments = [s for s in statements if s.upper().startswith("COMMENT ON")]
    inserts = [s for s in statements if s.upper().startswith("INSERT")]

    assert len(creates) == 8, f"expected the 8 TPC-H tables, got {len(creates)}"
    assert comments, "table/column COMMENTs are what exercise the DETERMINISTIC description path"
    assert inserts, "sample rows are needed for enum sampling to have values"
    assert any(s.strip().upper() == "COMMIT" for s in statements), "the seed must commit"
