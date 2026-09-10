# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests: the local scan worker writes only contract-valid SourceStatus values.

The Smithy enum (models/src/main/smithy/unified-sources.smithy SourceStatus) is
the contract the gateway's pydantic models enforce via GetSourceOutput — a
status string outside the enum made GET /namespaces/{ns}/sources/{id} return
500 for the entire run (the 2026-09-10 apollo13-transcripts incident). These
tests parse docker/scan-workers/worker.py's AST and check every literal status
passed to _update_source_status is a member of the generated enum, so a future
regression fails in CI instead of in a running ingestion.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WORKER = REPO_ROOT / "docker" / "scan-workers" / "worker.py"


def _source_status_members() -> set[str]:
    """Parse the enum members from the Smithy model (source of truth)."""
    smithy = REPO_ROOT / "models" / "src" / "main" / "smithy" / "unified-sources.smithy"
    text = smithy.read_text()
    block = text.split("enum SourceStatus {", 1)[1].split("}", 1)[0]
    members: set[str] = set()
    for line in block.splitlines():
        token = line.strip().split()[0] if line.strip() else ""
        if token and token.isupper() and token.replace("_", "").isalpha():
            members.add(token)
    return members


def _literal_statuses() -> list[tuple[int, str]]:
    """Every literal string passed to _update_source_status in worker.py."""
    tree = ast.parse(WORKER.read_text())
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_target = (isinstance(func, ast.Name) and func.id == "_update_source_status") or (
            isinstance(func, ast.Attribute) and func.attr == "_update_source_status"
        )
        if not is_target or len(node.args) < 3:
            continue
        status = node.args[2]
        if isinstance(status, ast.Constant) and isinstance(status.value, str):
            found.append((status.lineno, status.value))
    return found


def test_worker_module_imports_cleanly() -> None:
    """The worker module itself imports (boto3 available in the uv workspace)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("coa_local_worker_under_test", WORKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with patch.dict("sys.modules", {"boto3": MagicMock(), "botocore": MagicMock(), "botocore.config": MagicMock()}):
        spec.loader.exec_module(module)
    assert callable(module.run_doc_ingestion)


def test_every_source_status_write_is_in_the_enum() -> None:
    """No literal status write may fall outside SourceStatus (INGESTING did)."""
    members = _source_status_members()
    assert "SCANNING" in members
    assert "SCANNING_KG_BUILD" in members
    assert "INGESTING" not in members  # the regression this test pins
    literals = _literal_statuses()
    assert literals, "expected at least one literal status write in worker.py"
    bad = [(ln, s) for ln, s in literals if s not in members]
    assert not bad, f"non-enum source statuses written by the worker: {bad}"


def test_doc_ingestion_writes_scanning_then_kg_build() -> None:
    """Document ingestion stamps SCANNING before preprocess, SCANNING_KG_BUILD around kg-build."""
    source = WORKER.read_text()
    scanning = 'doc_source_id, namespace_id, "SCANNING")' in source
    kg_build = 'doc_source_id, namespace_id, "SCANNING_KG_BUILD")' in source
    assert scanning, "doc ingestion must stamp SCANNING (not INGESTING) before preprocess"
    assert kg_build, "doc ingestion must stamp SCANNING_KG_BUILD around run_kg_build"
    # Order: the SCANNING stamp precedes the SCANNING_KG_BUILD stamp.
    assert source.index('doc_source_id, namespace_id, "SCANNING")') < source.index(
        'doc_source_id, namespace_id, "SCANNING_KG_BUILD")'
    )