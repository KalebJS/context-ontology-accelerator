# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit test: structured induction persists ALL table-level matches to S3 (#908).

Regression test for the [:50] slice bug that dropped matches > 50 from BOTH the
DynamoDB metadata and the S3 artifact. The fix removes the slice so the full
match list reaches create_proposal → put_proposal_matches_s3.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from coa_ontology.inducer.services.data_catalog import CatalogTable

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _isolate_induce_catalog_globals():
    """Reset induce_catalog module globals after each test."""
    from coa_ontology import induce_catalog

    jobs_snapshot = dict(induce_catalog._jobs)
    namespaces_snapshot = dict(induce_catalog._job_namespaces)
    try:
        yield
    finally:
        induce_catalog._jobs.clear()
        induce_catalog._jobs.update(jobs_snapshot)
        induce_catalog._job_namespaces.clear()
        induce_catalog._job_namespaces.update(namespaces_snapshot)


def _schemas_mod():
    return __import__(
        "coa_ontology.inducer.schemas",
        fromlist=["JobStatus", "InductionReport", "ConceptMatch"],
    )


def test_all_table_matches_reach_s3_no_truncation() -> None:
    """An induction with >50 table-level matches persists ALL of them to S3.

    Before the fix: line 955 in induce_catalog.py applied [:50] before passing
    matches to create_proposal, so S3 got only the first 50. After the fix: the
    slice is removed, and create_proposal → put_proposal_matches_s3 writes the
    full list.

    This test asserts that the matches list passed to create_proposal contains
    ALL 75 table-level matches (no [:50] slice).
    """
    from coa_ontology.induce_catalog import (
        WorkbenchInductionRequest,
        _jobs,
        _run_induction,
    )

    schemas = _schemas_mod()
    ConceptMatch = schemas.ConceptMatch
    JobResponse = schemas.JobResponse
    JobStatus = schemas.JobStatus

    job = JobResponse(job_id="job-75-tables", status=JobStatus.PENDING, created_at="2026-08-11T00:00:00Z")
    _jobs["job-75-tables"] = job

    body = WorkbenchInductionRequest(
        datasource_ids=["bird-postgres"],
        ontology_uri_prefix="http://test.org/bird#",
    )

    # Generate 75 table-level matches (source_column == ""), mix of exact,
    # high_confidence, and novel to match the live repro.
    matches = []
    for i in range(75):
        if i < 17:
            match_type = "exact"
            matched_uri = f"http://schema.org/Table{i}"
            matched_ont_id = "http://schema.org"
            sim = 1.0
        elif i < 30:
            match_type = "high_confidence"
            matched_uri = f"http://fibo.org/Table{i}"
            matched_ont_id = "http://fibo.org"
            sim = 0.85
        else:
            match_type = "novel"
            matched_uri = None
            matched_ont_id = None
            sim = None

        matches.append(
            ConceptMatch(
                source_column="",
                source_table=f"t{i}",
                matched_class_uri=matched_uri,
                matched_ontology_id=matched_ont_id,
                similarity=sim,
                match_type=match_type,
            )
        )

    # Add a column-level match to verify it's filtered out
    matches.append(
        ConceptMatch(
            source_column="id",
            source_table="t0",
            matched_class_uri="http://schema.org/identifier",
            matched_ontology_id="http://schema.org",
            similarity=0.9,
            match_type="high_confidence",
        )
    )

    class _FakeStrategy:
        def induce(self, **kwargs):
            fake_graph = MagicMock()
            fake_graph.serialize.return_value = "@prefix : <#> ."
            return fake_graph, {f"t{i}" for i in range(75)}, matches, []

        def build_r2rml(self, **kwargs):
            fake_graph = MagicMock()
            fake_graph.serialize.return_value = "@prefix rr: <#> ."
            return fake_graph

    fake_constraint = MagicMock()
    fake_constraint.model_dump.return_value = {}

    with (
        patch(
            "coa_ontology.induce_catalog._fetch_catalog_from_smus",
            return_value={"databases": [{"name": "db"}], "sourceType": "POSTGRES"},
        ),
        patch(
            "coa_ontology.induce_catalog._catalog_to_tables",
            return_value=[
                {
                    "id": f"t-t{i}",
                    "name": f"t{i}",
                    "fullyQualifiedName": f"db.t{i}",
                    "columns": [{"name": "id", "dataType": "INTEGER"}],
                }
                for i in range(75)
            ],
        ),
        patch("coa_ontology.induce_catalog.get_strategy", return_value=_FakeStrategy),
        patch("coa_ontology.stores.build_stores", return_value=(MagicMock(), MagicMock())),
        patch("coa_ontology.stores.adapters.StoreOntologyCatalogAdapter", return_value=MagicMock()),
        patch(
            "coa_ontology.validation.shapes.config.generate_config_from_db",
            return_value=fake_constraint,
        ),
        patch("coa_ontology.induce_catalog.os.makedirs"),
        patch("builtins.open", MagicMock()),
        patch("coa_ontology.induce_catalog.dynamo_store") as mock_dynamo,
        patch("coa_ontology.induce_catalog.emit_induction_job_metrics"),
    ):
        mock_dynamo.list_proposals.return_value = []
        mock_dynamo.list_ontologies_registry.return_value = []
        mock_dynamo.put_proposal_matches_s3.return_value = "s3://bucket/key"

        _run_induction(
            job_id="job-75-tables",
            body=body,
            namespace="bird-ns",
            config={"catalog_source": "smus"},
            build_pipeline_fn=lambda _config: MagicMock(),
            schemas_mod=schemas,
            catalog_table_cls=CatalogTable,
        )

    assert job.status == JobStatus.COMPLETED

    # create_proposal was called with metadata containing ALL 75 table-level
    # matches. Before the fix, this would have been truncated to 50.
    create_proposal_calls = mock_dynamo.create_proposal.call_args_list
    assert len(create_proposal_calls) == 1
    metadata = create_proposal_calls[0].kwargs["metadata"]
    matches_to_create_proposal = metadata["matches"]

    # The regression: if this is 50, the [:50] slice was not removed.
    assert len(matches_to_create_proposal) == 75, (
        f"Expected 75 table-level matches to reach create_proposal, got {len(matches_to_create_proposal)}. "
        "If this is 50, the [:50] slice was not removed."
    )

    # Verify the match types histogram: 17 exact + 13 high_confidence + 45 novel = 75
    dumped_matches = [m if isinstance(m, dict) else m for m in matches_to_create_proposal]
    # Count by match_type field (works whether m is dict or has .match_type attr)
    exact_count = sum(
        1 for m in dumped_matches if (m.get("match_type") if isinstance(m, dict) else m.match_type) == "exact"
    )
    high_conf_count = sum(
        1 for m in dumped_matches if (m.get("match_type") if isinstance(m, dict) else m.match_type) == "high_confidence"
    )
    novel_count = sum(
        1 for m in dumped_matches if (m.get("match_type") if isinstance(m, dict) else m.match_type) == "novel"
    )

    assert exact_count == 17, f"Expected 17 exact matches, got {exact_count}"
    assert high_conf_count == 13, f"Expected 13 high_confidence matches, got {high_conf_count}"
    assert novel_count == 45, f"Expected 45 novel matches, got {novel_count}"


def test_all_column_level_matches_yields_empty_table_matches() -> None:
    """When every match is column-level, the persisted table-level list is empty.

    ``create_proposal`` filters matches to table-level (``source_column == ""``);
    a run where every match carries a ``source_column`` must persist ``[]``, not
    crash and not leak column-level entries into the proposal's grounding list.
    """
    from coa_ontology.induce_catalog import (
        WorkbenchInductionRequest,
        _jobs,
        _run_induction,
    )

    schemas = _schemas_mod()
    ConceptMatch = schemas.ConceptMatch
    JobResponse = schemas.JobResponse
    JobStatus = schemas.JobStatus

    job = JobResponse(job_id="job-cols-only", status=JobStatus.PENDING, created_at="2026-08-11T00:00:00Z")
    _jobs["job-cols-only"] = job

    body = WorkbenchInductionRequest(
        datasource_ids=["bird-postgres"],
        ontology_uri_prefix="http://test.org/bird#",
    )

    # Only column-level matches (source_column != "") — no table-level entries.
    matches = [
        ConceptMatch(
            source_column=f"col{i}",
            source_table="t0",
            matched_class_uri="http://schema.org/identifier",
            matched_ontology_id="http://schema.org",
            similarity=0.9,
            match_type="high_confidence",
        )
        for i in range(3)
    ]

    class _FakeStrategy:
        def induce(self, **kwargs):
            fake_graph = MagicMock()
            fake_graph.serialize.return_value = "@prefix : <#> ."
            return fake_graph, {"t0"}, matches, []

        def build_r2rml(self, **kwargs):
            fake_graph = MagicMock()
            fake_graph.serialize.return_value = "@prefix rr: <#> ."
            return fake_graph

    fake_constraint = MagicMock()
    fake_constraint.model_dump.return_value = {}

    with (
        patch(
            "coa_ontology.induce_catalog._fetch_catalog_from_smus",
            return_value={"databases": [{"name": "db"}], "sourceType": "POSTGRES"},
        ),
        patch(
            "coa_ontology.induce_catalog._catalog_to_tables",
            return_value=[
                {
                    "id": "t-t0",
                    "name": "t0",
                    "fullyQualifiedName": "db.t0",
                    "columns": [{"name": "id", "dataType": "INTEGER"}],
                }
            ],
        ),
        patch("coa_ontology.induce_catalog.get_strategy", return_value=_FakeStrategy),
        patch("coa_ontology.stores.build_stores", return_value=(MagicMock(), MagicMock())),
        patch("coa_ontology.stores.adapters.StoreOntologyCatalogAdapter", return_value=MagicMock()),
        patch(
            "coa_ontology.validation.shapes.config.generate_config_from_db",
            return_value=fake_constraint,
        ),
        patch("coa_ontology.induce_catalog.os.makedirs"),
        patch("builtins.open", MagicMock()),
        patch("coa_ontology.induce_catalog.dynamo_store") as mock_dynamo,
        patch("coa_ontology.induce_catalog.emit_induction_job_metrics"),
    ):
        mock_dynamo.list_proposals.return_value = []
        mock_dynamo.list_ontologies_registry.return_value = []
        mock_dynamo.put_proposal_matches_s3.return_value = "s3://bucket/key"

        _run_induction(
            job_id="job-cols-only",
            body=body,
            namespace="bird-ns",
            config={"catalog_source": "smus"},
            build_pipeline_fn=lambda _config: MagicMock(),
            schemas_mod=schemas,
            catalog_table_cls=CatalogTable,
        )

    assert job.status == JobStatus.COMPLETED
    create_proposal_calls = mock_dynamo.create_proposal.call_args_list
    assert len(create_proposal_calls) == 1
    metadata = create_proposal_calls[0].kwargs["metadata"]
    # Every match was column-level → the table-level grounding list is empty.
    assert metadata["matches"] == []
