# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Tier 2 T-Box context builder."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from coa_serve.tier2.ontop.tbox_context import (
    MetricContext,
    TBoxContext,
    TBoxContextBuilder,
)
from coa_serve.tier2.ontop.types import VectorHit


def _make_graph_client(results=None):
    """Create a mock GraphClient that returns the given results."""
    client = AsyncMock()
    client.query.return_value = results or []
    return client


def _make_ontology_hit(uri: str, score: float = 0.85, entity_id: str = "e1") -> VectorHit:
    return VectorHit(
        type="ontology_class",
        score=score,
        entity_id=entity_id,
        uri=uri,
        metadata={"type": "ontology_class", "entity_id": entity_id, "uri": uri},
    )


def _make_metric_hit(name: str = "revenue", score: float = 0.9) -> VectorHit:
    return VectorHit(
        type="metric",
        score=score,
        entity_id=f"m-{name}",
        metadata={
            "type": "metric",
            "entity_id": f"m-{name}",
            "name": name,
            "description": f"Total {name}",
            "formula": f"SUM({name})",
            "dimensions": ["region", "quarter"],
        },
    )


@pytest.mark.unit
class TestTBoxContextBuilder:
    async def test_build_with_ontology_hits(self):
        graph_results = [
            {
                "class": "http://example.org/ontology#Order",
                "label": "Order",
                "parentClass": None,
                "property": "http://example.org/ontology#hasAmount",
                "propLabel": "hasAmount",
                "range": "xsd:decimal",
            },
            {
                "class": "http://example.org/ontology#Customer",
                "label": "Customer",
                "parentClass": None,
                "property": "http://example.org/ontology#hasName",
                "propLabel": "hasName",
                "range": "xsd:string",
            },
        ]
        client = _make_graph_client(graph_results)
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        hits = [
            _make_ontology_hit("http://example.org/ontology#Order"),
            _make_ontology_hit("http://example.org/ontology#Customer"),
        ]

        context = await builder.build(hits, "demo")

        assert len(context.classes) == 2
        assert len(context.properties) == 2
        assert context.classes[0]["uri"] == "http://example.org/ontology#Order"
        assert context.classes[0]["label"] == "Order"

    async def test_build_with_metric_hits(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        hits = [_make_metric_hit("revenue"), _make_metric_hit("orders")]

        context = await builder.build(hits, "demo", query="show revenue")

        assert len(context.metrics) == 2
        assert context.metrics[0].name == "revenue"
        assert context.metrics[0].formula == "SUM(revenue)"
        assert context.metrics[0].dimensions == ["region", "quarter"]

    async def test_build_with_no_hits_uses_entity_fallback(self):
        graph_results = [
            {
                "class": "http://example.org/ontology#Policy",
                "label": "Policy",
                "parentClass": None,
                "property": None,
                "propLabel": None,
                "range": None,
            }
        ]
        client = _make_graph_client(graph_results)
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        context = await builder.build([], "demo", query="How many policies exist?")

        assert len(context.classes) >= 1
        # count query + entity fallback, which is now TWO queries (matched classes,
        # then their datatype properties) so property-row volume can no longer evict
        # a matched class. OP skipped: 1 class; aiContext skipped: no URIs. The
        # isMapped bridge probe uses the separate .ask method, so it does not add to
        # .query's count.
        assert client.query.call_count == 3

    async def test_build_empty_result_from_neptune(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        hits = [_make_ontology_hit("http://example.org/ontology#Missing")]
        context = await builder.build(hits, "demo")

        assert context.classes == []
        assert context.properties == []

    async def test_build_handles_neptune_error(self):
        client = AsyncMock()
        client.query.side_effect = RuntimeError("Connection failed")
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        hits = [_make_ontology_hit("http://example.org/ontology#Order")]
        context = await builder.build(hits, "demo")

        assert context.classes == []
        assert context.properties == []

    async def test_build_truncates_when_over_budget(self):
        # Generate enough results to exceed token budget
        graph_results = []
        for i in range(100):
            graph_results.append(
                {
                    "class": f"http://example.org/ontology#Class{i}",
                    "label": f"Class{i}",
                    "parentClass": None,
                    "property": f"http://example.org/ontology#prop{i}",
                    "propLabel": f"prop{i}",
                    "range": "xsd:string",
                }
            )
        client = _make_graph_client(graph_results)
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        hits = [_make_ontology_hit(f"http://example.org/ontology#Class{i}") for i in range(20)]
        context = await builder.build(hits, "demo", max_tokens=500)

        # Should be truncated
        assert context.token_estimate <= 500

    async def test_token_estimation(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        estimate = builder._estimate_tokens(
            [{"uri": "x", "label": "X"}] * 5,
            [{"uri": "y", "label": "Y", "domain": "x", "range": "z"}] * 10,
            [MetricContext(name="m1")] * 3,
        )
        # 5*20 + 10*30 + 3*40 = 100 + 300 + 120 = 520
        assert estimate == 520


@pytest.mark.unit
class TestTBoxContextFormatting:
    async def test_format_for_prompt_includes_classes(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        context = TBoxContext(
            classes=[
                {"uri": "http://example.org/ontology#Order", "label": "Order", "parent": None},
            ],
            properties=[
                {
                    "uri": "http://example.org/ontology#hasAmount",
                    "label": "hasAmount",
                    "domain": "http://example.org/ontology#Order",
                    "range": "xsd:decimal",
                },
            ],
            metrics=[
                MetricContext(
                    name="revenue",
                    description="Total revenue",
                    formula="SUM(amount)",
                    dimensions=["region"],
                ),
            ],
        )

        text = builder.format_for_prompt(context, "demo")

        assert "namespace: demo" in text
        assert "Order" in text
        assert "hasAmount" in text
        assert "revenue" in text
        assert "SUM(amount)" in text
        assert "region" in text
        assert "PREFIX ind: <http://example.org/ontology#>" in text

    async def test_format_for_prompt_prefix_from_slash_uri(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        context = TBoxContext(
            classes=[
                {"uri": "http://example.org/ontology/Order", "label": "Order", "parent": None},
            ],
            properties=[],
        )
        text = builder.format_for_prompt(context, "demo")
        assert "PREFIX ind: <http://example.org/ontology/>" in text

    async def test_format_for_prompt_empty_context(self):
        client = _make_graph_client([])
        builder = TBoxContextBuilder(client, graph_uri_template="https://test.local/{namespace}")

        context = TBoxContext(classes=[], properties=[], metrics=[])
        text = builder.format_for_prompt(context, "test")

        assert "namespace: test" in text
