# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Graph_Traversal_Tool and the edge-typed Query_Template (task 7).

Covers ``coa_serve.tier3.agentic.tools.graph_tool`` and the prepared
openCypher templates in ``coa_serve.tier3.agentic.templates``:

- the three traversal modes — ``keyword``, ``uris``, ``edge_typed`` — each return
  a :class:`ToolResult` (Req 2.3, 2.4);
- the ``edge_typed`` mode runs the :data:`EDGE_TYPED_TRAVERSAL` openCypher template
  against the toolkit ``GraphStore`` and returns the connected nodes + their
  supporting statement text (Req 5.5);
- entity ids and edge types are passed as openCypher **query parameters**, never
  interpolated into the query text (no injection surface);
- the tenant-scoped ``__Entity__`` label is built into the query text;
- the entity count (1-100) and edge-type count (1-50) bounds are enforced before
  any query is issued (Req 5.5);
- requested edge types are normalized for the ontology→property-graph match, and a
  no-match yields an empty (non-degraded) result (Req 3.9);
- the Tool NEVER raises — backend errors and timeouts degrade to a
  :class:`ToolResult` (Req 9.1-9.2);
- module load of ``graph_tool`` does NOT import ``graphrag_toolkit`` (Req 12.3).

The toolkit ``GraphStore`` is replaced with a synchronous fake whose
``execute_query_with_retry`` records the (cypher, params) it was called with and
returns canned rows — so the tests exercise the tool's real query-building and
result-parsing paths while needing no Neptune and no ``graphrag_toolkit`` access.
The tenant label formatter is patched to a deterministic string so the tests need
not import the toolkit ``TenantId``.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from coa_serve.tier3.agentic.templates import normalize_edge_type
from coa_serve.tier3.agentic.tools import (
    GraphTraversalTool,
    ToolRegistry,
    build_graph_tool,
)
from coa_serve.tier3.agentic.tools.graph_tool import normalize_graphrag_search_text

GRAPH_STORE_URI = "neptune-db://fake-host:8182"


class _FakeStore:
    """Synchronous stand-in for the toolkit GraphStore.

    Records every ``execute_query_with_retry(cypher, params)`` call and returns
    canned rows, or raises ``error`` to exercise the never-raises contract.
    """

    def __init__(self, rows=None, error=None):
        self.rows = rows if rows is not None else []
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    def execute_query_with_retry(self, cypher, params, **_kw):
        self.calls.append((cypher, params))
        if self.error is not None:
            raise self.error
        return self.rows


def _tool(rows=None, error=None):
    """A GraphTraversalTool wired to a `_FakeStore` (no real toolkit/Neptune)."""
    tool = GraphTraversalTool(GRAPH_STORE_URI)
    store = _FakeStore(rows, error)
    tool._store = store  # bypass lazy GraphStoreFactory build
    return tool, store


@pytest.fixture(autouse=True)
def _fake_labels(monkeypatch):
    """Patch tenant-label formatting so tests need not import the toolkit TenantId.

    Mirrors the real shape (tenant-suffixed, backtick-wrapped) deterministically:
    ``__Entity__`` for namespace ``retail`` → `` `__Entity__retail__` ``.
    """
    monkeypatch.setattr(
        GraphTraversalTool,
        "_format_label",
        staticmethod(lambda namespace, label: f"`{label}{namespace}__`"),
    )


# ── Interface & registration ───────────────────────────────────────


@pytest.mark.unit
class TestToolInterface:
    def test_name_and_schemas(self):
        tool, _ = _tool()
        assert tool.name == "graph_traversal"
        assert tool.description.strip()
        assert isinstance(tool.input_schema, dict) and "mode" in tool.input_schema
        assert isinstance(tool.output_schema, dict)

    def test_is_registrable(self):
        registry = ToolRegistry()
        tool, _ = _tool()
        registry.register(tool)
        assert registry.names() == ["graph_traversal"]

    def test_build_graph_tool_registers_single_tool(self):
        registry = ToolRegistry()
        name = build_graph_tool(registry, GRAPH_STORE_URI)
        assert name == "graph_traversal"
        assert registry.names() == ["graph_traversal"]
        assert isinstance(registry.get("graph_traversal"), GraphTraversalTool)


# ── keyword mode ───────────────────────────────────────────────────


@pytest.mark.unit
class TestKeywordMode:
    async def test_returns_entities_and_passes_terms_param(self):
        rows = [{"uri": "e:claim", "label": "Claim", "class": "Concept"}]
        tool, store = _tool(rows)
        out = await tool.invoke(namespace="insurance", mode="keyword", query="What are the claims?")

        assert len(store.calls) == 1
        cypher, params = store.calls[0]
        # The tenant-scoped entity label is in the query text...
        assert "`__Entity__insurance__`" in cypher
        # ...but the search terms are passed as a parameter, not interpolated.
        assert "claims" not in cypher
        assert "claims" in params["terms"]
        assert params["containers"] == []
        assert params["probes"] == params["terms"]
        assert len(out.entities) == 1
        assert out.entities[0].label == "Claim"
        assert out.items and out.items[0]["type"] == "entity"
        assert "Claim" in out.produced_directions

    async def test_unicode_candidates_match_the_persisted_search_string_contract(self):
        tool, store = _tool([])
        await tool.invoke(
            namespace="global",
            mode="keyword",
            query="İstanbul कर्मचारी เครื่องปรับอากาศ",
        )

        cypher, params = store.calls[0]
        assert params["terms"] == ["i̇stanbul", "कर मच र", "เคร องปร บอากาศ"]
        # ``İstanbul`` is Latin script, so it bounds a region rather than appearing
        # inside one, and each non-Latin word is its own reverse haystack.
        assert params["containers"] == [
            normalize_graphrag_search_text("कर्मचारी"),
            normalize_graphrag_search_text("เครื่องปรับอากาศ"),
        ]
        assert params["probes"] == params["terms"]
        assert all(probe not in cypher for probe in params["probes"])
        assert "$containers" in cypher
        assert "probe CONTAINS e.search_str" in cypher
        assert "size(replace(e.search_str, ' ', '')) >= 2" in cypher

    @pytest.mark.parametrize(
        "value",
        [
            "เครื่องปรับอากาศ",
            "कर्मचारी",
            "cafe\u0301",
            "İstanbul",
            "order-id",
            "order_id",
        ],
    )
    def test_local_search_string_normalizer_matches_locked_toolkit(self, value):
        # Test-only import: production graph_tool must retain its lazy toolkit
        # boundary and therefore carries a tiny compatible implementation.
        from coa_serve.tier3.agentic.tools.graph_tool import _GRAPHRAG_SEARCH_STRING_PATTERN
        from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import (
            SEARCH_STRING_PATTERN,
            search_string_from,
        )

        assert normalize_graphrag_search_text(value) == search_string_from(value)
        # Pin the contract itself, not only these samples.
        assert _GRAPHRAG_SEARCH_STRING_PATTERN.pattern == SEARCH_STRING_PATTERN.pattern

    async def test_no_space_thai_container_can_contain_the_normalized_stored_label(self):
        tool, store = _tool([])
        await tool.invoke(
            namespace="global",
            mode="keyword",
            query="เครื่องปรับอากาศรุ่นอะไร",
        )

        _, params = store.calls[0]
        assert normalize_graphrag_search_text("เครื่องปรับอากาศ") in params["containers"][0]

    async def test_a_long_no_space_thai_query_still_produces_a_container_probe(self):
        tool, store = _tool([])
        thai = "ขอทราบรายละเอียดของเครื่องปรับอากาศทั้งหมดในอาคารนี้รวมถึงผู้ผลิตรุ่นและวันที่ติดตั้ง"
        await tool.invoke(namespace="global", mode="keyword", query=thai)

        _, params = store.calls[0]
        assert params["terms"] == []
        assert params["containers"] == [normalize_graphrag_search_text(thai)]
        assert params["probes"] == params["containers"]

    async def test_korean_particle_suffixed_query_gets_a_container_probe(self):
        tool, store = _tool([])
        korean = "이 시설에 설치된 엘리베이터의 제조사와 모델명을 알려줘"
        await tool.invoke(namespace="global", mode="keyword", query=korean)

        _, params = store.calls[0]
        assert "제조사와" in params["terms"]
        assert "제조사와" in params["containers"]

    async def test_a_latin_query_ships_no_reverse_arm_at_all(self):
        # The reverse arm is a separate template so ``size()`` — the only use of it
        # here — never reaches Neptune for a query that cannot use the arm.
        tool, store = _tool([])
        await tool.invoke(namespace="global", mode="keyword", query="show me the elevator model")

        cypher, params = store.calls[0]
        assert params["containers"] == []
        assert params["probes"] == params["terms"]
        assert "size(" not in cypher
        assert "$containers" not in cypher

    async def test_a_latin_query_gets_no_container_probe(self):
        tool, store = _tool([])
        await tool.invoke(namespace="global", mode="keyword", query="show me the elevator model")

        _, params = store.calls[0]
        assert params["containers"] == []
        assert params["probes"] == params["terms"]

    async def test_japanese_budget_keeps_late_concepts_in_terms_and_containers(self):
        tool, store = _tool([])
        await tool.invoke(
            namespace="global",
            mode="keyword",
            query="この施設に設置されているエレベーターのメーカーと型番を教えて",
        )

        _, params = store.calls[0]
        assert params["terms"] == ["施設", "設置", "エレベーター", "メーカー", "型番"]
        assert params["containers"][0].endswith("型番を教えて")

    async def test_keyword_is_default_mode(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="ns", query="show the policies")
        assert len(store.calls) == 1
        assert out.entities == ()

    async def test_no_usable_terms_degrades_without_query(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="ns", mode="keyword", query="a an of")
        assert store.calls == []
        assert "no usable terms" in out.detail


# ── uris mode ──────────────────────────────────────────────────────


@pytest.mark.unit
class TestUrisMode:
    async def test_expands_from_entity_ids(self):
        rows = [
            {
                "source": "e:claim",
                "edge": "covers",
                "target": "e:policy",
                "targetLabel": "Policy",
                "targetClass": "Concept",
            }
        ]
        tool, store = _tool(rows)
        out = await tool.invoke(namespace="insurance", mode="uris", uris=["e:claim"])

        assert len(store.calls) == 1
        cypher, params = store.calls[0]
        assert "id(s) IN $entity_ids" in cypher
        assert params["entity_ids"] == ["e:claim"]
        assert len(out.entities) == 1
        assert out.entities[0].uri == "e:policy"
        assert out.entities[0].relationships[0]["source_uri"] == "e:claim"

    async def test_no_seed_ids_degrades_without_query(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="insurance", mode="uris", uris=[])
        assert out.entities == ()
        assert store.calls == []


# ── edge_typed mode: the prepared Query_Template (Req 5.5) ──────────


@pytest.mark.unit
class TestEdgeTypedMode:
    def _competes_rows(self):
        return [
            {
                "source": "e:amazon",
                "edge": "COMPETES_WITH",
                "target": "e:walmart",
                "targetLabel": "Walmart",
                "targetClass": "Company",
                "targetTexts": ["Walmart is a multinational retail corporation."],
            },
            {
                "source": "e:amazon",
                "edge": "COMPETES_WITH",
                "target": "e:target",
                "targetLabel": "Target",
                "targetClass": "Company",
                "targetTexts": ["Target Corporation is a retail chain.", "Target sells groceries."],
            },
        ]

    async def test_query_uses_params_not_interpolation(self):
        tool, store = _tool(self._competes_rows())
        await tool.invoke(
            namespace="retail",
            mode="edge_typed",
            entity_ids=["e:amazon"],
            edge_types=["COMPETES_WITH"],
        )
        assert len(store.calls) == 1
        cypher, params = store.calls[0]
        # Tenant-scoped labels are formatted into the query text.
        assert "`__Entity__retail__`" in cypher
        assert "`__Statement__retail__`" in cypher
        # The single relationship type and id-based match are present.
        assert "`__RELATION__`" in cypher
        assert "id(s) IN $entity_ids" in cypher
        # Entity ids and edge types are PARAMETERS, never interpolated.
        assert "e:amazon" not in cypher
        assert "COMPETES_WITH" not in cypher
        assert params["entity_ids"] == ["e:amazon"]
        assert params["edge_types_norm"] == ["competes_with"]

    async def test_returns_connected_nodes_and_text(self):
        tool, _ = _tool(self._competes_rows())
        out = await tool.invoke(
            namespace="retail",
            mode="edge_typed",
            entity_ids=["e:amazon"],
            edge_types=["COMPETES_WITH"],
        )
        # Connected nodes returned as entities.
        assert {e.uri for e in out.entities} == {"e:walmart", "e:target"}
        # Connected node TEXT returned as chunks (Req 5.5).
        texts = {c.text for c in out.chunks}
        assert "Walmart is a multinational retail corporation." in texts
        assert "Target Corporation is a retail chain." in texts
        assert "Target sells groceries." in texts
        # Each connected entity carries the inbound source/edge relationship.
        walmart = next(e for e in out.entities if e.uri == "e:walmart")
        assert walmart.relationships[0]["predicate"] == "COMPETES_WITH"
        assert walmart.relationships[0]["source_uri"] == "e:amazon"
        # items mirror chunks + entities; branch seeds surfaced.
        assert len(out.items) == len(out.chunks) + len(out.entities)
        assert "Walmart" in out.produced_directions

    async def test_multiple_edge_types_normalized_in_params(self):
        tool, store = _tool([])
        await tool.invoke(
            namespace="retail",
            mode="edge_typed",
            entity_ids=["e:amazon", "e:ebay"],
            edge_types=["COMPETES WITH", "has-financials"],
        )
        _, params = store.calls[0]
        assert params["entity_ids"] == ["e:amazon", "e:ebay"]
        # spaces and hyphens normalized to underscores, lowercased, deduped+sorted.
        assert params["edge_types_norm"] == ["competes_with", "has_financials"]
        assert normalize_edge_type("COMPETES WITH") == "competes_with"

    async def test_no_match_returns_empty_non_degraded(self):
        # Edge type valid + in bounds, but the graph has no matching relation.
        tool, _ = _tool([])
        out = await tool.invoke(
            namespace="retail",
            mode="edge_typed",
            entity_ids=["e:amazon"],
            edge_types=["NONEXISTENT_REL"],
        )
        assert out.entities == ()
        assert out.chunks == ()
        # Empty, but NOT a degradation — no error/timeout wording.
        assert "error" not in out.detail and "timeout" not in out.detail

    async def test_target_without_text_still_returns_entity(self):
        rows = [
            {
                "source": "e:amazon",
                "edge": "COMPETES_WITH",
                "target": "e:walmart",
                "targetLabel": "Walmart",
                "targetClass": "Company",
                "targetTexts": [],
            }
        ]
        tool, _ = _tool(rows)
        out = await tool.invoke(
            namespace="retail",
            mode="edge_typed",
            entity_ids=["e:amazon"],
            edge_types=["COMPETES_WITH"],
        )
        assert len(out.entities) == 1
        assert out.chunks == ()


# ── edge_typed: bounds enforcement (Req 5.5) ───────────────────────


@pytest.mark.unit
class TestEdgeTypedBounds:
    async def test_zero_entities_rejected_no_query(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=[], edge_types=["x"])
        assert store.calls == []
        assert "out of" in out.detail and "entity" in out.detail
        assert out.entities == () and out.chunks == ()

    async def test_too_many_entities_rejected_no_query(self):
        tool, store = _tool([])
        entity_ids = [f"e:{i}" for i in range(101)]  # 101 > 100
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=entity_ids, edge_types=["x"])
        assert store.calls == []
        assert "out of" in out.detail

    async def test_exactly_100_entities_allowed(self):
        tool, store = _tool([])
        entity_ids = [f"e:{i}" for i in range(100)]  # 100 == MAX
        await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=entity_ids, edge_types=["x"])
        assert len(store.calls) == 1

    async def test_zero_edge_types_rejected_no_query(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=["e:1"], edge_types=[])
        assert store.calls == []
        assert "out of" in out.detail and "edge-type" in out.detail

    async def test_too_many_edge_types_rejected_no_query(self):
        tool, store = _tool([])
        edge_types = [f"rel{i}" for i in range(51)]  # 51 > 50
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=["e:1"], edge_types=edge_types)
        assert store.calls == []
        assert "out of" in out.detail

    async def test_exactly_50_edge_types_allowed(self):
        tool, store = _tool([])
        edge_types = [f"rel{i}" for i in range(50)]  # 50 == MAX
        await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=["e:1"], edge_types=edge_types)
        assert len(store.calls) == 1


# ── edge_types: predicate discovery (idea 5 soft-prior) ────────────


@pytest.mark.unit
class TestEdgeTypesMode:
    """The 'edge_types' mode discovers real predicates present on given entities so
    the planner can propose graph relationships the pruned ontology omits."""

    async def test_returns_present_predicates_as_edge_type_items(self):
        tool, store = _tool(
            rows=[
                {"edge": "HAS SEGMENT", "freq": 12},
                {"edge": "GENERATES REVENUE IN", "freq": 7},
            ]
        )
        result = await tool.invoke(namespace="retail", mode="edge_types", entity_ids=["e1"])

        edges = [(i["edge"], i["freq"]) for i in result.items if i.get("type") == "edge_type"]
        assert edges == [("HAS SEGMENT", 12), ("GENERATES REVENUE IN", 7)]
        assert "HAS SEGMENT" in result.detail
        # entity ids passed as a query PARAMETER, not interpolated.
        cypher, params = store.calls[0]
        assert params == {"entity_ids": ["e1"]}
        assert "$entity_ids" in cypher

    async def test_no_entity_ids_rejected_without_query(self):
        tool, store = _tool(rows=[])
        result = await tool.invoke(namespace="retail", mode="edge_types", entity_ids=[])
        assert "out of bounds" in result.detail
        assert store.calls == []  # bounds checked before any query

    async def test_empty_result_is_non_degraded(self):
        tool, _store = _tool(rows=[])
        result = await tool.invoke(namespace="retail", mode="edge_types", entity_ids=["e1"])
        assert result.items == ()
        assert "0 present predicates" in result.detail


# ── never raises (Req 9.1-9.2) ─────────────────────────────────────


@pytest.mark.unit
class TestNeverRaises:
    async def test_backend_error_degrades(self):
        tool, _ = _tool(error=RuntimeError("neptune down"))
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=["e:1"], edge_types=["x"])
        assert "error" in out.detail
        assert out.entities == ()

    async def test_timeout_degrades(self):
        tool, _ = _tool(error=TimeoutError())
        out = await tool.invoke(namespace="ns", mode="edge_typed", entity_ids=["e:1"], edge_types=["x"])
        assert "timeout" in out.detail

    async def test_unknown_mode_degrades(self):
        tool, store = _tool([])
        out = await tool.invoke(namespace="ns", mode="nonsense")
        assert "unknown mode" in out.detail
        assert store.calls == []


# ── Lazy-import invariant (Req 12.3) ───────────────────────────────


@pytest.mark.unit
class TestLazyImportInvariant:
    def test_importing_graph_tool_does_not_import_graphrag(self):
        """``import ...tools.graph_tool`` must NOT import ``graphrag_toolkit``.

        Runs in a fresh subprocess (current ``sys.path`` propagated) so a
        ``graphrag_toolkit`` already imported by a sibling test cannot mask a
        regression (mirrors the task-1/task-6 invariant tests)."""
        code = (
            "import sys; "
            "import coa_serve.tier3.agentic.tools.graph_tool; "
            "leaked = sorted(m for m in sys.modules if m == 'graphrag_toolkit' "
            "or m.startswith('graphrag_toolkit.')); "
            "assert not leaked, leaked; "
            "print('OK')"
        )
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
        assert result.returncode == 0, (
            f"importing graph_tool leaked graphrag_toolkit or failed.\nstdout={result.stdout}\nstderr={result.stderr}"
        )
        assert "OK" in result.stdout
