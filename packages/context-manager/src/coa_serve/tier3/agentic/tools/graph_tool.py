# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Graph_Traversal_Tool: entity/edge traversal over the Knowledge_Graph (Req 2.3).

:class:`GraphTraversalTool` traverses the graphrag-toolkit **property graph** (the
graph kg-build populates in Neptune Database) using **openCypher**, executed
through the toolkit ``GraphStore``. It is NOT SPARQL and does NOT use the serve
layer's ``GraphTraverser``/``NeptuneGraphClient`` (those target the RDF ontology
graph — a different dataset and query language; see the design's "Two graphs").

Three traversal modes are exposed through the uniform :class:`Tool` contract:

- ``keyword``    — find entities whose ``search_str`` contains terms extracted
  from a natural-language sub-question.
- ``uris``       — expand one hop over any ``__RELATION__`` edge from known entity
  ids.
- ``edge_typed`` — the prepared :data:`EDGE_TYPED_TRAVERSAL` Query_Template
  (Req 5.5): given 1-100 entity ids and 1-50 edge types, traverse exactly those
  edge types from those entities and return the connected nodes and their
  supporting statement text. This is the edge-type constraint the toolkit's own
  retrievers do not impose.

Ontology→property-graph edge-type mapping (Req 3.9): the requested edge types
(chosen by the planner from the ontology's edge-type set) are normalized with
:func:`normalize_edge_type` and matched against the same normalization of the
property graph's ``__RELATION__.value`` predicate strings, so the two independent
vocabularies reconcile despite casing/separator differences. A requested edge type
with no normalized match simply yields zero rows (an empty, non-degraded result).

Import discipline (Requirement 12.3): module load imports only the dependency-free
``.base`` contract, the template strings, and the sibling ``tier3`` result
dataclasses (``ChunkResult`` / ``GraphEntity``), none of which import
``graphrag_toolkit``. The toolkit ``GraphStoreFactory`` and ``TenantId`` are
imported **lazily** inside the first invocation, so importing this module never
pulls the toolkit into ``sys.modules``.

Per-Tool failure tolerance (Req 9.1-9.2): :meth:`GraphTraversalTool.invoke` never
raises for an expected failure. A bad/empty parameter set, an out-of-bounds
entity/edge count, an unconfigured graph store, a backend error, or a query
timeout all yield a (possibly empty) :class:`ToolResult` whose ``detail`` records
the degradation so the controller can record a degraded source and continue.
"""

from __future__ import annotations

import asyncio
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import structlog

from ...graph_traverser import GraphEntity
from ...vector_retriever import ChunkResult
from ..templates import (
    DEFAULT_EDGE_TYPED_LIMIT,
    EDGE_TYPED_TRAVERSAL,
    KEYWORD_ENTITY_SEARCH,
    KEYWORD_ENTITY_SEARCH_WITH_CONTAINERS,
    MAX_EDGE_TYPES,
    MAX_ENTITIES,
    MIN_EDGE_TYPES,
    MIN_ENTITIES,
    URI_EXPANSION,
    normalize_edge_type,
)
from .base import ToolResult

logger = structlog.get_logger(__name__)

TOOL_NAME = "graph_traversal"

# Compatibility contract with graphrag-lexical-graph 3.18.7's persisted
# ``Entity.search_str``. Keep this tiny local copy instead of importing the
# toolkit's private graph_utils module at import time: that module eagerly pulls
# the toolkit and LlamaIndex graph stack, violating this tool's lazy-import
# boundary. A unit test compares this function with the installed helper.
_GRAPHRAG_SEARCH_STRING_PATTERN = re.compile(r"([^\s\w]|_)+")


def normalize_graphrag_search_text(value: str) -> str:
    """Normalize a query candidate exactly like toolkit 3.18.7 ``search_str``."""
    normalized = _GRAPHRAG_SEARCH_STRING_PATTERN.sub(" ", value)
    while "  " in normalized:
        normalized = normalized.replace("  ", " ")
    return normalized.lower().strip()


# Default wall-clock cap for a single graph query. The toolkit call is synchronous
# and cannot be cancelled once running, so it executes on a bounded worker pool and
# is awaited under this timeout; a timed-out call is abandoned (its worker drains)
# rather than cancelled, bounding thread usage. The controller also applies the
# configured per-tool timeout around invoke().
_GRAPH_QUERY_TIMEOUT_S: float = float(os.environ.get("AGENTIC_GRAPH_QUERY_TIMEOUT_S", "30"))

# Bounded executor for the synchronous toolkit GraphStore calls (mirrors the
# discipline in BaselineLexicalRetriever): excess concurrent queries queue rather
# than spawning unbounded threads, and a timed-out uncancellable call cannot starve
# unrelated to_thread work.
_GRAPH_MAX_WORKERS: int = int(os.environ.get("AGENTIC_GRAPH_MAX_WORKERS", "4"))
_EXECUTOR = ThreadPoolExecutor(max_workers=_GRAPH_MAX_WORKERS, thread_name_prefix="agentic-graph")

_DESCRIPTION = (
    "Graph traversal over the knowledge graph (property graph). Four modes: "
    "'keyword' finds entities whose labels match terms in the sub-question; 'uris' "
    "expands one hop outward from known entity ids; 'edge_types' discovers which "
    "edge-type predicates actually exist on given entity ids (use this to learn the "
    "real relationships available before an edge_typed traversal); 'edge_typed' "
    "traverses a specific set of edge types (1-50) from a specific set of entity ids "
    "(1-100) and returns the connected nodes and their text. Prefer 'edge_typed' "
    "when you know which edge type connects the concepts you need (e.g. follow "
    "HAS_SEGMENT from a company entity). Edge types SHOULD be drawn from the "
    "ontology when it lists a fitting one, but you MAY use a predicate discovered via "
    "'edge_types' that the ontology omits — the ontology is a pruned subset of the "
    "graph's real relationships. Entity ids come from a prior entity-anchoring retrieval."
)

_INPUT_SCHEMA: dict = {
    "mode": "str (one of: keyword, uris, edge_types, edge_typed)",
    "query": "str (keyword mode: the sub-question text)",
    "uris": "list[str] (uris mode: seed entity ids)",
    "entity_ids": "list[str] (edge_types/edge_typed mode: 1-100 source entity ids)",
    "edge_types": "list[str] (edge_typed mode: 1-50 edge-type labels)",
    "limit": "int (max rows, default 100)",
}
_OUTPUT_SCHEMA: dict = {"entities": "list", "chunks": "list", "items": "list"}


def _entity_item(entity: GraphEntity) -> dict:
    """Normalize a ``GraphEntity`` to a plain dict for ``ToolResult.items``."""
    return {
        "type": "entity",
        "uri": entity.uri,
        "label": entity.label,
        "entity_type": entity.type,
        "relationships": list(entity.relationships),
    }


def _chunk_item(chunk: ChunkResult) -> dict:
    """Normalize a connected-node ``ChunkResult`` to a dict for ``ToolResult.items``."""
    return {
        "type": "chunk",
        "chunk_id": chunk.chunk_id,
        "text": chunk.text,
        "source_doc": chunk.source_doc,
        "uri": chunk.uri,
        "label": chunk.label,
    }


class GraphTraversalTool:
    """openCypher traversal of the property graph via the toolkit ``GraphStore``.

    The four uniform interface fields are static, graphrag-free data set at
    construction, so the Tool is registrable and describable to the planner
    without any toolkit access. The toolkit ``GraphStore`` is built lazily on first
    use and cached for the tool's lifetime.
    """

    def __init__(self, graph_store_uri: str, *, query_timeout_s: float = _GRAPH_QUERY_TIMEOUT_S) -> None:
        """Build the Tool over a graphrag-toolkit graph-store URI.

        Args:
            graph_store_uri: The same ``neptune-db://<host>:8182`` (or
                ``neptune-graph://g-…``) URI the strategy tools/kg-build use. The
                toolkit ``GraphStore`` is constructed from it lazily inside the
                first invocation (Req 12.3).
            query_timeout_s: Wall-clock cap applied around each graph query.
        """
        self.name = TOOL_NAME
        self.description = _DESCRIPTION
        self.input_schema = dict(_INPUT_SCHEMA)
        self.output_schema = dict(_OUTPUT_SCHEMA)
        self._graph_store_uri = graph_store_uri
        self._query_timeout_s = query_timeout_s
        self._store: Any = None

    # ── lazy toolkit wiring (Req 12.3) ─────────────────────────────────

    def _get_store(self) -> Any:
        """Build (once) and return the toolkit ``GraphStore``; lazy toolkit import."""
        if self._store is None:
            from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory  # lazy

            self._store = GraphStoreFactory.for_graph_store(self._graph_store_uri)
            logger.info("agentic_graph_store_built", uri=self._graph_store_uri)
        return self._store

    @staticmethod
    def _entity_label(namespace: str) -> str:
        """Tenant-scoped, backtick-wrapped ``__Entity__`` label for ``namespace``."""
        return GraphTraversalTool._format_label(namespace, "__Entity__")

    @staticmethod
    def _statement_label(namespace: str) -> str:
        """Tenant-scoped, backtick-wrapped ``__Statement__`` label for ``namespace``."""
        return GraphTraversalTool._format_label(namespace, "__Statement__")

    @staticmethod
    def _format_label(namespace: str, label: str) -> str:
        """Format ``label`` for the namespace's graphrag tenant (authoritative).

        Derives the tenant exactly as kg-build/`BaselineLexicalRetriever` do
        (``to_graphrag_tenant_id``) and uses the toolkit ``TenantId.format_label``
        so the label string matches what is stored in the graph. Both imports are
        lazy (the toolkit one for Req 12.3; ``to_graphrag_tenant_id`` is toolkit-free
        but kept local for symmetry).
        """
        from coa_common.constants import to_graphrag_tenant_id
        from graphrag_toolkit.lexical_graph import TenantId  # lazy

        return TenantId(to_graphrag_tenant_id(namespace)).format_label(label)

    async def _run_cypher(self, cypher: str, params: dict) -> list[dict]:
        """Execute openCypher on the toolkit store under the query timeout.

        The toolkit call is synchronous; it runs on a bounded worker pool and is
        awaited under :attr:`_query_timeout_s`. ``asyncio.shield`` ensures a
        timeout abandons (rather than cancels) the in-flight worker, so the pool
        is never left with a dangling cancellation.
        """
        store = self._get_store()
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(_EXECUTOR, lambda: store.execute_query_with_retry(cypher, params))
        rows = await asyncio.wait_for(asyncio.shield(fut), timeout=self._query_timeout_s)
        return list(rows or [])

    # ── dispatch ───────────────────────────────────────────────────────

    async def invoke(self, *, namespace: str, mode: str = "keyword", **kwargs: Any) -> ToolResult:
        """Run a traversal scoped to ``namespace`` and return a :class:`ToolResult`.

        Dispatches on ``mode`` (``keyword`` / ``uris`` / ``edge_typed``). Never
        raises for an expected failure: an unknown mode, invalid parameters, an
        out-of-bounds entity/edge count, an unconfigured store, a backend error,
        or a timeout all return a degraded :class:`ToolResult` whose ``detail``
        records the reason (Req 9.1-9.2).
        """
        try:
            if mode == "keyword":
                return await self._invoke_keyword(namespace, **kwargs)
            if mode == "uris":
                return await self._invoke_uris(namespace, **kwargs)
            if mode == "edge_typed":
                return await self._invoke_edge_typed(namespace, **kwargs)
            if mode == "edge_types":
                return await self._invoke_edge_types(namespace, **kwargs)
            return ToolResult(detail=f"{self.name}: unknown mode {mode!r}")
        except TimeoutError:
            logger.warning("graph_tool_timeout", tool=self.name, mode=mode)
            return ToolResult(detail=f"{self.name} timeout in mode {mode!r}")
        except Exception as exc:  # noqa: BLE001 - a Tool must never raise (Req 9.1)
            logger.warning(
                "graph_tool_invoke_error",
                tool=self.name,
                mode=mode,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return ToolResult(detail=f"{self.name} error: {type(exc).__name__}")

    # ── keyword mode ───────────────────────────────────────────────────

    async def _invoke_keyword(
        self, namespace: str, *, query: str = "", limit: int = DEFAULT_EDGE_TYPED_LIMIT, **_: Any
    ) -> ToolResult:
        # Build one bounded raw plan, then apply the property graph's persisted
        # ``search_str`` contract at this sink only. RDF consumers intentionally
        # keep their LCASE-compatible spelling instead.
        from ....query_utils import build_query_search_plan

        search_plan = build_query_search_plan(query or "", max_count=5)
        terms = list(
            dict.fromkeys(
                normalized
                for candidate in search_plan.terms
                if (normalized := normalize_graphrag_search_text(candidate))
            )
        )
        containers = list(
            dict.fromkeys(
                normalized
                for candidate in search_plan.containers
                if (normalized := normalize_graphrag_search_text(candidate))
            )
        )
        probes = list(dict.fromkeys(terms + containers))
        if not probes:
            return ToolResult(detail=f"{self.name}[keyword]: no usable terms in query")
        template = KEYWORD_ENTITY_SEARCH_WITH_CONTAINERS if containers else KEYWORD_ENTITY_SEARCH
        cypher = template.format(entity_label=self._entity_label(namespace), limit=int(limit))
        rows = await self._run_cypher(cypher, {"probes": probes, "terms": terms, "containers": containers})
        entities = [
            GraphEntity(uri=row.get("uri", ""), label=row.get("label", ""), type=row.get("class", ""))
            for row in rows
            if row.get("uri")
        ]
        return self._entities_result(entities, f"{self.name}[keyword]: {len(entities)} entities")

    # ── uris mode ──────────────────────────────────────────────────────

    async def _invoke_uris(
        self, namespace: str, *, uris: list[str] | None = None, limit: int = DEFAULT_EDGE_TYPED_LIMIT, **_: Any
    ) -> ToolResult:
        entity_ids = [u for u in (uris or []) if u]
        if not entity_ids:
            return ToolResult(detail=f"{self.name}[uris]: no seed entity ids supplied")
        cypher = URI_EXPANSION.format(entity_label=self._entity_label(namespace), limit=int(limit))
        rows = await self._run_cypher(cypher, {"entity_ids": entity_ids})
        entities = self._connected_entities(rows)
        return self._entities_result(entities, f"{self.name}[uris]: {len(entities)} connected entities")

    # ── edge_types mode (predicate discovery for the soft ontology prior) ──

    async def _invoke_edge_types(
        self, namespace: str, *, entity_ids: list[str] | None = None, limit: int = 50, **_: Any
    ) -> ToolResult:
        """Discover the edge-type predicates actually present on the given entities.

        Runs :data:`ENTITY_EDGE_TYPES` and returns the raw ``r.value`` predicates
        (with occurrence counts) as ``edge_type`` items so the planner can propose
        real graph predicates that the pruned ontology omits (the soft-prior half of
        idea 5). No chunks/entities are gathered — this is a metadata probe — so it
        does not itself count as retrieval progress; the discovered predicates are
        surfaced to the planner via the result ``detail`` and cached on the context.
        """
        from ..templates import ENTITY_EDGE_TYPES

        ids = [e for e in (entity_ids or []) if e]
        if not (MIN_ENTITIES <= len(ids) <= MAX_ENTITIES):
            return ToolResult(
                detail=(
                    f"{self.name}[edge_types]: entity count {len(ids)} out of bounds [{MIN_ENTITIES}, {MAX_ENTITIES}]"
                )
            )
        cypher = ENTITY_EDGE_TYPES.format(entity_label=self._entity_label(namespace), limit=int(limit))
        rows = await self._run_cypher(cypher, {"entity_ids": ids})
        items = tuple(
            {"type": "edge_type", "edge": row.get("edge", ""), "freq": row.get("freq", 0)}
            for row in rows
            if row.get("edge")
        )
        labels = ", ".join(f"{i['edge']}({i['freq']})" for i in items[:20]) or "(none)"
        return ToolResult(items=items, detail=f"{self.name}[edge_types]: {len(items)} present predicates: {labels}")

    # ── edge_typed mode (prepared Query_Template, Req 5.5) ─────────────

    async def _invoke_edge_typed(
        self,
        namespace: str,
        *,
        entity_ids: list[str] | None = None,
        edge_types: list[str] | None = None,
        limit: int = DEFAULT_EDGE_TYPED_LIMIT,
        **_: Any,
    ) -> ToolResult:
        """Fill and execute the edge-typed traversal template (Req 5.5).

        Validates the entity count (1-100) and edge-type count (1-50) before
        building the query, then runs the openCypher template with entity ids and
        normalized edge types passed as query parameters. Returns the connected
        entities plus, where present, their supporting statement text as chunks.
        """
        entity_ids = [e for e in (entity_ids or []) if e]
        edge_types = [e for e in (edge_types or []) if e]

        if not (MIN_ENTITIES <= len(entity_ids) <= MAX_ENTITIES):
            return ToolResult(
                detail=(
                    f"{self.name}[edge_typed]: entity count {len(entity_ids)} out of "
                    f"bounds [{MIN_ENTITIES}, {MAX_ENTITIES}]"
                )
            )
        if not (MIN_EDGE_TYPES <= len(edge_types) <= MAX_EDGE_TYPES):
            return ToolResult(
                detail=(
                    f"{self.name}[edge_typed]: edge-type count {len(edge_types)} out of "
                    f"bounds [{MIN_EDGE_TYPES}, {MAX_EDGE_TYPES}]"
                )
            )

        # Normalize requested edge types to match stored r.value (Req 3.9 mapping).
        edge_types_norm = sorted({normalize_edge_type(e) for e in edge_types})

        cypher = EDGE_TYPED_TRAVERSAL.format(
            entity_label=self._entity_label(namespace),
            statement_label=self._statement_label(namespace),
            limit=int(limit),
        )
        rows = await self._run_cypher(cypher, {"entity_ids": entity_ids, "edge_types_norm": edge_types_norm})
        return self._parse_edge_typed(rows)

    # ── result assembly ────────────────────────────────────────────────

    @staticmethod
    def _connected_entities(rows: list[dict]) -> list[GraphEntity]:
        """Group ``source``/``edge``/``target`` rows into one entity per target."""
        by_target: dict[str, GraphEntity] = {}
        rels: dict[str, list[dict]] = {}
        for row in rows:
            target = row.get("target", "")
            if not target:
                continue
            if target not in by_target:
                by_target[target] = GraphEntity(
                    uri=target,
                    label=row.get("targetLabel", "") or "",
                    type=row.get("targetClass", "") or "",
                )
                rels[target] = []
            if row.get("edge") and row.get("source"):
                rels[target].append(
                    {
                        "predicate": row["edge"],
                        "source_uri": row["source"],
                        "target_uri": target,
                        # Canonical relationship key consumed by the Synthesizer
                        # (matches GraphTraverser's shape); the target IS this
                        # connected entity, so its label is the target label.
                        "target_label": row.get("targetLabel", "") or "",
                    }
                )
        return [
            GraphEntity(uri=e.uri, label=e.label, type=e.type, relationships=tuple(rels[e.uri]))
            for e in by_target.values()
        ]

    def _entities_result(self, entities: list[GraphEntity], detail: str) -> ToolResult:
        items = tuple(_entity_item(e) for e in entities)
        directions = tuple(e.label for e in entities if e.label)
        return ToolResult(entities=tuple(entities), items=items, detail=detail, produced_directions=directions)

    def _parse_edge_typed(self, rows: list[dict]) -> ToolResult:
        """Parse edge-typed rows into connected entities + their statement text.

        Each row binds ``source``, ``edge``, ``target``, ``targetLabel``,
        ``targetClass`` and a ``targetTexts`` list of supporting statement
        strings. Rows are grouped by ``target`` into one :class:`GraphEntity`
        (carrying the inbound ``source``/``edge`` relationship), and each non-empty
        statement text yields a :class:`ChunkResult` so the connected node text
        flows through to synthesis (Req 5.5).
        """
        entities = self._connected_entities(rows)

        chunks: list[ChunkResult] = []
        seen_text: set[str] = set()
        for row in rows:
            target = row.get("target", "")
            if not target:
                continue
            label = row.get("targetLabel", "") or ""
            for idx, text in enumerate(row.get("targetTexts", []) or []):
                if not text:
                    continue
                key = f"{target}#{text}"
                if key in seen_text:
                    continue
                seen_text.add(key)
                chunks.append(
                    ChunkResult(
                        chunk_id=f"{target}#stmt{idx}",
                        text=text,
                        source_doc="",
                        relevance_score=1.0,
                        label=label,
                        uri=target,
                    )
                )

        items = tuple(_chunk_item(c) for c in chunks) + tuple(_entity_item(e) for e in entities)
        directions = tuple(e.label for e in entities if e.label)
        detail = f"{self.name}[edge_typed]: {len(entities)} connected nodes, {len(chunks)} with text"
        return ToolResult(
            chunks=tuple(chunks),
            entities=tuple(entities),
            items=items,
            detail=detail,
            produced_directions=directions,
        )


def build_graph_tool(registry: Any, graph_store_uri: str) -> str:
    """Register the single :class:`GraphTraversalTool` into ``registry``.

    Mirrors :func:`build_strategy_tools`: a thin builder the factory (task 17)
    calls when the Neptune graph endpoint is configured. Returns the registered
    Tool name.

    Args:
        registry: The :class:`ToolRegistry` to register the tool into.
        graph_store_uri: The graphrag-toolkit graph-store URI the tool queries.

    Returns:
        The registered Tool name (``"graph_traversal"``).
    """
    tool = GraphTraversalTool(graph_store_uri)
    registry.register(tool)
    logger.info("graph_tool_registered", tool_name=tool.name)
    return tool.name
