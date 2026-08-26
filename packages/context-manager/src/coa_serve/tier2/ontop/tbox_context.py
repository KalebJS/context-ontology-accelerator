# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assemble query-relevant T-Box context from Neptune published graph.

The T-Box context provides the ontology schema (classes, properties, metrics)
needed by the NL-to-SPARQL translation prompt. Context is assembled from
vector search hits produced during routing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import structlog
from coa_common.constants import URN_PREFIX, VOCAB_URI

from ...clients.base import GraphClient
from ...query_utils import (
    DEFAULT_GRAPH_URI_TEMPLATE,
    build_query_search_plan,
    escape_sparql_string_literal,
    get_graph_uri_template,
    namespace_graph_prefix,
    normalize_label_match_text,
    object_properties_sparql,
    validate_namespace,
)
from .types import VectorHit

logger = structlog.get_logger(__name__)

# Full IRI for coa:distinctValues — the sampled distinct enum values a datatype
# property may carry (emitted at induction from low-cardinality categorical
# columns). Used as a full IRI (not a prefix) so the SPARQL needs no PREFIX decl.
_SCL_DISTINCT_VALUES = f"{VOCAB_URI}distinctValues"
# Max sampled values rendered per property in the prompt (keeps the context lean;
# a handful of literals is enough for the LLM to anchor WHERE-clause values).
_MAX_DISTINCT_VALUES_IN_PROMPT = 12

# Maximum URIs per SPARQL VALUES clause (Neptune query complexity limit)
_MAX_SPARQL_VALUES_URIS = 50
# Maximum results per SPARQL query
_SPARQL_RESULT_LIMIT = 2000
# Cap on FK join paths folded into the prompt. Lower than _SPARQL_RESULT_LIMIT
# because these are rendered as prose for the LLM, not walked programmatically.
_OBJECT_PROPERTY_LIMIT = 200
# Namespace class count threshold for full-context fetch (bypass vector search)
_FULL_CONTEXT_CLASS_THRESHOLD = 200

# Tier-2 answerability gate. A class is included in the structured-query T-Box
# context ONLY if it carries the ``coa:isMapped true`` marker written at ingest
# (i.e. it is the rr:class of an R2RML TriplesMap → SQL-backed → answerable by
# Ontop / NL->SQL). Unmapped classes (unstructured-induced, foundational) are
# excluded so the LLM never authors a query over a class Ontop can't resolve.
# This is a REQUIRED triple pattern (absent marker == hidden), not OPTIONAL.
# The full IRI is inlined because ``coa:`` is not a Neptune default prefix and
# the graph client injects no PREFIX prolog. The SPARQL keyword ``true`` is
# exactly the typed literal ``"true"^^xsd:boolean`` — the same term store_class
# writes — so it matches by term equality.
_IS_MAPPED_IRI = f"{VOCAB_URI}isMapped"
_IS_MAPPED_PATTERN = f"?class <{_IS_MAPPED_IRI}> true ."
# ─────────────────────────────────────────────────────────────────────────────
# LEGACY-NAMESPACE BRIDGE (temporary compatibility shim)
#
# The ``coa:isMapped`` marker is written only by the post-marker ingest path
# (NDB backend). A namespace ingested BEFORE the marker existed — or via a
# backend that never writes it — carries ZERO isMapped triples, so the required
# ``_IS_MAPPED_PATTERN`` gate would hide EVERY class and silently break Tier-2
# for that namespace (structured queries fall through to Tier-3). To avoid
# regressing existing users, ``build`` probes once per request whether the
# namespace has ANY mapped marker (``_namespace_has_mapped_markers``); when it
# has none it drops the gate entirely (``mapped=False``), restoring the
# pre-filter behavior (all classes exposed) for that namespace only. A namespace
# with even ONE mapped class keeps the strict gate. The gate is selected
# per-request via ``_class_gate(mapped)`` / ``_parent_pattern(mapped)`` so both
# behaviors coexist across namespaces.
#
# ── HOW TO REMOVE THIS BRIDGE (revert to the strict pre-bridge behavior) ──
# Added 2026-07-16; TEMPORARY. Revisit by ~2026-10 (≈3 months). If you are here
# after that and the bridge still exists, it has likely outlived its purpose:
# verify all live namespaces carry coa:isMapped markers, then remove it.
# The end state is the original strict "absent marker == hidden" behavior —
# adopt it once all live namespaces have been re-inducted (so they carry
# coa:isMapped markers) and users have been given notice. Removal is a pure
# deletion, no logic to re-derive: every bridge site below is tagged
# ``# BRIDGE:`` and carries the exact PRE-BRIDGE line to restore. To remove:
# 1. Delete ``_namespace_has_mapped_markers`` and the ``mapped = await …``
# probe call + fallback log in ``build``.
# 2. Delete ``_NO_GATE``, ``_UNGATED_PARENT_PATTERN``, ``_class_gate``,
# ``_parent_pattern``, and the ``mapped`` parameter on the four fetch
# methods.
# 3. At each ``# BRIDGE:`` site, replace the ``_class_gate(mapped)`` /
# ``_parent_pattern(mapped)`` / conditional-gate call with the PRE-BRIDGE
# line quoted in its comment (the module constants ``_IS_MAPPED_PATTERN`` /
# ``_MAPPED_PARENT_PATTERN`` used unconditionally).
# 4. Delete the bridge tests (class ``TestIsMappedLegacyBridge`` in
# test_tbox_context_strstarts.py) AND revert the two ancillary test edits:
# the ``graph_client.ask = AsyncMock(...)`` line + bridge comment added to
# TestTBoxGlossaryMappedGate (same file), and the reworded ``call_count``
# comment in test_tier2_tbox_context.py.
# 5. Restore the prose comments that were reworded to mention the bridge (this
# block, the dark-namespace canary above ``if not context.classes``, and the
# count/fetch-all/object-property explanatory comments) to their pre-bridge
# wording — these are comment-only and must be hand-reverted.
_NO_GATE = ""  # empty triple pattern → no isMapped restriction (legacy fallback)
# Parent (rdfs:subClassOf) is surfaced to the prompt as ``subClassOf: <parent>``,
# so it must be gated the same way as the class itself and as object-property
# ends: bind ?parentClass ONLY when the parent is itself a MAPPED class. Without
# the inner isMapped guard, a grounded class (``ind:Orders rdfs:subClassOf
# fibo:PurchaseOrder``) leaks the unmapped foundational parent IRI into the
# structured prompt — the exact "unmapped class name reaches the LLM" failure the
# object-property gate prevents. The parent's isMapped marker lives in the same
# named graph as the subClassOf triple (store_class writes all of a class's
# triples into one graph), so an inner ``?parentClass <isMapped> true`` matches a
# mapped sibling parent and correctly excludes any foundational/unmapped parent.
_MAPPED_PARENT_PATTERN = f"OPTIONAL {{ ?class rdfs:subClassOf ?parentClass . ?parentClass <{_IS_MAPPED_IRI}> true . }}"
# Legacy-fallback parent pattern: surface the subClassOf parent WITHOUT the
# parent-is-mapped guard. Used only when the namespace has zero isMapped markers,
# so there is no mapped/unmapped distinction to enforce — the pre-filter behavior.
_UNGATED_PARENT_PATTERN = "OPTIONAL { ?class rdfs:subClassOf ?parentClass . }"


# BRIDGE: pre-bridge, callers used the module constant ``_IS_MAPPED_PATTERN``
# directly (always gated). This helper adds the ``mapped=False`` fallback.
def _class_gate(mapped: bool) -> str:
    """The ``?class <isMapped> true`` gate, or empty for the legacy fallback.

    ``mapped=True`` (the namespace has ≥1 isMapped marker) → the required gate,
    so unmapped classes stay hidden. ``mapped=False`` (namespace predates the
    marker / backend never writes it) → empty, exposing all classes.
    """
    return _IS_MAPPED_PATTERN if mapped else _NO_GATE


# BRIDGE: pre-bridge, callers used ``_MAPPED_PARENT_PATTERN`` directly.
def _parent_pattern(mapped: bool) -> str:
    """subClassOf-parent OPTIONAL — parent-mapped-gated, or ungated for fallback."""
    return _MAPPED_PARENT_PATTERN if mapped else _UNGATED_PARENT_PATTERN


# Per-class property OPTIONAL. Restricted to owl:DatatypeProperty (columns) ON
# PURPOSE: a datatype property's rdfs:range is an xsd:* term (xsd:integer, …),
# never a class IRI, so surfacing it in the prompt's "Properties" section can
# never leak a class name. An OBJECT property's range IS a class IRI — and when
# that target class is unmapped (unstructured / foundational), an un-gated
# ``?property rdfs:range ?range`` would reintroduce an UNANSWERABLE class name
# into the structured prompt via the property's range (the object-property
# equivalent of the subClassOf-parent leak). Object-property join paths are
# surfaced separately by ``_fetch_object_properties``, mapped-gated on BOTH ends;
# excluding them here means the only object-property edges the LLM ever sees are
# mapped<->mapped. (``owl:`` / ``rdfs:`` resolve as Neptune default prefixes, as
# elsewhere in these queries.)
_DATATYPE_PROPS_OPTIONAL = f"""OPTIONAL {{
              ?property a owl:DatatypeProperty .
              ?property rdfs:domain ?class .
              ?property rdfs:label ?propLabel .
              ?property rdfs:range ?range .
              OPTIONAL {{ ?property <{_SCL_DISTINCT_VALUES}> ?distinctValue . }}
            }}"""
# Approximate tokens per ontology element (for prompt budget estimation)
_TOKENS_PER_CLASS = 20
_TOKENS_PER_PROPERTY = 30
_TOKENS_PER_OBJECT_PROPERTY = 25
_TOKENS_PER_METRIC = 40


_SAFE_URI_RE = re.compile(r"^https?://[^\s<>\"{}|\\^`]+$")


def _is_safe_sparql_uri(uri: str) -> bool:
    """Return True if uri is safe for SPARQL angle-bracket interpolation."""
    return bool(_SAFE_URI_RE.match(uri)) and ">" not in uri


__all__ = ["TBoxContext", "TBoxContextBuilder", "DEFAULT_GRAPH_URI_TEMPLATE"]


@dataclass
class MetricContext:
    """A metric's context for the NL-to-SPARQL prompt."""

    name: str
    description: str = ""
    formula: str = ""
    dimensions: list[str] = field(default_factory=list)


@dataclass
class AiContextTerm:
    """A term parsed from a node's OSI-spec ``:aiContext`` literal.

    Sourced from the ``:aiContext`` JSON literal on an ontology/metric node:
    business synonyms and free-text instructions that ground the LLM's
    translation (e.g. "churn" → the ontology URI; "use net amount, not gross").
    """

    label: str
    synonyms: list[str] = field(default_factory=list)
    instructions: str = ""


@dataclass
class TBoxContext:
    """Ontology T-Box schema subset for LLM prompt inclusion."""

    classes: list[dict[str, Any]]  # [{uri, label, parent}]
    properties: list[dict[str, Any]]  # [{uri, label, domain, range}]
    object_properties: list[dict[str, Any]] = field(default_factory=list)  # [{uri, label, domain, range_class}]
    metrics: list[MetricContext] = field(default_factory=list)
    glossary: list[AiContextTerm] = field(default_factory=list)  # aiContext terms
    token_estimate: int = 0


class TBoxContextBuilder:
    """Builds T-Box context from routing's vector search hits.

    Partitions hits by type, fetches full definitions from Neptune for
    ontology hits, and formats metric context from metric hits.
    """

    def __init__(self, graph_client: GraphClient, graph_uri_template: str | None = None, metric_resolver=None):
        """Bind the graph client, URI template, and optional metric resolver.

        Args:
            graph_client: Graph client used to fetch full ontology definitions.
            graph_uri_template: Optional named-graph URI template; a default is
                resolved when None.
            metric_resolver: Optional Tier-1 resolver that enriches metric hits
                with full Neptune metric definitions.
        """
        self._graph = graph_client
        self._graph_uri_template = get_graph_uri_template(graph_uri_template)
        # optional Tier-1 MetricResolver — when present, metric vector
        # hits are enriched with the FULL Neptune-loaded :GovernedMetric
        # definition (description/dimensions/formula) instead of only the sparse
        # vector-hit metadata. Optional + duck-typed to avoid a hard import cycle.
        self._metric_resolver = metric_resolver

    async def build(
        self,
        vector_hits: list[VectorHit],
        namespace: str,
        query: str = "",
        max_tokens: int = 20_000,
    ) -> TBoxContext:
        """Build T-Box context from vector search hits.

        Args:
            vector_hits: Typed vector search results from routing.
            namespace: Ontology namespace.
            query: Original NL query (used for entity-based fallback if no hits).
            max_tokens: Token budget for the returned context.

        Returns:
            TBoxContext with classes, properties, and metric summaries.
        """
        # Partition hits by type
        ontology_hits = [h for h in vector_hits if h.type in ("ontology_class", "ontology_property", "ontology")]
        metric_hits = [h for h in vector_hits if h.type == "metric"]

        # Build metric context from metric hits
        metrics = self._build_metric_context(metric_hits)

        # BRIDGE: probe ONCE whether this namespace has any coa:isMapped marker
        # (see the LEGACY-NAMESPACE BRIDGE block at module top). ``mapped`` then
        # selects the strict gate (has markers) or the pre-filter fallback (none).
        # Pre-bridge, this probe did not exist and every fetch below was
        # unconditionally gated — to remove, delete this call and pass nothing.
        mapped = await self._namespace_has_mapped_markers(namespace)
        if not mapped:
            logger.info(
                "tbox_ismapped_bridge_fallback",
                namespace=namespace,
                detail=(
                    "namespace has 0 coa:isMapped markers — dropping the mapped-class gate "
                    "and exposing all classes (legacy/pre-marker fallback so Tier-2 keeps working)"
                ),
            )

        # Fetch ontology definitions from Neptune
        classes: list[dict[str, Any]] = []
        properties: list[dict[str, Any]] = []

        # Strategy: For small namespaces (< threshold classes), fetch ALL classes
        # and properties to ensure complete context. This avoids vector-search
        # misses that cause InternalError on valid queries.
        full_context = await self._try_full_namespace_context(namespace, mapped)
        if full_context is not None:
            classes, properties = full_context
        elif ontology_hits:
            classes, properties = await self._fetch_ontology_context(ontology_hits, namespace, mapped)
        elif query:
            # Fallback: use query entities to fetch context
            classes, properties = await self._fetch_by_entities(query, namespace, mapped)

        # Fetch ObjectProperties (FK join paths) only when multiple classes are
        # present — single-class queries don't need join paths, saving a Neptune roundtrip.
        object_properties: list[dict[str, Any]] = []
        if len(classes) > 1:
            object_properties = await self._fetch_object_properties(namespace, mapped)

        # fetch :aiContext glossary (synonyms/instructions) for the hit
        # nodes so business terms map to ontology URIs. Best-effort — never blocks
        # translation; an empty/failed fetch simply yields no glossary section.
        #
        # GATED to the mapped context: a glossary entry grounds a business term to
        # an ontology node ("churn" → <...#Churn>), so it must only reference nodes
        # that actually entered the structured context — the mapped classes /
        # datatype properties fetched above, plus metric hits (metrics are
        # answerable via the Tier-1 metric path). Feeding an UNMAPPED class's
        # aiContext would ground a business term to a class the LLM cannot query —
        # the glossary form of the unmapped-name leak the class / subClassOf-parent
        # / object-property gates already prevent. Intersect the raw hits with the
        # mapped URIs (no extra Neptune round-trip — the sets are already in hand).
        mapped_uris = {c["uri"] for c in classes} | {p["uri"] for p in properties}
        glossary_hits = [h for h in vector_hits if getattr(h, "uri", "") in mapped_uris or h.type == "metric"]
        glossary = await self._fetch_ai_context(glossary_hits, namespace)

        context = TBoxContext(
            classes=classes,
            properties=properties,
            object_properties=object_properties,
            metrics=metrics,
            glossary=glossary,
            token_estimate=self._estimate_tokens(classes, properties, metrics, object_properties),
        )

        if context.token_estimate > max_tokens:
            context = self._truncate(context, max_tokens)

        logger.info(
            "tbox_context_built",
            namespace=namespace,
            classes=len(context.classes),
            properties=len(context.properties),
            metrics=len(context.metrics),
            tokens=context.token_estimate,
        )
        # Dark-namespace canary: the structured-query context resolved to zero
        # classes. Routing still sent a structured question here, so Tier-2
        # will yield nothing and the router falls back to Tier-3 silently. With the
        # bridge above, a pre-marker namespace no longer trips this (its gate is
        # dropped), so reaching here with ``mapped=True`` means a genuinely empty
        # mapped schema (ontology with no R2RML-mapped classes) and with
        # ``mapped=False`` means the namespace has no classes at all. Surfaced so
        # the silent degradation is at least observable in logs.
        if not context.classes:
            # Message branches on ``mapped``: with the bridge, a zero-marker
            # (legacy/NA) namespace already had its gate dropped, so an empty
            # result there means the namespace has no classes at all —
            # re-induction guidance would be wrong. Only the gated (mapped=True)
            # case is a "no R2RML-mapped classes / may predate the marker" state.
            if mapped:
                detail = (
                    "0 coa:isMapped classes reachable — Tier-2 structured queries will "
                    "return no context (ontology has no R2RML-mapped classes; a namespace "
                    "predating the marker would instead take the legacy fallback)"
                )
            else:
                detail = (
                    "namespace has no coa:isMapped markers (legacy fallback active, gate "
                    "already dropped) yet resolved zero classes — it appears to have no "
                    "ontology classes at all; re-induction will not change this"
                )
            logger.warning("tbox_context_no_mapped_classes", namespace=namespace, detail=detail)
        return context

    async def _namespace_has_mapped_markers(self, namespace: str) -> bool:
        """Probe whether the namespace has ANY ``coa:isMapped true`` marker.

        The legacy-namespace bridge: returns True when at least one mapped class
        exists (keep the strict gate), False when none do (drop the gate and
        expose all classes — the pre-marker fallback).

        Fail-safe default is **True** (keep the strict gate) on any error or when
        no graph template is configured: an unfiltered fallback exposes MORE (all
        classes, incl. unmapped/foundational) to the LLM prompt, so defaulting to
        the gate on uncertainty is the conservative choice — it never leaks
        unmapped class names on a transient probe failure. A genuinely-legacy
        namespace whose probe fails simply keeps returning no context (the prior
        behavior), which the dark-namespace canary already logs.
        """
        validate_namespace(namespace)
        if not self._graph_uri_template:
            return True

        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            return True

        ask_sparql = f"""
        ASK {{
          GRAPH ?g {{
            ?c <{_IS_MAPPED_IRI}> true .
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }}
        """
        try:
            # GraphClient.ask returns a plain bool for SPARQL ASK.
            return bool(await self._graph.ask(ask_sparql))
        except Exception as e:
            logger.warning("tbox_ismapped_probe_failed", namespace=namespace, error=str(e))
            return True  # conservative: keep the gate on probe failure

    async def _try_full_namespace_context(
        self,
        namespace: str,
        mapped: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        """For small namespaces, fetch ALL classes and properties.

        Returns None if namespace is too large (> _FULL_CONTEXT_CLASS_THRESHOLD)
        or if the fetch fails. This ensures complete ontology context for the LLM,
        eliminating vector-search misses that cause InternalError on valid queries.
        """
        validate_namespace(namespace)

        if not self._graph_uri_template:
            return None

        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            return None

        # First: count classes to check threshold. When ``mapped`` (the namespace
        # has isMapped markers), counts only MAPPED classes — the ones we'll
        # actually expose — so the threshold reflects the size of the structured
        # schema the LLM sees. In the legacy fallback (``mapped`` False) the gate
        # is dropped and ALL classes count, matching the pre-filter behavior.
        # BRIDGE: this ``gate`` local is used in the count/classes/props queries
        # below. PRE-BRIDGE those queries inlined ``_IS_MAPPED_PATTERN`` directly;
        # to remove, drop this line and substitute ``_IS_MAPPED_PATTERN`` for every
        # ``{gate}`` and ``_MAPPED_PARENT_PATTERN`` for every ``{_parent_pattern(mapped)}``.
        gate = _class_gate(mapped)
        count_sparql = f"""
        SELECT (COUNT(DISTINCT ?class) AS ?cnt)
        WHERE {{
          GRAPH ?g {{
            ?class a owl:Class .
            {gate}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }}
        """
        try:
            count_result = await self._graph.query(count_sparql)
            if not count_result:
                return None
            # Neptune client returns flat binding dicts: {"cnt": "75"}
            cnt_val = count_result[0].get("cnt", "0")
            class_count = int(cnt_val) if isinstance(cnt_val, (int, float)) else int(str(cnt_val))
            if class_count == 0 or class_count > _FULL_CONTEXT_CLASS_THRESHOLD:
                logger.info(
                    "full_context_skipped",
                    namespace=namespace,
                    class_count=class_count,
                    threshold=_FULL_CONTEXT_CLASS_THRESHOLD,
                )
                return None
        except Exception as e:
            logger.warning("full_context_count_failed", namespace=namespace, error=str(e))
            return None

        # Fetch ALL classes and their properties for this namespace. When
        # ``mapped``, ``gate`` (= _IS_MAPPED_PATTERN) is required (absent marker ==
        # class hidden) so unmapped (unstructured / foundational) classes never
        # enter the structured-query prompt; in the legacy fallback ``gate`` is
        # empty and all classes are fetched (pre-filter behavior).
        #
        # TWO queries, NOT one joined query. A single ``?class … OPTIONAL{{?property}}``
        # SELECT emits one row per (class, property) pair; with up to
        # _FULL_CONTEXT_CLASS_THRESHOLD(200) classes each averaging >10 datatype
        # properties, the joined cardinality exceeds LIMIT _SPARQL_RESULT_LIMIT
        # (2000) and — with no ORDER BY — whichever classes' rows land past the cut
        # VANISH ENTIRELY from the parsed result (a mapped class silently missing
        # from the prompt). Splitting guarantees the class list is complete
        # (bounded by the ≤200 threshold, so it can never hit the row cap); only
        # the secondary property list can still truncate, which merely drops some
        # column hints (the class is still present + answerable) and is logged.
        classes_sparql = f"""
        SELECT DISTINCT ?class ?label ?parentClass
        WHERE {{
          GRAPH ?g {{
            ?class a owl:Class .
            {gate}
            ?class rdfs:label ?label .
            {_parent_pattern(mapped)}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """
        # Datatype properties of mapped classes. ``?class`` is gated on isMapped
        # here too: without it, a property whose domain is an UNMAPPED class would
        # make ``_parse_results`` add that unmapped class (with an empty label) to
        # the class dict — reintroducing the exact unmapped-class leak the feature
        # prevents. Restricted to owl:DatatypeProperty (range is xsd:*, never a
        # class IRI); object-property join paths are surfaced by
        # ``_fetch_object_properties`` (mapped-gated on both ends). The optional
        # ``?distinctValue`` carries per-column allowed-values (categorical enum
        # samples) into NL→SQL/SPARQL generation.
        props_sparql = f"""
        SELECT ?class ?property ?range ?propLabel ?distinctValue
        WHERE {{
          GRAPH ?g {{
            ?class a owl:Class .
            {gate}
            ?property a owl:DatatypeProperty .
            ?property rdfs:domain ?class .
            ?property rdfs:label ?propLabel .
            ?property rdfs:range ?range .
            OPTIONAL {{ ?property <{_SCL_DISTINCT_VALUES}> ?distinctValue . }}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """

        try:
            class_results = await self._graph.query(classes_sparql)
            if not class_results:
                return None
            prop_results = await self._graph.query(props_sparql)
            if len(prop_results) >= _SPARQL_RESULT_LIMIT:
                logger.warning(
                    "full_context_properties_truncated",
                    namespace=namespace,
                    limit=_SPARQL_RESULT_LIMIT,
                    detail="datatype-property list hit the row cap; some column hints omitted (classes unaffected)",
                )
            # Class rows FIRST so every mapped class is registered with its label
            # and parent before property rows (which carry no label) are folded in;
            # _parse_results dedups classes by URI, so property rows only append
            # properties and never overwrite a class entry.
            classes, properties = self._parse_results(class_results + prop_results)
            logger.info(
                "full_namespace_context_loaded",
                namespace=namespace,
                classes=len(classes),
                properties=len(properties),
            )
            return classes, properties
        except Exception as e:
            logger.warning("full_context_fetch_failed", namespace=namespace, error=str(e))
            return None

    async def _fetch_object_properties(self, namespace: str, mapped: bool = True) -> list[dict[str, Any]]:
        """Fetch ObjectProperties (FK join paths) for the namespace."""
        if not self._graph_uri_template:
            return []

        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            return []

        # When ``mapped``: only surface object-property edges where BOTH ends are
        # MAPPED classes (both required patterns). Tier-2 turns these into SQL
        # joins; an edge to an unmapped class can't be joined and would
        # reintroduce an unmapped class name into the prompt. Edges between
        # unmapped classes (e.g. unstructured<->unstructured) stay in the graph
        # for Tier-3 / grounding-traversal — just not structured-query
        # material. In the legacy fallback (``mapped`` False, no markers exist)
        # both end-gates are dropped so all join paths surface, matching the
        # pre-filter behavior.
        # BRIDGE: passing ``mapped_gate_iri=None`` is the legacy fallback described
        # above. To remove the bridge, pass ``_IS_MAPPED_IRI`` unconditionally —
        # that yields the pre-bridge query (both ends mapped-gated).
        #
        # The query text itself lives in ``query_utils.object_properties_sparql``
        # because the Tier-2 FK-traversal tool reads the same edges; keeping one
        # definition means a fix there (label handling, scoping, performance)
        # reaches both callers instead of one.
        sparql = object_properties_sparql(
            graph_uri_prefix,
            limit=_OBJECT_PROPERTY_LIMIT,
            mapped_gate_iri=_IS_MAPPED_IRI if mapped else None,
        )
        try:
            results = await self._graph.query(sparql)
            obj_props = []
            for row in results:
                if row.get("op") and row.get("domain") and row.get("range"):
                    obj_props.append(
                        {
                            "uri": row["op"],
                            "label": row.get("opLabel", ""),
                            "domain": row["domain"],
                            "domain_label": row.get("domainLabel", ""),
                            "range_class": row["range"],
                            "range_label": row.get("rangeLabel", ""),
                        }
                    )
            return obj_props
        except Exception as e:
            logger.warning("object_property_fetch_failed", namespace=namespace, error=str(e))
            return []

    async def _fetch_ontology_context(
        self,
        ontology_hits: list[VectorHit],
        namespace: str,
        mapped: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch full class/property definitions from Neptune for ontology vector hits."""
        validate_namespace(namespace)

        if not self._graph_uri_template:
            logger.warning("tbox_missing_graph_uri_template")
            return [], []

        class_uris = [h.uri for h in ontology_hits if h.uri and h.type == "ontology_class"]
        prop_uris = [h.uri for h in ontology_hits if h.uri and h.type == "ontology_property"]

        if not class_uris and not prop_uris:
            return [], []

        all_uris = (class_uris + prop_uris)[:_MAX_SPARQL_VALUES_URIS]

        if len(class_uris) + len(prop_uris) > _MAX_SPARQL_VALUES_URIS:
            logger.warning(
                "tbox_uri_list_truncated",
                total=len(class_uris) + len(prop_uris),
                limit=_MAX_SPARQL_VALUES_URIS,
                namespace=namespace,
            )

        safe_uris = [uri for uri in all_uris if _is_safe_sparql_uri(uri)]
        if not safe_uris:
            logger.warning("tbox_all_uris_rejected", namespace=namespace, count=len(all_uris))
            return [], []

        class_uri_set = set(class_uris)
        prop_uri_set = set(prop_uris)
        safe_class_uris = [u for u in safe_uris if u in class_uri_set]
        safe_prop_uris = [u for u in safe_uris if u in prop_uri_set]

        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            logger.warning("tbox_unsafe_graph_uri_prefix", prefix=graph_uri_prefix, namespace=namespace)
            return [], []

        class_values = " ".join(f"(<{uri}>)" for uri in safe_class_uris) if safe_class_uris else ""
        prop_values = " ".join(f"(<{uri}>)" for uri in safe_prop_uris) if safe_prop_uris else ""

        # BRIDGE: PRE-BRIDGE the UNION clauses below inlined ``_IS_MAPPED_PATTERN``
        # and ``_MAPPED_PARENT_PATTERN`` directly; to remove, drop this local and
        # substitute those constants for ``{gate}`` / ``{_parent_pattern(mapped)}``.
        gate = _class_gate(mapped)
        union_clauses: list[str] = []
        if class_values:
            union_clauses.append(f"""
            {{
              VALUES (?class) {{ {class_values} }}
              ?class a owl:Class .
              {gate}
              ?class rdfs:label ?label .
              {_parent_pattern(mapped)}
              {_DATATYPE_PROPS_OPTIONAL}
            }}""")
        if prop_values:
            # Restricted to owl:DatatypeProperty for the same reason as the
            # class-driven OPTIONAL: a datatype property's range is an xsd:* term
            # (safe + useful column-type signal), while an object property's range
            # is a class IRI that, if unmapped, would leak an unanswerable class
            # name into the prompt. Object-property edges are surfaced separately
            # by ``_fetch_object_properties`` (mapped-gated on both ends).
            union_clauses.append(f"""
            {{
              VALUES (?property) {{ {prop_values} }}
              ?property a owl:DatatypeProperty .
              ?property rdfs:domain ?class .
              ?property rdfs:label ?propLabel .
              OPTIONAL {{ ?property rdfs:range ?range . }}
              OPTIONAL {{ ?property <{_SCL_DISTINCT_VALUES}> ?distinctValue . }}
              ?class a owl:Class .
              {gate}
              ?class rdfs:label ?label .
              {_parent_pattern(mapped)}
            }}""")

        sparql = f"""
        SELECT ?class ?property ?range ?label ?propLabel ?parentClass ?distinctValue
        WHERE {{
          GRAPH ?g {{
            {" UNION ".join(union_clauses)}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """

        try:
            results = await self._graph.query(sparql)
        except Exception as e:
            logger.error("tbox_fetch_failed", namespace=namespace, error=str(e))
            return [], []

        return self._parse_results(results)

    async def _fetch_by_entities(
        self,
        query: str,
        namespace: str,
        mapped: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fallback: fetch ontology context by matching NL query entities against class labels."""
        validate_namespace(namespace)
        search_plan = build_query_search_plan(query, max_count=5)

        if not search_plan.terms and not search_plan.containers:
            logger.info("tbox_entity_fetch_no_search_candidates", namespace=namespace, query_length=len(query))
            return [], []

        if not self._graph_uri_template:
            logger.warning("tbox_missing_graph_uri_template")
            return [], []

        forward_filters = [
            f'CONTAINS(LCASE(?label), "{escape_sparql_string_literal(normalize_label_match_text(term))}")'
            for term in search_plan.terms
        ]
        # ``STR()`` is required on the reverse arm and not on the forward one:
        # SPARQL argument compatibility accepts CONTAINS(lang-tagged, simple) but
        # rejects CONTAINS(simple, lang-tagged) as a type error, which FILTER then
        # swallows as false. Without STR() every ``@ja``/``@th`` label would be
        # invisible to reverse containment. STRLEN keeps one-character labels from
        # matching any query that happens to contain that character.
        reverse_filters = [
            "(STRLEN(STR(?label)) >= 2 && "
            f'CONTAINS("{escape_sparql_string_literal(normalize_label_match_text(container))}", '
            "LCASE(STR(?label))))"
            for container in search_plan.containers
        ]
        filters = " || ".join(forward_filters + reverse_filters)
        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            logger.warning("tbox_unsafe_graph_uri_prefix", prefix=graph_uri_prefix)
            return [], []

        # BRIDGE: PRE-BRIDGE, ``{_class_gate(mapped)}`` was ``{_IS_MAPPED_PATTERN}``
        # and ``{_parent_pattern(mapped)}`` was ``{_MAPPED_PARENT_PATTERN}`` (both
        # unconditional).
        #
        # Split into a class query and a property query for the same reason
        # ``_try_full_namespace_context`` is split: joined against the datatype
        # OPTIONAL, one class fans out to (properties x distinct values x parents x
        # labels) rows, and with no ORDER BY a class whose rows all land past
        # ``_SPARQL_RESULT_LIMIT`` vanishes entirely from the parsed result — a
        # matched class silently missing from the structured prompt. Reverse
        # containment made that reachable: it matches far more classes than a
        # handful of short forward terms did, and a multilingual namespace carries
        # one label per language per class. DISTINCT plus the bounded class list
        # keeps the class query one row per (class, label, parent).
        classes_sparql = f"""
        SELECT DISTINCT ?class ?label ?parentClass
        WHERE {{
          GRAPH ?g {{
            ?class a owl:Class .
            {_class_gate(mapped)}
            ?class rdfs:label ?label .
            FILTER({filters})
            {_parent_pattern(mapped)}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """

        try:
            class_results = await self._graph.query(classes_sparql)
        except Exception as e:
            logger.error("tbox_entity_fetch_failed", namespace=namespace, error=str(e))
            return [], []

        if not class_results:
            return [], []

        if len(class_results) >= _SPARQL_RESULT_LIMIT:
            # The class query is one row per (class, label, parent) and is not
            # ordered, so past the cap it is Neptune's row order that decides which
            # matched classes reach the prompt. Reachable on a large multilingual
            # namespace, and silent until now.
            logger.warning(
                "tbox_entity_classes_truncated",
                namespace=namespace,
                limit=_SPARQL_RESULT_LIMIT,
                detail="matched-class rows hit the row cap; some matched classes may be missing from the prompt",
            )

        # De-duplicate before the VALUES cap. The class query is DISTINCT over
        # (?class, ?label, ?parentClass), so a class carrying several language
        # labels — exactly the namespaces this MR serves — contributes one row per
        # label. Capping the raw rows let 50 VALUES entries cover far fewer distinct
        # classes and left the rest with no column hints, and the duplicates
        # multiplied the property rows as well.
        matched_uris = list(
            dict.fromkeys(
                row["class"] for row in class_results if row.get("class") and _is_safe_sparql_uri(row["class"])
            )
        )
        if not matched_uris:
            return self._parse_results(class_results)

        if len(matched_uris) > _MAX_SPARQL_VALUES_URIS:
            logger.info(
                "tbox_entity_property_uris_truncated",
                namespace=namespace,
                matched=len(matched_uris),
                limit=_MAX_SPARQL_VALUES_URIS,
                detail="column hints fetched for the first N matched classes; all matched classes still surface",
            )

        # Properties are fetched for the classes the label match already selected,
        # so this query cannot introduce a class the gate above rejected. Bounded
        # by the same VALUES cap the vector-hit path uses.
        class_values = " ".join(f"(<{uri}>)" for uri in matched_uris[:_MAX_SPARQL_VALUES_URIS])
        props_sparql = f"""
        SELECT ?class ?property ?range ?propLabel ?distinctValue
        WHERE {{
          GRAPH ?g {{
            VALUES (?class) {{ {class_values} }}
            ?property a owl:DatatypeProperty .
            ?property rdfs:domain ?class .
            ?property rdfs:label ?propLabel .
            ?property rdfs:range ?range .
            OPTIONAL {{ ?property <{_SCL_DISTINCT_VALUES}> ?distinctValue . }}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """

        try:
            prop_results = await self._graph.query(props_sparql)
        except Exception as e:
            # A property-list failure only costs column hints; the matched classes
            # are already answerable, so degrade rather than drop them.
            logger.warning("tbox_entity_property_fetch_failed", namespace=namespace, error=str(e))
            prop_results = []

        if len(prop_results) >= _SPARQL_RESULT_LIMIT:
            logger.warning(
                "tbox_entity_properties_truncated",
                namespace=namespace,
                limit=_SPARQL_RESULT_LIMIT,
                detail="datatype-property list hit the row cap; some column hints omitted (classes unaffected)",
            )

        # Class rows FIRST so every matched class is registered with its label and
        # parent before property rows (which carry no label) are folded in.
        return self._parse_results(class_results + prop_results)

    async def _fetch_ai_context(self, vector_hits: list[VectorHit], namespace: str) -> list[AiContextTerm]:
        """Fetch :aiContext JSON literals from Neptune for the hit nodes.

        ``:aiContext`` carries business synonyms + instructions authored per
        ontology/metric node. We batch-query the literal for the hit URIs and
        parse synonyms/instructions into AiContextTerm entries.

        Fail-open: any error, missing template, or no safe URIs returns [] so
        translation proceeds without a glossary (never raises).
        """
        uris = [h.uri for h in vector_hits if getattr(h, "uri", "") and _is_safe_sparql_uri(h.uri)]
        if not uris or not self._graph_uri_template:
            return []
        uris = uris[:_MAX_SPARQL_VALUES_URIS]
        try:
            validate_namespace(namespace)
        except Exception:
            return []

        # Scope to the namespace's graph(s) so we never read another namespace's
        # :aiContext (matches the _fetch_by_entities graph-prefix filter pattern).
        graph_uri_prefix = namespace_graph_prefix(self._graph_uri_template, namespace)
        if not _is_safe_sparql_uri(graph_uri_prefix):
            logger.warning("aicontext_unsafe_graph_uri_prefix", prefix=graph_uri_prefix)
            return []

        values_clause = " ".join(f"(<{u}>)" for u in uris)
        sparql = f"""
        SELECT ?s ?label ?aiContext
        WHERE {{
          GRAPH ?g {{
            VALUES (?s) {{ {values_clause} }}
            ?s <urn:{URN_PREFIX}:vocab#aiContext> ?aiContext .
            OPTIONAL {{ ?s rdfs:label ?label . }}
          }}
          FILTER(STRSTARTS(STR(?g), "{graph_uri_prefix}"))
        }} LIMIT {_SPARQL_RESULT_LIMIT}
        """
        try:
            results = await self._graph.query(sparql)
        except Exception as e:
            logger.warning("aicontext_fetch_failed", namespace=namespace, error=str(e))
            return []

        return self._parse_ai_context(results)

    @staticmethod
    def _parse_ai_context(results: list[dict[str, Any]]) -> list[AiContextTerm]:
        """Parse :aiContext JSON literals into AiContextTerm entries."""
        import json as _json

        terms: list[AiContextTerm] = []
        for row in results:
            raw = row.get("aiContext", "")
            if not raw:
                continue
            try:
                ctx = _json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(ctx, dict):
                continue
            synonyms = ctx.get("synonyms", [])
            if not isinstance(synonyms, list):
                synonyms = []
            instructions = ctx.get("instructions", "")
            if not isinstance(instructions, str):
                instructions = ""
            label = row.get("label", "") or ctx.get("label", "")
            if synonyms or instructions:
                terms.append(
                    AiContextTerm(
                        label=str(label),
                        synonyms=[str(s) for s in synonyms],
                        instructions=instructions,
                    )
                )
        return terms

    def _build_metric_context(self, metric_hits: list[VectorHit]) -> list[MetricContext]:
        """Build metric context for the prompt from metric vector hits.

        when a Tier-1 MetricResolver is available, enrich each hit with
        the FULL Neptune-loaded :GovernedMetric definition (matched by metric_id
        or name); fall back to the sparse vector-hit metadata when the resolver
        has no entry (e.g. a hit that isn't a governed metric, or resolver not
        yet loaded). Never raises — enrichment is best-effort.
        """
        metrics = []
        for hit in metric_hits:
            meta = hit.metadata
            name = meta.get("name", hit.entity_id)
            description = meta.get("description", "")
            formula = meta.get("formula", "")
            dimensions = meta.get("dimensions", [])

            defn = self._resolve_full_metric(hit, name)
            if defn is not None:
                # Prefer the authoritative Neptune definition; keep hit metadata
                # only where the resolver leaves a field empty.
                name = defn.name or name
                description = defn.description or description
                formula = defn.sql_template or formula
                dimensions = defn.dimensions or dimensions

            metrics.append(MetricContext(name=name, description=description, formula=formula, dimensions=dimensions))
        return metrics

    def _resolve_full_metric(self, hit: VectorHit, name: str):
        """Look up the full metric definition for a hit (by id then name). Best-effort."""
        if self._metric_resolver is None:
            return None
        try:
            entity_id = hit.metadata.get("metric_id") or hit.entity_id
            defn = self._metric_resolver.lookup(entity_id) if entity_id else None
            if defn is None and name:
                # Fall back to name-keyed lookup via the resolver's snapshot.
                defn = self._metric_resolver._snapshot.by_name.get(name.lower())
            return defn
        except Exception:
            return None

    def _parse_results(self, results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Parse Neptune SPARQL results into classes and properties.

        A property may span multiple result rows when it carries several
        coa:distinctValues (one row per value). We key properties by
        (domain, uri) and accumulate distinct sample values so the property
        appears once with all its sampled enum literals.
        """
        classes: dict[str, dict[str, Any]] = {}
        properties: dict[tuple[str, str], dict[str, Any]] = {}

        for row in results:
            cls_uri = row.get("class")
            if cls_uri and cls_uri not in classes:
                classes[cls_uri] = {
                    "uri": cls_uri,
                    "label": row.get("label", ""),
                    "parent": row.get("parentClass"),
                }
            prop_uri = row.get("property")
            if prop_uri and cls_uri:
                key = (cls_uri, prop_uri)
                prop = properties.get(key)
                if prop is None:
                    prop = {
                        "uri": prop_uri,
                        "label": row.get("propLabel", ""),
                        "domain": cls_uri,
                        "range": row.get("range", ""),
                        "distinct_values": [],
                    }
                    properties[key] = prop
                sample = row.get("distinctValue")
                if sample and sample not in prop["distinct_values"]:
                    prop["distinct_values"].append(str(sample))

        return list(classes.values()), list(properties.values())

    def _estimate_tokens(
        self,
        classes: list[dict[str, Any]],
        properties: list[dict[str, Any]],
        metrics: list[MetricContext],
        object_properties: list[dict[str, Any]] | None = None,
    ) -> int:
        """Estimate token count for the T-Box context."""
        return (
            (len(classes) * _TOKENS_PER_CLASS)
            + (len(properties) * _TOKENS_PER_PROPERTY)
            + (len(object_properties or []) * _TOKENS_PER_OBJECT_PROPERTY)
            + (len(metrics) * _TOKENS_PER_METRIC)
        )

    def _truncate(self, context: TBoxContext, max_tokens: int) -> TBoxContext:
        """Proportionally trim context to fit within the token budget.

        Lists are ordered by relevance (vector search cosine similarity),
        so slicing from the front preserves the most important items.
        """
        ratio = max_tokens / max(context.token_estimate, 1)
        max_classes = max(int(len(context.classes) * ratio), 1)
        max_props = int(len(context.properties) * ratio)
        max_obj_props = int(len(context.object_properties) * ratio)
        max_metrics = max(int(len(context.metrics) * ratio), 1)
        truncated_classes = context.classes[:max_classes]
        truncated_props = context.properties[:max_props]
        truncated_obj_props = context.object_properties[:max_obj_props]
        truncated_metrics = context.metrics[:max_metrics]
        return TBoxContext(
            classes=truncated_classes,
            properties=truncated_props,
            object_properties=truncated_obj_props,
            metrics=truncated_metrics,
            glossary=context.glossary,  # glossary is small; keep it intact on truncation
            token_estimate=self._estimate_tokens(
                truncated_classes, truncated_props, truncated_metrics, truncated_obj_props
            ),
        )

    def format_for_prompt(self, context: TBoxContext, namespace: str) -> str:
        """Format TBoxContext as text for inclusion in an LLM prompt.

        Uses prefixed short forms (ind:ClassName) so the LLM generates SPARQL
        with correct URIs instead of inventing its own prefix.
        """
        prefix_uri = ""
        if context.classes and context.classes[0].get("uri"):
            uri = context.classes[0]["uri"]
            prefix_uri = uri.rsplit("#", 1)[0] + "#" if "#" in uri else uri.rsplit("/", 1)[0] + "/"

        _XSD = "http://www.w3.org/2001/XMLSchema#"

        def _shorten(uri: str) -> str:
            if prefix_uri and uri.startswith(prefix_uri):
                return f"ind:{uri[len(prefix_uri) :]}"
            if uri.startswith(_XSD):
                return f"xsd:{uri[len(_XSD) :]}"
            return f"<{uri}>"

        sections = [f"Ontology namespace: {namespace}"]
        if prefix_uri:
            sections.append(f"PREFIX ind: <{prefix_uri}>")
            sections.append(f"PREFIX xsd: <{_XSD}>")
            sections.append("")

        if context.classes:
            sections.append("Classes:")
            for c in context.classes:
                short = _shorten(c["uri"])
                line = f" {short} (label: {c['label']})"
                if c.get("parent"):
                    line += f" subClassOf: {_shorten(c['parent'])}"
                sections.append(line)

        if context.properties:
            sections.append("\nProperties:")
            for p in context.properties:
                domain = _shorten(p["domain"])
                rng = _shorten(p["range"])
                line = f" {_shorten(p['uri'])} (label: {p['label']}) domain: {domain} range: {rng}"
                # Sampled distinct values for categorical columns — tells the LLM
                # the EXACT literals to use in FILTER/WHERE clauses instead of
                # guessing (e.g. return_state = "RETURNED", not "returned").
                distinct_values = p.get("distinct_values") or []
                if distinct_values:
                    shown = ", ".join(f'"{v}"' for v in distinct_values[:_MAX_DISTINCT_VALUES_IN_PROMPT])
                    line += f" allowed values: [{shown}]"
                sections.append(line)

        if context.object_properties:
            sections.append("\nJoin paths (ObjectProperties — use these for multi-table queries):")
            for op in context.object_properties:
                domain_short = _shorten(op["domain"]) if op.get("domain") else "?"
                range_short = _shorten(op["range_class"]) if op.get("range_class") else "?"
                op_short = _shorten(op["uri"])
                domain_label = op.get("domain_label", "")
                range_label = op.get("range_label", "")
                label_hint = ""
                if domain_label and range_label:
                    label_hint = f" ({domain_label} → {range_label})"
                sections.append(f" {domain_short} → {range_short} via {op_short}{label_hint}")

        if context.metrics:
            sections.append("\nMetrics:")
            for m in context.metrics:
                line = f" Metric: {m.name}"
                if m.description:
                    line += f" — {m.description}"
                if m.formula:
                    line += f" [formula: {m.formula}]"
                if m.dimensions:
                    line += f" [dimensions: {', '.join(m.dimensions)}]"
                sections.append(line)

        # glossary grounds business terms to the ontology. Synonyms tell
        # the LLM which term maps to which class/property; instructions carry
        # author guidance (e.g. which field to prefer).
        if context.glossary:
            sections.append("\nGlossary (business terms → ontology):")
            for term in context.glossary:
                parts: list[str] = []
                if term.label:
                    parts.append(term.label)
                if term.synonyms:
                    parts.append(f"synonyms: {', '.join(term.synonyms)}")
                if term.instructions:
                    parts.append(f"note: {term.instructions}")
                if parts:
                    sections.append(f" - {' | '.join(parts)}")

        return "\n".join(sections)
