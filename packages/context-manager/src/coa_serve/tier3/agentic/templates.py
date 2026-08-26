# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepared, parameterized graph-query templates for the Agentic Retriever.

A Query_Template is a *prepared* retrieval pattern the agent fills in rather than
authoring a graph query from scratch (Req 5). Preferring a template over an
Ad_Hoc_Query keeps graph retrieval reliable and avoids query-generation errors
(Req 5.1-5.3). This module owns the templates the ``Graph_Traversal_Tool`` runs,
including the one required by Requirement 5.5 — the edge-typed traversal.

Which graph these target (load-bearing — see the design's "Two graphs")
----------------------------------------------------------------------
These are **openCypher** templates over the graphrag-toolkit **property graph**
(the graph kg-build populates in Neptune Database), NOT SPARQL over the RDF
ontology graph. They are executed through the toolkit ``GraphStore`` (which
speaks openCypher to Neptune), never through the serve layer's SPARQL
``GraphTraverser``/``NeptuneGraphClient``.

Property-graph schema these templates assume (verified against the toolkit
``EntityGraphBuilder`` / ``EntityRelationGraphBuilder`` / ``StatementGraphBuilder``):

- Entity node label ``__Entity__``, **tenant-scoped** — the caller formats it via
  ``TenantId(tenant).format_label("__Entity__")`` and passes the already
  backtick-wrapped label as ``entity_label``. The entity's ``entityId`` is stored
  as the Neptune node id, so entities are matched/returned with ``id(...)`` (true
  for both Neptune Database and Neptune Analytics, which both key entities by
  ``~id``).
- Entity properties: ``value`` (display name) and ``class`` (classification).
- Entity↔entity relations: ``(:__Entity__)-[r:`__RELATION__` {value: <predicate>}]-(:__Entity__)``.
  There is a single relationship type ``__RELATION__``; the edge type is the
  ``value`` property on it (an LLM-extracted predicate string).
- Connected-node text: an entity's supporting statements are reached via
  ``(:__Entity__)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(:__Statement__)``;
  the statement text is the ``__Statement__.value`` property. The statement label
  is also tenant-scoped (``statement_label``).

Import discipline (Requirement 12.3): this module is dependency-free — it holds
only template strings, bound constants, and a normalization helper, and never
imports ``graphrag_toolkit`` (nor anything that does), so importing it never pulls
the toolkit into ``sys.modules``.

Parameter discipline: entity ids and edge types are passed to Neptune as
openCypher **query parameters** (``$entity_ids``, ``$edge_types_norm``,
``$terms``) — never interpolated into the query text — so there is no
query-injection surface. Only the tenant-scoped labels (derived internally, never
from caller input) and the integer ``limit`` are formatted into the query text.
"""

from __future__ import annotations

# Inclusive parameter bounds for the edge-typed traversal template (Req 5.5).
# The Graph_Traversal_Tool enforces these before populating the template so an
# out-of-bounds request is never assembled into a query.
MIN_ENTITIES: int = 1
MAX_ENTITIES: int = 100
MIN_EDGE_TYPES: int = 1
MAX_EDGE_TYPES: int = 50

# Default row cap for a traversal result set.
DEFAULT_EDGE_TYPED_LIMIT: int = 100

# ── Chunk-window expansion bounds (idea 6) ─────────────────────────────
# Inclusive bounds for the chunk-window expansion template. Enforced by the
# Chunk_Expansion_Tool before a query is assembled.
MIN_CHUNK_IDS: int = 1
MAX_CHUNK_IDS: int = 50
DEFAULT_CHUNK_WINDOW_HOPS: int = 1
MAX_CHUNK_WINDOW_HOPS: int = 5
DEFAULT_CHUNK_WINDOW_LIMIT: int = 50


def normalize_edge_type(value: str) -> str:
    """Normalize an edge-type label for cross-graph matching (Req 3.9 mapping).

    The RDF ontology's edge-type labels and the property graph's
    ``__RELATION__.value`` predicate strings come from independent vocabularies,
    so they are compared on a normalized form rather than byte-for-byte. This
    Python normalization MUST stay identical to the openCypher normalization
    applied to ``r.value`` in the templates below (``_CYPHER_EDGE_NORM``):
    lowercase, then map spaces and hyphens to underscores.

    Args:
        value: An edge-type label (from the ontology) or a raw ``r.value``.

    Returns:
        The normalized comparison key.
    """
    return value.lower().replace(" ", "_").replace("-", "_")


# The openCypher expression that normalizes a stored ``r.value`` the same way
# :func:`normalize_edge_type` normalizes the requested edge types. Kept as a
# named constant so the two normalizations cannot drift.
_CYPHER_EDGE_NORM: str = "toLower(replace(replace(r.value, ' ', '_'), '-', '_'))"


# The Requirement 5.5 prepared template: traverse the given edge types from the
# given entities and return the connected nodes plus their supporting statement
# text. ``entity_label`` / ``statement_label`` are tenant-scoped, already
# backtick-wrapped labels; ``$entity_ids`` / ``$edge_types_norm`` are query
# parameters; ``limit`` is an int. ``collect(DISTINCT stext)`` drops nulls, so a
# connected node with no supporting statement yields an empty text list.
EDGE_TYPED_TRAVERSAL: str = (
    "MATCH (s:{entity_label})-[r:`__RELATION__`]-(t:{entity_label})\n"
    "WHERE id(s) IN $entity_ids\n"
    "  AND " + _CYPHER_EDGE_NORM + " IN $edge_types_norm\n"
    "OPTIONAL MATCH (t)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(st:{statement_label})\n"
    "WITH id(s) AS source, r.value AS edge, id(t) AS target,\n"
    "     t.value AS targetLabel, t.class AS targetClass, st.value AS stext\n"
    "RETURN source, edge, target, targetLabel, targetClass,\n"
    "       collect(DISTINCT stext) AS targetTexts\n"
    "LIMIT {limit}"
)


# ``uris`` mode: expand one hop over any ``__RELATION__`` edge from the given
# entity ids (no edge-type constraint). Same connected-node shape as the
# edge-typed template minus the text hop.
URI_EXPANSION: str = (
    "MATCH (s:{entity_label})-[r:`__RELATION__`]-(t:{entity_label})\n"
    "WHERE id(s) IN $entity_ids\n"
    "RETURN id(s) AS source, r.value AS edge, id(t) AS target,\n"
    "       t.value AS targetLabel, t.class AS targetClass\n"
    "LIMIT {limit}"
)


# ``edge_types`` mode: discover the edge-type predicates actually PRESENT on the
# given entities in the graph, with an occurrence count so the planner can prefer
# the most common ones. This exists because the induced ontology is a heavily-
# pruned subset of the graph's predicate vocabulary (~14% in measured cases), so
# the planner — shown only ontology edges — cannot otherwise propose the majority
# of real, relevant predicates (e.g. financial relations like "HAS SEGMENT").
# Returns the RAW ``r.value`` (not normalized) so the planner sees real labels; the
# edge-typed traversal re-normalizes on match. ``$entity_ids`` is a query
# parameter; ``limit`` is an int.
ENTITY_EDGE_TYPES: str = (
    "MATCH (s:{entity_label})-[r:`__RELATION__`]-(:{entity_label})\n"
    "WHERE id(s) IN $entity_ids\n"
    "RETURN r.value AS edge, count(*) AS freq\n"
    "ORDER BY freq DESC\n"
    "LIMIT {limit}"
)


# ``keyword`` mode: match entities by their persisted ``search_str``. ``$terms``
# are per-word forward candidates (``search_str CONTAINS term``); ``$containers``
# are the query's non-Latin script runs for the reverse direction (``container
# CONTAINS search_str``), which is how a stored label is found inside a Chinese,
# Japanese, Thai or Korean clause the serve layer cannot segment. ``$probes`` is the
# union in source order, because Neptune openCypher supports neither the ``any()``
# list predicate nor two UNWIND arms in one WHERE. All three values are parameters,
# never interpolated.
KEYWORD_ENTITY_SEARCH: str = (
    "UNWIND $probes AS probe\n"
    "MATCH (e:{entity_label})\n"
    "WHERE probe IN $terms AND e.search_str CONTAINS probe\n"
    "RETURN DISTINCT id(e) AS uri, e.value AS label, e.class AS class\n"
    "LIMIT {limit}"
)


# The reverse arm is a SEPARATE template so a query with no containers never ships
# it. ``size()`` on a string is supported by Neptune ("this overloaded method
# currently only works for pattern expressions, lists, and strings" — openCypher
# specification compliance), but it is the only use of it here, and a rejected
# construct would degrade keyword mode for EVERY language rather than only the
# scripts that need the reverse direction.
#
# The floor is counted over the search string WITHOUT spaces, unlike the SPARQL
# sinks' ``STRLEN(STR(?label)) >= 2``. The toolkit's ``search_str`` normalizer
# replaces every combining mark with a space, so a Thai or Indic label arrives
# shredded (``รุ่น`` -> ``ร น``); counting raw length would admit one-character
# fragments that appear inside almost any container of the same script.
KEYWORD_ENTITY_SEARCH_WITH_CONTAINERS: str = (
    "UNWIND $probes AS probe\n"
    "MATCH (e:{entity_label})\n"
    "WHERE (probe IN $terms AND e.search_str CONTAINS probe)\n"
    "   OR (probe IN $containers AND e.search_str IS NOT NULL\n"
    "       AND size(replace(e.search_str, ' ', '')) >= 2\n"
    "       AND probe CONTAINS e.search_str)\n"
    "RETURN DISTINCT id(e) AS uri, e.value AS label, e.class AS class\n"
    "LIMIT {limit}"
)


# Chunk-window expansion (idea 6): given chunk ids the agent deemed relevant,
# return the chunks PHYSICALLY ADJACENT in the source document by traversing the
# toolkit's chunk-sequence edges (`__NEXT__` / `__PREVIOUS__`) up to `hops` hops.
# Verified on the dev graph: chunks are keyed by the Neptune node id (the
# ``chunkId`` property is null), and the chunk text lives in ``c.value`` — so
# seeds are matched and neighbors returned via ``id(...)``. ``chunk_label`` is the
# tenant-scoped, backtick-wrapped ``__Chunk__`` label; ``$chunk_ids`` is a query
# parameter; ``hops``/``limit`` are ints. The seed chunks themselves are excluded
# from the result (they are already in the accumulated context). An undirected
# match (`-[...]-`) returns neighbors on BOTH sides (before and after).
CHUNK_WINDOW_EXPANSION: str = (
    "MATCH (c:{chunk_label})\n"
    "WHERE id(c) IN $chunk_ids\n"
    "MATCH (c)-[:`__NEXT__`|`__PREVIOUS__`*1..{hops}]-(adj:{chunk_label})\n"
    "WHERE NOT id(adj) IN $chunk_ids\n"
    "RETURN DISTINCT id(adj) AS chunkId, adj.value AS text\n"
    "LIMIT {limit}"
)
