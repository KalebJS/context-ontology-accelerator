# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neo4j implementation of the LexicalGraphStore protocol (local Docker stack).

Production reads the graphrag-toolkit lexical graph from Neptune DB via the
``neptunedata`` openCypher API (:mod:`ndb_lexical`). The local Docker stack
replaces Neptune with Fuseki (SPARQL metadata) plus a Neo4j container for the
document/lexical graph, and Fuseki cannot serve the neptunedata API — so the
induction read path needs a bolt:// backend. This class fills that gap,
mirroring ``ndb_lexical.NeptuneDatabaseLexicalStore`` query-for-query against
the ``neo4j`` Python driver.

Routing is env-switched: ``induce_unstructured._build_lexical_store``
constructs this store when ``GRAPH_STORE_URI`` (a bolt:// URI, the same
convention as ``kg_build.graph_store_uri`` and the coa_serve client factory)
is set. When unset, the production neptunedata path is unchanged.

Label scoping matches the graphrag-toolkit tenant scheme exactly (verified
against the populated local Neo4j graph): ``__SYS_Class__`` becomes
``__SYS_Class__<tenant>__`` as a backticked label, while edge labels
(``__SYS_RELATION__``, ``__SUBJECT__``, ``__OBJECT__``, ``__BELONGS_TO__``)
are NOT tenant-suffixed.
"""

from __future__ import annotations

import logging
import time
from urllib.parse import unquote, urlsplit

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired, TransientError

from coa_ontology.inducer.unstructured.stores.na_lexical import (
    _BATCH_SIZE,
    _MAX_RETRIES,
    _extract_spc_predicate_complement,
    _extract_spo_predicate,
)
from coa_ontology.inducer.unstructured.stores.protocol import (
    ClassRecord,
    ClassSchemaResult,
    EntityRecord,
    FactRecord,
    RelationRecord,
    TopicRecord,
)

log = logging.getLogger(__name__)

# URI schemes accepted for the driver connection this store opens. ``bolt+s``/
# ``bolt+ssc`` cover TLS-terminated Neo4j; plain ``bolt`` is what the local
# Docker stack exposes.
_ALLOWED_SCHEMES = ("bolt", "bolt+s", "bolt+ssc")


class Neo4jLexicalStore:
    """LexicalGraphStore backed by Neo4j over bolt (local Docker stack).

    Args:
        uri: Bolt URI including credentials, e.g.
            ``bolt://neo4j:password@neo4j:7687`` (same ``GRAPH_STORE_URI``
            convention as the other graph consumers). A URI without
            credentials connects unauthenticated (auth=None).
        tenant_id: Tenant ID suffix for multi-tenant label scoping
            (e.g. ``1e57a8ef3189454989f44481e``).

    Raises:
        ValueError: If ``uri`` is not a bolt:// (or bolt+s://) URI — this is
            the guard that stops a production-style Neptune URL from ever
            reaching the driver.
    """

    def __init__(self, uri: str, tenant_id: str = "") -> None:
        """Open a Neo4j driver for the given bolt URI.

        The ``GRAPH_STORE_URI`` convention embeds credentials in the netloc
        (``bolt://user:password@host:7687``), but the neo4j driver refuses
        credentials IN the URI — they must go through the ``auth`` kwarg, so
        this constructor strips them and connects to the bare host URI.

        Args:
            uri: Bolt URI including credentials (parsed from the netloc).
            tenant_id: Tenant ID suffix for multi-tenant label scoping.

        Raises:
            ValueError: If the URI scheme is not one of the bolt schemes.
        """
        scheme = (urlsplit(uri).scheme or "").lower()
        if scheme not in _ALLOWED_SCHEMES:
            raise ValueError(
                f"Neo4jLexicalStore requires a bolt:// (or bolt+s://) URI for the local "
                f"document graph; got scheme {scheme!r} in {uri!r}. Set GRAPH_STORE_URI to "
                "bolt://user:password@neo4j:7687 (the Neptune paths are separate)."
            )
        self._tenant_id = tenant_id
        parsed = urlsplit(uri)
        if parsed.username:
            auth: tuple[str, str] | None = (unquote(parsed.username), unquote(parsed.password or ""))
            netloc = parsed.hostname or ""
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"
            driver_uri = f"{parsed.scheme}://{netloc}"
        else:
            auth = None
            driver_uri = uri
        # Keep the credential-free form for error messages (the raw URI embeds
        # the password — never log it).
        self._uri = driver_uri
        self._driver: Driver = GraphDatabase.driver(driver_uri, auth=auth)

    def close(self) -> None:
        """Close the driver connection pool."""
        self._driver.close()

    def _label(self, base: str) -> str:
        """Scope a label with tenant ID if set: __Entity__ → __Entity__<tenant_id>__."""
        if self._tenant_id:
            return f"`__{base}__{self._tenant_id}__`"
        return f"`__{base}__`"

    def _cypher(self, cypher: str, params: dict | None = None) -> list[dict]:
        """Execute a Cypher query and return the records as plain dicts.

        Retries transient failures (connection blips, transient server
        errors) with the same exponential backoff the Neptune
        implementations use; wraps permanent failures in RuntimeError so
        the pipeline's error contract holds.

        Args:
            cypher: Cypher query string (whitespace is collapsed, mirroring
                the Neptune stores; queries here are parameterized and hold
                no string literals).
            params: Optional bound parameters.

        Returns:
            List of result record dicts keyed by RETURN alias.

        Raises:
            RuntimeError: If the query fails permanently (or retries are
                exhausted).
        """
        query = " ".join(cypher.split())
        run_params = params or {}
        last_error: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                with self._driver.session() as session:
                    result = session.run(query, run_params)
                    return [record.data() for record in result]
            except (TransientError, ServiceUnavailable, SessionExpired) as e:
                last_error = e
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(0.1 * (2**attempt))
                    continue
            except Neo4jError as e:
                raise RuntimeError(
                    f"Neo4j query failed (uri={self._uri}, error={type(e).__name__}): {e}"
                ) from e
            except Exception as e:
                raise RuntimeError(f"Neo4j query failed (uri={self._uri}): {e}") from e

        raise RuntimeError(
            f"Neo4j query failed after {_MAX_RETRIES} attempts (uri={self._uri})"
        ) from last_error

    # ── Protocol implementation ─────────────────────────────────────────

    def get_class_nodes(self) -> list[ClassRecord]:
        """Return all class nodes with their occurrence counts, most frequent first.

        Returns:
            One :class:`ClassRecord` per ``SYS_Class`` node in the graph.
        """
        results = self._cypher(
            f"MATCH (c:{self._label('SYS_Class')}) RETURN c.value AS value, c.count AS count ORDER BY c.count DESC"
        )
        return [
            ClassRecord(value=row["value"], count=int(row["count"]) if row.get("count") is not None else 0)
            for row in results
            if row.get("value") is not None
        ]

    def get_class_relations(self) -> list[RelationRecord]:
        """Return the class-to-class relations with their occurrence counts.

        Returns:
            One :class:`RelationRecord` per ``SYS_RELATION`` edge between class nodes.
        """
        results = self._cypher(
            f"MATCH (s:{self._label('SYS_Class')})-[r:`__SYS_RELATION__`]->(t:{self._label('SYS_Class')}) "
            "RETURN s.value AS source_class, r.value AS predicate, "
            "t.value AS target_class, r.count AS count"
        )
        return [
            RelationRecord(
                source_class=row["source_class"],
                predicate=row["predicate"],
                target_class=row["target_class"],
                count=int(row["count"]) if row.get("count") is not None else 0,
            )
            for row in results
            if row.get("source_class") and row.get("predicate") and row.get("target_class")
        ]

    def get_entities_by_class(self, classification: str, limit: int = 100) -> list[EntityRecord]:
        """Return entities whose classification matches, up to a limit.

        Args:
            classification: Class value to filter entities by.
            limit: Maximum number of entities to return.

        Returns:
            The matching :class:`EntityRecord` list.
        """
        results = self._cypher(
            f"MATCH (e:{self._label('Entity')}) "
            "WHERE e.class = $classification "
            "RETURN elementId(e) AS entity_id, e.value AS value, e.class AS classification "
            "LIMIT $limit",
            params={"classification": classification, "limit": limit},
        )
        return [
            EntityRecord(entity_id=row["entity_id"], value=row["value"], classification=row["classification"])
            for row in results
            if row.get("entity_id") is not None and row.get("value") is not None
        ]

    def get_facts_for_entities(self, entity_ids: list[str]) -> list[FactRecord]:
        """Return the facts asserted about the given entities, batching the lookup.

        Args:
            entity_ids: elementId strings of the subject entities to fetch
                facts for (as returned by :meth:`get_entities_by_class`).

        Returns:
            The :class:`FactRecord` list across all batches; empty if no ids given.
        """
        if not entity_ids:
            return []
        all_facts: list[FactRecord] = []
        for i in range(0, len(entity_ids), _BATCH_SIZE):
            chunk = entity_ids[i : i + _BATCH_SIZE]
            all_facts.extend(self._get_facts_batch(chunk))
        return all_facts

    def _get_facts_batch(self, entity_ids: list[str]) -> list[FactRecord]:
        """Return facts for a single batch of entity IDs.

        The graph model matches the Neptune implementations:
        ``(subject:__Entity__)-[:__SUBJECT__]->(f:__Fact__)`` with an
        optional ``(object:__Entity__)-[:__OBJECT__]->(f)`` edge. The
        fact's predicate is parsed out of the ``value`` string with the
        shared graphrag-toolkit helpers.

        Args:
            entity_ids: Batch of elementId strings (at most _BATCH_SIZE).

        Returns:
            List of FactRecord instances for this batch.
        """
        results = self._cypher(
            f"MATCH (subj:{self._label('Entity')})-[:`__SUBJECT__`]->(f:{self._label('Fact')}) "
            "WHERE elementId(subj) IN $entity_ids "
            f"OPTIONAL MATCH (obj:{self._label('Entity')})-[:`__OBJECT__`]->(f) "
            "RETURN subj.value AS subject_value, subj.class AS subject_class, "
            "f.value AS fact_value, obj.value AS object_value, obj.class AS object_class",
            params={"entity_ids": entity_ids},
        )
        facts: list[FactRecord] = []
        for row in results:
            fact_value = row.get("fact_value")
            subj_value = row.get("subject_value")
            if not fact_value or not subj_value:
                continue

            has_object = row.get("object_value") is not None

            if has_object:
                predicate = _extract_spo_predicate(
                    fact_value=fact_value,
                    subject_value=subj_value,
                    object_value=row["object_value"],
                )
                complement = None
            else:
                predicate, complement = _extract_spc_predicate_complement(
                    fact_value=fact_value,
                    subject_value=subj_value,
                )

            if not predicate:
                continue

            facts.append(
                FactRecord(
                    subject_value=subj_value,
                    subject_class=row.get("subject_class", ""),
                    predicate=predicate,
                    object_value=row.get("object_value") if has_object else None,
                    object_class=row.get("object_class") if has_object else None,
                    complement=complement,
                )
            )
        return facts

    def get_topics(self) -> list[TopicRecord]:
        """Return all topic nodes with the count of statements belonging to each.

        Returns:
            One :class:`TopicRecord` per ``Topic`` node in the graph.
        """
        results = self._cypher(
            f"MATCH (t:{self._label('Topic')}) "
            f"OPTIONAL MATCH (s:{self._label('Statement')})-[:`__BELONGS_TO__`]->(t) "
            "WITH t, count(s) AS stmt_count "
            "RETURN elementId(t) AS topic_id, t.value AS value, stmt_count AS statement_count"
        )
        topics: list[TopicRecord] = []
        for row in results:
            stmt_count = row.get("statement_count")
            topics.append(
                TopicRecord(
                    topic_id=row["topic_id"],
                    value=row["value"],
                    statement_count=int(stmt_count) if stmt_count is not None else 0,
                )
            )
        return [t for t in topics if t.topic_id is not None and t.value is not None]

    def get_graph_schema(self) -> ClassSchemaResult:
        """Return the graph schema: its class nodes and their relations.

        Returns:
            A :class:`ClassSchemaResult` combining class nodes and relations.

        Raises:
            ValueError: If the graph contains no class entries.
        """
        classes = self.get_class_nodes()
        if not classes:
            raise ValueError("Graph contains zero class entries — not a valid lexical graph")
        relations = self.get_class_relations()
        return ClassSchemaResult(classes=classes, relations=relations)

    def health_check(self) -> dict:
        """Probe the Neo4j connection with a trivial query.

        Returns:
            A status dict reporting healthy/unhealthy plus the backend name and
            any error message.
        """
        try:
            self._cypher("RETURN 1 AS ping")
            return {"status": "healthy", "backend": "neo4j"}
        except Exception as e:
            return {"status": "unhealthy", "backend": "neo4j", "error": str(e)}