# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Neo4j lexical store and the GRAPH_STORE_URI routing switch.

The store tests mock the neo4j driver (session.run) the same way the NA/ND
lexical store tests mock their boto3 clients — no live Neo4j needed. The
routing tests patch both concrete store classes in
``induce_unstructured._build_lexical_store`` (same convention as
``test_ssrf_prevention_induce_unstructured.py``) and control
``GRAPH_STORE_URI`` via monkeypatch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from neo4j.exceptions import Neo4jError, TransientError

pytestmark = pytest.mark.unit

from coa_ontology.inducer.routers.induce_unstructured import _build_lexical_store
from coa_ontology.inducer.unstructured.stores.neo4j_lexical import Neo4jLexicalStore
from coa_ontology.inducer.unstructured.stores.protocol import (
    ClassRecord,
    ClassSchemaResult,
    EntityRecord,
    FactRecord,
    LexicalGraphStore,
    RelationRecord,
    TopicRecord,
)

_TENANT = "1e57a8ef3189454989f44481e"
_URI = "bolt://neo4j:coa-graph-pw@neo4j:7687"


def _make_store(mock_driver: MagicMock, tenant_id: str = _TENANT) -> Neo4jLexicalStore:
    """Create a store instance wired to a mocked neo4j driver."""
    with patch("coa_ontology.inducer.unstructured.stores.neo4j_lexical.GraphDatabase") as gd:
        gd.driver.return_value = mock_driver
        store = Neo4jLexicalStore(uri=_URI, tenant_id=tenant_id)
    return store


def _make_driver(rows: list[dict]) -> MagicMock:
    """Mock driver whose sessions yield the given records."""
    driver = MagicMock()
    session = MagicMock()
    session.run.return_value = [MagicMock(data=lambda r=row: r) for row in rows]
    driver.session.return_value.__enter__.return_value = session
    return driver


class TestNeo4jLexicalStoreInit:
    """Constructor: URI parsing, scheme guard, label scoping."""

    def test_rejects_non_bolt_scheme(self):
        """A https:// URI must be refused — the Neptune URL must never reach the driver."""
        with pytest.raises(ValueError, match="bolt"):
            Neo4jLexicalStore(uri="https://fuseki:8182", tenant_id=_TENANT)

    def test_accepts_bolt_plus_s_scheme(self):
        """TLS bolt schemes are accepted for TLS-terminated Neo4j."""
        with patch("coa_ontology.inducer.unstructured.stores.neo4j_lexical.GraphDatabase") as gd:
            Neo4jLexicalStore(uri="bolt+s://neo4j.example:7687", tenant_id=_TENANT)
            gd.driver.assert_called_once()
            assert gd.driver.call_args.args[0] == "bolt+s://neo4j.example:7687"

    def test_accepts_bolt_uri_and_parses_credentials(self):
        """A bolt:// URI with embedded credentials constructs the driver.

        The driver refuses credentials IN the URI (ConfigurationError), so the
        constructor must strip them from the netloc and pass them via auth= —
        the connection URI stays credential-free."""
        with patch("coa_ontology.inducer.unstructured.stores.neo4j_lexical.GraphDatabase") as gd:
            store = Neo4jLexicalStore(uri=_URI, tenant_id=_TENANT)
            gd.driver.assert_called_once()
            args, kwargs = gd.driver.call_args
            # Credentials stripped from the connection URI...
            assert args[0] == "bolt://neo4j:7687"
            # ...and passed as the auth tuple instead.
            assert kwargs["auth"] == ("neo4j", "coa-graph-pw")
        assert store._driver is not None

    def test_no_credentials_connects_unauthenticated(self):
        """A credential-less bolt URI passes auth=None (driver default auth absent)."""
        with patch("coa_ontology.inducer.unstructured.stores.neo4j_lexical.GraphDatabase") as gd:
            Neo4jLexicalStore(uri="bolt://neo4j:7687", tenant_id="")
            gd.driver.assert_called_once()
            assert gd.driver.call_args.kwargs["auth"] is None

    def test_satisfies_protocol(self):
        """Neo4jLexicalStore satisfies the LexicalGraphStore runtime protocol."""
        store = _make_store(_make_driver([]))
        assert isinstance(store, LexicalGraphStore)

    def test_label_scoping_with_tenant(self):
        """Labels get the tenant suffix and backticks: __Entity__<tenant>__."""
        store = _make_store(_make_driver([]), tenant_id=_TENANT)
        assert store._label("Entity") == f"`__Entity__{_TENANT}__`"
        assert store._label("SYS_Class") == f"`__SYS_Class__{_TENANT}__`"

    def test_label_scoping_without_tenant(self):
        """No tenant → bare backticked base label."""
        store = _make_store(_make_driver([]), tenant_id="")
        assert store._label("Entity") == "`__Entity__`"


class TestGetClassNodes:
    def test_returns_class_records(self):
        driver = _make_driver(
            [
                {"value": "Product", "count": 54},
                {"value": "Organization", "count": 32},
            ]
        )
        store = _make_store(driver)
        result = store.get_class_nodes()

        assert result == [ClassRecord(value="Product", count=54), ClassRecord(value="Organization", count=32)]
        query = driver.session.return_value.__enter__.return_value.run.call_args[0][0]
        # Tenant-scoped, backticked label reaches the server.
        assert f"`__SYS_Class__{_TENANT}__`" in query

    def test_null_count_defaults_to_zero(self):
        driver = _make_driver([{"value": "Product", "count": None}])
        store = _make_store(driver)
        assert store.get_class_nodes() == [ClassRecord(value="Product", count=0)]

    def test_empty_graph_returns_empty_list(self):
        store = _make_store(_make_driver([]))
        assert store.get_class_nodes() == []


class TestGetClassRelations:
    def test_returns_relation_records(self):
        driver = _make_driver(
            [
                {"source_class": "Personnel", "predicate": "FOLLOWS", "target_class": "Policy", "count": 2},
            ]
        )
        store = _make_store(driver)
        assert store.get_class_relations() == [
            RelationRecord(source_class="Personnel", predicate="FOLLOWS", target_class="Policy", count=2)
        ]

    def test_incomplete_rows_are_skipped(self):
        driver = _make_driver(
            [
                {"source_class": "A", "predicate": None, "target_class": "C", "count": 1},
            ]
        )
        store = _make_store(driver)
        assert store.get_class_relations() == []


class TestGetEntitiesByClass:
    def test_returns_entity_records_with_element_ids(self):
        driver = _make_driver(
            [
                {
                    "entity_id": "4:674c6b88-d9b2-488b-a0a1-c18df73bc99c:48",
                    "value": "Nimbus Analytics",
                    "classification": "Organization",
                },
            ]
        )
        store = _make_store(driver)
        result = store.get_entities_by_class(classification="Organization", limit=100)

        assert result == [
            EntityRecord(
                entity_id="4:674c6b88-d9b2-488b-a0a1-c18df73bc99c:48",
                value="Nimbus Analytics",
                classification="Organization",
            )
        ]
        session = driver.session.return_value.__enter__.return_value
        assert session.run.call_args[0][1] == {"classification": "Organization", "limit": 100}

    def test_zero_matches_returns_empty_list(self):
        store = _make_store(_make_driver([]))
        assert store.get_entities_by_class(classification="Nonexistent") == []


class TestGetFactsForEntities:
    def test_spo_fact_extracts_predicate(self):
        driver = _make_driver(
            [
                {
                    "subject_value": "Nimbus Analytics",
                    "subject_class": "Organization",
                    "fact_value": "Nimbus Analytics OFFERS small, focused portfolio",
                    "object_value": "small, focused portfolio",
                    "object_class": "Product",
                },
            ]
        )
        store = _make_store(driver)
        facts = store.get_facts_for_entities(["4:674c6b88:48"])
        assert facts == [
            FactRecord(
                subject_value="Nimbus Analytics",
                subject_class="Organization",
                predicate="OFFERS",
                object_value="small, focused portfolio",
                object_class="Product",
                complement=None,
            )
        ]

    def test_spc_fact_extracts_predicate_and_complement(self):
        driver = _make_driver(
            [
                {
                    "subject_value": "Nimbus Analytics",
                    "subject_class": "Organization",
                    "fact_value": "Nimbus Analytics TYPE organization",
                    "object_value": None,
                    "object_class": None,
                },
            ]
        )
        store = _make_store(driver)
        facts = store.get_facts_for_entities(["4:674c6b88:48"])
        assert facts == [
            FactRecord(
                subject_value="Nimbus Analytics",
                subject_class="Organization",
                predicate="TYPE",
                object_value=None,
                object_class=None,
                complement="organization",
            )
        ]

    def test_empty_entity_ids_short_circuits(self):
        driver = MagicMock()
        store = _make_store(driver)
        assert store.get_facts_for_entities([]) == []
        driver.session.assert_not_called()

    def test_batching_chunks_queries(self):
        """> _BATCH_SIZE ids issue one query per chunk (protocol requirement)."""
        from coa_ontology.inducer.unstructured.stores.na_lexical import _BATCH_SIZE

        driver = MagicMock()
        session = MagicMock()
        session.run.return_value = []
        driver.session.return_value.__enter__.return_value = session

        store = _make_store(driver)
        with patch.object(store, "_get_facts_batch", wraps=store._get_facts_batch) as spy:
            ids = [f"id-{i}" for i in range(_BATCH_SIZE + 5)]
            store.get_facts_for_entities(ids)
        assert spy.call_count == 2
        assert len(spy.call_args_list[0][0][0]) == _BATCH_SIZE
        assert len(spy.call_args_list[1][0][0]) == 5


class TestGetTopics:
    def test_returns_topic_records(self):
        driver = _make_driver(
            [
                {"topic_id": "4:674c6b88:3", "value": "Returns Policy", "statement_count": 22},
            ]
        )
        store = _make_store(driver)
        assert store.get_topics() == [TopicRecord(topic_id="4:674c6b88:3", value="Returns Policy", statement_count=22)]

    def test_zero_statement_topic_kept_with_zero_count(self):
        driver = _make_driver(
            [
                {"topic_id": "t1", "value": "Lonely Topic", "statement_count": 0},
            ]
        )
        store = _make_store(driver)
        topics = store.get_topics()
        assert len(topics) == 1
        assert topics[0].statement_count == 0


class TestGetGraphSchema:
    def test_combines_classes_and_relations(self):
        driver = _make_driver(
            [
                {"value": "Product", "count": 54},
            ]
        )
        store = _make_store(driver)
        schema = store.get_graph_schema()
        assert isinstance(schema, ClassSchemaResult)
        assert schema.classes == [ClassRecord(value="Product", count=54)]
        assert schema.relations == []

    def test_zero_classes_raises_valueerror(self):
        store = _make_store(_make_driver([]))
        with pytest.raises(ValueError, match="zero class entries"):
            store.get_graph_schema()


class TestErrorHandling:
    def test_neo4j_error_wrapped_in_runtimeerror(self):
        driver = MagicMock()
        session = MagicMock()
        session.run.side_effect = Neo4jError("bad query")
        driver.session.return_value.__enter__.return_value = session
        store = _make_store(driver)
        with pytest.raises(RuntimeError, match="Neo4j query failed"):
            store.get_class_nodes()

    def test_transient_error_retries_then_succeeds(self):
        """A transient error on the first attempt is retried, not raised."""
        driver = MagicMock()
        session = MagicMock()
        good = [MagicMock(data=lambda: {"value": "Product", "count": 1})]
        session.run.side_effect = [TransientError("blip"), good]
        driver.session.return_value.__enter__.return_value = session
        store = _make_store(driver)
        result = store.get_class_nodes()
        assert result == [ClassRecord(value="Product", count=1)]
        assert session.run.call_count == 2

    def test_health_check_healthy(self):
        store = _make_store(_make_driver([{"ping": 1}]))
        assert store.health_check() == {"status": "healthy", "backend": "neo4j"}

    def test_health_check_unhealthy(self):
        driver = MagicMock()
        session = MagicMock()
        session.run.side_effect = Neo4jError("down")
        driver.session.return_value.__enter__.return_value = session
        store = _make_store(driver)
        result = store.health_check()
        assert result["status"] == "unhealthy"
        assert result["backend"] == "neo4j"


# ── Routing: _build_lexical_store honors GRAPH_STORE_URI ─────────────────


class TestBuildLexicalStoreRouting:
    """GRAPH_STORE_URI set → Neo4j store; unset → production Neptune paths."""

    def test_env_set_builds_neo4j_store(self, monkeypatch):
        monkeypatch.setenv("GRAPH_STORE_URI", _URI)
        app_config = {"neptune_endpoint": "fuseki"}
        store = _build_lexical_store("neptune-db", "us-east-1", app_config, namespace="ns-test")
        assert isinstance(store, Neo4jLexicalStore)
        assert store._tenant_id != ""  # tenant derived from namespace
        store.close()

    def test_env_unset_builds_neptune_db_store(self, monkeypatch):
        monkeypatch.delenv("GRAPH_STORE_URI", raising=False)
        app_config = {"neptune_endpoint": "https://real-cluster.us-east-1.neptune.amazonaws.com:8182"}
        with (
            patch(
                "coa_ontology.inducer.routers.induce_unstructured.NeptuneAnalyticsLexicalStore"
            ) as mock_na,
            patch("coa_ontology.inducer.routers.induce_unstructured.NeptuneDatabaseLexicalStore") as mock_ndb,
        ):
            _build_lexical_store("neptune-db", "us-east-1", app_config, namespace="ns-test")
            mock_ndb.assert_called_once()
            call_kwargs = mock_ndb.call_args.kwargs
            assert call_kwargs["endpoint"] == "real-cluster.us-east-1.neptune.amazonaws.com"
            assert call_kwargs["tenant_id"] != ""
            mock_na.assert_not_called()

    def test_env_set_overrides_na_arn_routing(self, monkeypatch):
        """When the env override is set it wins even for an explicit NA ARN —
        the local stack has no Neptune of either flavor."""
        monkeypatch.setenv("GRAPH_STORE_URI", _URI)
        arn = "arn:aws:neptune-graph:us-east-1:123456789012:graph/g-abc123"
        app_config = {"neptune_endpoint": "ignored"}
        store = _build_lexical_store(arn, "us-east-1", app_config, namespace="ns-test")
        assert isinstance(store, Neo4jLexicalStore)
        store.close()

    def test_env_unset_neptune_paths_untouched(self, monkeypatch):
        """Regression guard: without the env var, an NA ARN still routes to the
        NA store exactly as before."""
        monkeypatch.delenv("GRAPH_STORE_URI", raising=False)
        arn = "arn:aws:neptune-graph:us-west-2:123456789012:graph/g-test"
        app_config = {"neptune_endpoint": "cluster.us-west-2.neptune.amazonaws.com"}
        with (
            patch(
                "coa_ontology.inducer.routers.induce_unstructured.NeptuneAnalyticsLexicalStore"
            ) as mock_na,
            patch("coa_ontology.inducer.routers.induce_unstructured.NeptuneDatabaseLexicalStore") as mock_ndb,
        ):
            _build_lexical_store(arn, "us-west-2", app_config)
            mock_na.assert_called_once_with(graph_id="g-test", region="us-west-2")
            mock_ndb.assert_not_called()