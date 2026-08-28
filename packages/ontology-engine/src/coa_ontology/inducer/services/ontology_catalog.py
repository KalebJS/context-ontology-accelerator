# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Client for the ontology-catalog service."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from urllib.parse import quote

import httpx
from coa_common.opensearch import retry_call

log = logging.getLogger(__name__)

# Reuse oss_retry's retry_call loop (libs/common) for the legacy HTTP catalog
# client so a transient blip on recall doesn't fail an induction — only the
# transient predicate + limits differ from oss_retry, the loop is shared. This
# client is NOT the deployed hot path (StoreOntologyCatalogAdapter over oss_retry
# is) — kept symmetric so both catalog impls behave alike. Retry 429 + retryable
# 5xx and connect/timeout faults; re-raise on exhaustion. Non-retryable 4xx
# (400/403/404/409) and other statuses propagate immediately.
_HTTP_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_HTTP_MAX_RETRIES = int(os.getenv("CATALOG_HTTP_MAX_RETRIES", "6"))
_HTTP_MAX_BACKOFF_S = float(os.getenv("CATALOG_HTTP_MAX_BACKOFF_S", "30"))


def _http_transient(exc: Exception) -> bool:
    if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException)):
        return True  # network blip / timeout — retry
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _HTTP_RETRY_STATUS
    return False


def _retry[T](op: str, fn: Callable[[], T]) -> T:
    """Retry the legacy HTTP catalog client on transient httpx faults.

    Reuses the shared retry_call loop (same semantics as oss_retry; only the
    transient predicate + limits differ). Not the deployed hot path
    (StoreOntologyCatalogAdapter over oss_retry is) — kept symmetric so both
    catalog impls behave alike.
    """
    return retry_call(
        op,
        fn,
        is_transient=_http_transient,
        max_retries=_HTTP_MAX_RETRIES,
        max_backoff_s=_HTTP_MAX_BACKOFF_S,
    )


class OntologyCatalogClient:
    """HTTP client for the ontology-catalog service (ontologies and embeddings)."""

    def __init__(self, base_url: str):
        """Store the base URL of the ontology-catalog service.

        Args:
            base_url: Root URL of the ontology-catalog HTTP API.
        """
        self.base_url = base_url

    def _ont_url(self, ontology_id: str) -> str:
        return f"{self.base_url}/ontologies/{quote(ontology_id, safe='')}"

    def search_embeddings(
        self,
        vector: list[float],
        embedding_type: str,
        model_id: str,
        entity_type: str = "class",
        ontology_id: str | None = None,
        top_k: int = 5,
        namespace: str | None = None,
    ) -> list[dict]:
        """Search the catalog for entities nearest to an embedding vector.

        Args:
            vector: Query embedding to search against.
            embedding_type: Embedding family (e.g. "lexical") to search within.
            model_id: Embedding model id, used to select the matching index.
            entity_type: Entity kind to search ("class" or "property").
            ontology_id: Optional ontology to scope the search to.
            top_k: Number of nearest matches to return.
            namespace: Namespace whose per-namespace index to query; None uses default.

        Returns:
            The list of matching entity records with their scores.
        """
        payload = {
            "vector": vector,
            "embedding_type": embedding_type,
            "model_id": model_id,
            "entity_type": entity_type,
            "top_k": top_k,
        }
        if ontology_id is not None:
            payload["ontology_id"] = ontology_id
        # /embeddings/search scopes to a per-namespace OpenSearch index
        # ({OSS_INDEX}-{namespace}; bare when namespace==default). Omitting
        # ?namespace= makes the endpoint default to "default" and query the
        # bare index — empty on multi-namespace deployments. Thread it when
        # provided so recall hits the correct index. (Signature also matches
        # StoreOntologyCatalogAdapter.search_embeddings so GroundingService
        # can call either implementation uniformly.)
        params = {"namespace": namespace} if namespace is not None else None

        def _do() -> list[dict]:
            with httpx.Client(timeout=60) as client:
                resp = client.post(f"{self.base_url}/embeddings/search", json=payload, params=params)
                resp.raise_for_status()
                return resp.json()

        return _retry("search_embeddings", _do)

    def get_embeddings_for_entity(
        self,
        entity_uri: str,
        embedding_type: str | None = None,
        namespace: str | None = None,
    ) -> list[dict]:
        """Fetch stored embeddings for a specific entity URI.

        Args:
            entity_uri: URI of the entity whose embeddings to retrieve.
            embedding_type: Optional embedding family filter.
            namespace: Optional namespace whose index to query.

        Returns:
            The list of embedding records for the entity.
        """
        params: dict[str, str] = {}
        if embedding_type:
            params["embedding_type"] = embedding_type
        if namespace is not None:
            params["namespace"] = namespace
        with httpx.Client(timeout=10) as client:
            resp = client.get(f"{self.base_url}/embeddings/by-entity/{entity_uri}", params=params)
            resp.raise_for_status()
            return resp.json()

    def create_ontology(self, ontology_data: dict) -> dict:
        """Create an ontology record, returning the existing one on URI conflict.

        Args:
            ontology_data: Ontology metadata payload; must include ``uri``.

        Returns:
            The created ontology record, or the existing record if one already
            exists with the same URI.
        """
        with httpx.Client(timeout=30) as client:
            resp = client.post(f"{self.base_url}/ontologies/", json=ontology_data)
            if resp.status_code == 409:
                return self.get_ontology_by_uri(ontology_data["uri"])
            resp.raise_for_status()
            return resp.json()

    def get_ontology_by_uri(self, uri: str) -> dict:
        """Look up a single ontology record by its URI.

        Args:
            uri: The ontology URI to search for.

        Returns:
            The matching ontology record.

        Raises:
            ValueError: If no ontology with the given URI exists.
        """
        with httpx.Client(timeout=30) as client:
            resp = client.get(f"{self.base_url}/ontologies/", params={"uri": uri})
            resp.raise_for_status()
            payload = resp.json()
            # ListOntologies answers with a {"ontologies": [...]} envelope; a bare
            # array is still accepted so an older deployment keeps working. Note a
            # non-empty dict is truthy, so the envelope must be unwrapped BEFORE
            # the emptiness check or ``results[0]`` raises KeyError instead.
            results = payload.get("ontologies") or [] if isinstance(payload, dict) else payload
            if not results:
                raise ValueError(f"Ontology with URI '{uri}' not found")
            return results[0]

    def upload_ontology_file(self, ontology_id: str, content: bytes, filename: str = "ontology.ttl") -> dict:
        """Upload the serialized ontology file for an existing ontology record.

        Args:
            ontology_id: Id of the ontology to attach the file to.
            content: Raw file bytes (Turtle by default).
            filename: Name to record for the uploaded file.

        Returns:
            The updated ontology record returned by the service.
        """
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                f"{self._ont_url(ontology_id)}/upload",
                files={"file": (filename, content, "text/turtle")},
            )
            resp.raise_for_status()
            return resp.json()

    def store_embeddings_batch(self, embeddings: list[dict]) -> list[dict]:
        """Persist a batch of entity embeddings to the catalog.

        Args:
            embeddings: Embedding records to store.

        Returns:
            The stored embedding records returned by the service.
        """
        with httpx.Client(timeout=120) as client:
            resp = client.post(
                f"{self.base_url}/embeddings/batch",
                json={"embeddings": embeddings},
            )
            resp.raise_for_status()
            return resp.json()
