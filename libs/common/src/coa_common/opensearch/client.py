# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``AossVectorClient`` — the shared AOSS vector client.

See the package docstring (``coa_common.opensearch``) for the
engine/filtering contract and the AOSS quirks this client encapsulates.

The client is transport-pluggable:

- **direct** (default): a SigV4-signed opensearch-py client, live credentials,
  full data plane (create / bulk / search / count / delete). Used by
  ontology-engine and metric-service.
- **proxy**: a caller-supplied ``transport`` callable that executes a *search*
  body elsewhere (e.g. the serve AgentCore search-proxy Lambda). Search/count
  only; write/index-admin ops raise. Used by the serve retrieval tiers.

Filtering is always server-side: :meth:`knn_search` and :meth:`count` build a
``bool``/``filter`` from the given ``term``/``exists`` clauses and push it into
the k-NN query (or a plain count query).
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Any

import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import RequestError

from coa_common.opensearch.retry import bulk_with_retry, oss_retry

log = logging.getLogger(__name__)

_AOSS_SERVICE = "aoss"  # OpenSearch Serverless — NOT "es"

# ``delete_by_term`` re-verifies to zero against AOSS's eventually-consistent
# search (search lags the index, so an empty *search* page is NOT proof the
# deletes are complete). ``_DELETE_VERIFY_POLL_S`` is the gap between
# delete-then-recount passes; ``_DELETE_VERIFY_STALL_TIMEOUT_S`` is a
# *no-progress* deadline (resets whenever the loop deletes ≥1 more doc), NOT a
# wall-clock cap — a large term-set whose docs keep disappearing takes as long
# as it needs, but a genuinely wedged index bails out. Mirrors ingest.py's
# embedding-readiness wait semantics.
_DELETE_VERIFY_POLL_S = float(os.getenv("OSS_DELETE_VERIFY_POLL_S", "2"))
_DELETE_VERIFY_STALL_TIMEOUT_S = float(os.getenv("OSS_DELETE_VERIFY_STALL_TIMEOUT_S", "60"))


def _is_index_not_found(exc: Exception) -> bool:
    """True if ``exc`` indicates the index doesn't exist.

    For a *deletion* verify loop a missing index means the matching docs are
    definitively gone. Matches on class name / message so it works regardless of
    which opensearch-py exception subclass surfaces the 404.
    """
    name = type(exc).__name__
    msg = str(exc).lower()
    return (
        name in ("NotFoundError", "IndexNotFoundError")
        or "index_not_found" in msg
        or "no such index" in msg
        or "resource_not_found" in msg
    )


# Fields the shared ontology/metric vector index stores. keyword → exact
# match/exists filtering server-side; text → not filterable (prompt/display only).
_KEYWORD_FIELDS = (
    "entity_uri",
    "ontology_id",
    "entity_type",
    "embedding_type",
    "model_id",
    "namespace",
    # R2RML data-source provenance — written only for structured (Tier-2-capable)
    # classes. Explicit keyword so serve NL→SQL can gate on it server-side
    # (exists/term inside the k-NN filter) reliably on new indexes. Older indexes
    # that predate this only have it dynamically mapped, where ``exists`` still works.
    "data_source_id",
)
_TEXT_FIELDS = (
    # Raw text fed to the embedder — kept verbatim for re-examination / re-embed.
    "text",
    # Richer schema context for the serve NL→SQL prompt (e.g. column allowed
    # values). NOT embedded — stored for prompt use only.
    "context_text",
)


def build_index_mapping(dims: int) -> dict:
    """Canonical vector-index mapping — the single source of truth.

    ``embedding`` is a **method-less** ``knn_vector`` on AOSS: NEXTGEN
    auto-resolves to Faiss/HNSW (an explicit ``method.engine`` is rejected
    there). On a self-managed OpenSearch cluster (local Docker stack) the
    method-less default resolves to **NMSLIB**, which does not support
    filtered k-NN queries; set ``OSS_KNN_ENGINE=faiss`` to pin
    ``method.engine`` explicitly and keep server-side ``knn.embedding.filter``
    working.
    All filterable fields are ``keyword`` so server-side ``term``/``exists``
    filters match exactly.
    """
    props: dict[str, Any] = {f: {"type": "keyword"} for f in _KEYWORD_FIELDS}
    props.update({f: {"type": "text"} for f in _TEXT_FIELDS})
    embedding: dict[str, Any] = {"type": "knn_vector", "dimension": dims}
    engine = os.getenv("OSS_KNN_ENGINE", "").strip()
    if engine:
        # Explicit engine for self-managed clusters; AOSS forbids this key.
        # Faiss only supports l2 (nmslib only cosinesimil) — pick a valid pair.
        space = "l2" if engine == "faiss" else "cosinesimil"
        embedding["method"] = {"name": "hnsw", "engine": engine, "space_type": space}
    props["embedding"] = embedding
    return {"settings": {"index": {"knn": True}}, "mappings": {"properties": props}}


# Transport for the proxy path: given (action, index, body) returns the raw
# opensearch response dict. Only "search" is exercised through this today.
Transport = Callable[[str, str, dict], dict]


class _RetryingIndices:
    """Proxy over ``client.indices`` that retries transient AOSS faults."""

    def __init__(self, indices: Any):
        self._indices = indices

    def __getattr__(self, name: str) -> Callable[..., Any]:
        target = getattr(self._indices, name)

        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return oss_retry(f"indices.{name}", lambda: target(*args, **kwargs))

        return _wrapped


class _RetryingClient:
    """Retry-wrapping proxy over an ``OpenSearch`` client for transient AOSS faults.

    Retries transient AOSS faults on
    every operation (index / search / delete / indices.*), so a single 429/5xx
    blip doesn't fail the whole read or write. Non-transient errors (4xx,
    RequestError) propagate immediately. The underlying client is exposed as
    ``.raw`` for helpers (e.g. ``opensearchpy.helpers.bulk``) that need it.
    """

    def __init__(self, client: OpenSearch):
        self.raw = client
        self.indices = _RetryingIndices(client.indices)

    def __getattr__(self, name: str) -> Callable[..., Any]:
        target = getattr(self.raw, name)
        if not callable(target):
            return target

        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return oss_retry(name, lambda: target(*args, **kwargs))

        return _wrapped


def _build_signed_client(endpoint: str, region: str, timeout: int) -> OpenSearch:
    host = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise RuntimeError("AWS credentials not available")
    # LIVE credentials object (not a frozen snapshot): AWSV4SignerAuth re-signs
    # each request with current creds, so a cached client survives Fargate
    # task-role token rotation. Service must be "aoss" (defaults to "es").
    auth = AWSV4SignerAuth(credentials, region, _AOSS_SERVICE)
    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=timeout,
        # Size to the WIDEST inducer ThreadPoolExecutor that shares this client so
        # it doesn't exhaust the default maxsize=10 pool ("Connection pool is full,
        # discarding connection"). Both induction passes hit the same client: the
        # first-pass grounding recall fans out INDUCER_GROUNDING_WORKERS-wide
        # (default 24) and the second-pass column match INDUCER_COLUMN_MATCH_WORKERS
        # (default 16). Sizing only to the column-match width starved the wider
        # grounding pass.
        pool_maxsize=max(
            int(os.getenv("INDUCER_COLUMN_MATCH_WORKERS", "16")),
            int(os.getenv("INDUCER_GROUNDING_WORKERS", "24")),
            10,
        ),
    )


def _build_unsigned_client(endpoint: str, timeout: int) -> OpenSearch:
    """Plain-HTTP opensearch-py client for a local (Docker) OpenSearch node.

    Selected when ``OPENSEARCH_AUTH=none``. The endpoint must carry its own
    scheme (``http://opensearch:9200``); security is disabled on that node, so
    no credentials are attached. Pool sizing matches the signed client so
    inducer fan-out doesn't starve the pool.
    """
    host = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    use_ssl = endpoint.startswith("https://")
    port = 443 if use_ssl else 9200
    if ":" in host:
        host, _, port_s = host.rpartition(":")
        port = int(port_s)
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        use_ssl=use_ssl,
        verify_certs=False,
        connection_class=RequestsHttpConnection,
        timeout=timeout,
        pool_maxsize=max(
            int(os.getenv("INDUCER_COLUMN_MATCH_WORKERS", "16")),
            int(os.getenv("INDUCER_GROUNDING_WORKERS", "24")),
            10,
        ),
    )


def _filter_clause(filters: list[dict] | None) -> dict | None:
    """Wrap a list of leaf filter clauses in a ``bool``/``filter``, or None."""
    return {"bool": {"filter": filters}} if filters else None


class AossVectorClient:
    """Shared AOSS vector client. See the module docstring.

    Args:
        endpoint: AOSS collection endpoint (``https://…aoss.<region>.on.aws``).
            Required for the direct transport; ignored for the proxy transport.
        region: AWS region for SigV4 signing (direct transport).
        dimensions: knn_vector dimension for ``build_index_mapping``.
        timeout: opensearch-py request timeout (seconds), direct transport.
        transport: optional search-only proxy callable. When set, the client
            never builds a signed opensearch-py client and write/admin ops raise.
    """

    def __init__(
        self,
        *,
        endpoint: str = "",
        region: str = "us-east-1",
        dimensions: int = 1024,
        timeout: int = 60,
        transport: Transport | None = None,
    ):
        """Configure the AOSS client for either direct or proxy transport (see class Args)."""
        self.endpoint = endpoint
        self.region = region
        self.dimensions = dimensions
        self.timeout = timeout
        self._transport = transport
        self._client: _RetryingClient | None = None
        self._ensured_indices: set[str] = set()

    # ── transport ──────────────────────────────────────────────────────

    @property
    def is_proxy(self) -> bool:
        """True when configured for search-only proxy transport (no direct client)."""
        return self._transport is not None

    def _c(self) -> _RetryingClient:
        """Retry-wrapped direct client. Raises if this client is proxy-only."""
        if self._transport is not None:
            raise RuntimeError("AossVectorClient is proxy-only; direct client operations are unavailable")
        if self._client is None:
            if not self.endpoint:
                raise RuntimeError("AOSS endpoint not set — configure the collection endpoint")
            # OPENSEARCH_AUTH=none selects the unsigned local client (Docker
            # stack: vanilla OpenSearch with security disabled). Default keeps
            # the production SigV4 AOSS path unchanged.
            if os.getenv("OPENSEARCH_AUTH", "sigv4").lower() == "none":
                self._client = _RetryingClient(_build_unsigned_client(self.endpoint, self.timeout))
            else:
                self._client = _RetryingClient(_build_signed_client(self.endpoint, self.region, self.timeout))
        return self._client

    def raw_client(self):
        """Return the retry-wrapped opensearch-py client for low-level/debug access.

        Intended for ad-hoc ``.get`` / ``.count`` in scripts. Raises for proxy-only
        clients. Prefer the typed methods (knn_search / count / …) in app code.
        """
        return self._c()

    def _search(self, index: str, body: dict) -> dict:
        """Execute a search body via the proxy transport or the direct client.

        A missing index reads as "no data" rather than an error: every read
        helper funnels through here, so this one guard lets callers query an
        index that was never created (or was torn down with its namespace)
        without first calling :meth:`ensure_index`. That matters because an
        ``ensure_index`` on the READ path resurrects an empty index for a
        deleted namespace, and nothing ever reclaims it — AOSS caps a
        collection at 1000 indexes, so leaked shells eventually break every
        write with ``index_limit_breached``.
        """
        try:
            if self._transport is not None:
                return self._transport("search", index, body)
            return self._c().search(index=index, body=body)
        except Exception as e:
            if _is_index_not_found(e):
                log.debug("search on missing index %s — returning empty result", index)
                return {"hits": {"hits": [], "total": {"value": 0}}}
            raise

    # ── index admin (direct only) ──────────────────────────────────────

    def ensure_index(self, index: str) -> str:
        """Create ``index`` with the canonical mapping if it doesn't exist."""
        if index in self._ensured_indices:
            return index
        c = self._c()
        if not c.indices.exists(index=index):
            try:
                c.indices.create(index=index, body=build_index_mapping(self.dimensions))
                log.info("created OpenSearch index %s (dims=%d)", index, self.dimensions)
            except RequestError as e:
                # Race: another caller created it between exists() and create().
                if e.error == "resource_already_exists_exception":
                    log.debug("index %s already exists (concurrent create)", index)
                else:
                    raise
        self._ensured_indices.add(index)
        return index

    def delete_index(self, index: str) -> bool:
        """Delete ``index``. Returns True if it existed."""
        c = self._c()
        if not c.indices.exists(index=index):
            return False
        c.indices.delete(index=index)
        self._ensured_indices.discard(index)
        log.info("deleted OpenSearch index %s", index)
        return True

    # ── writes (direct only) ────────────────────────────────────────────

    def index_document(self, index: str, doc: dict) -> None:
        """Append a single document (AOSS auto-assigns ``_id``)."""
        self._c().index(index=index, body=doc)

    def bulk_index(self, index: str, docs: list[dict]) -> None:
        """Bulk-append documents with per-item transient-fault retry."""
        if not docs:
            return
        actions = [{"_op_type": "index", "_index": index, **d} for d in docs]
        bulk_with_retry(self._c().raw, actions)

    def delete_by_term(self, index: str, field: str, value: str, page: int = 500) -> int:
        """Delete every doc where ``field == value``, verifying the term reaches zero.

        AOSS has no ``_delete_by_query`` on VECTORSEARCH, so we search for
        matching docs and delete each by its auto-assigned ``_id``. AOSS
        ``search`` is also NOT read-your-writes consistent (near-real-time
        refresh lag), so a naive "delete visible hits until a search returns
        empty" loop can stop early: an intermediate search returns 0 hits from a
        stale index view and breaks the loop, leaving a tail of docs that were
        never issued a delete. (Observed live: property embeddings orphaned after
        an ontology delete.)

        To be correct against that lag, we loop delete-then-recount until the
        term-query ``count`` is a *stable* zero, tracking a no-progress stall
        deadline (resets whenever we delete ≥1 more doc, mirrors ingest.py's
        readiness wait). On a genuine stall we log and stop rather than spin
        forever — the caller verifies teardown and retries. Returns the number of
        docs deleted.
        """
        c = self._c()
        term = [{"term": {field: value}}]
        deleted = 0
        stall_deadline = time.perf_counter() + _DELETE_VERIFY_STALL_TIMEOUT_S

        while True:
            # 1. Fetch a page of matching doc ids and delete them. Goes through
            #    _search so a missing index reads as zero hits (nothing to
            #    delete) instead of raising — callers no longer ensure_index on
            #    the delete path, since doing so resurrected empty shells.
            resp = self._search(
                index,
                {"size": page, "_source": False, "query": {"term": {field: value}}},
            )
            hits = _hits(resp)
            deleted_this_pass = 0
            for h in hits:
                try:
                    c.delete(index=index, id=h["_id"])
                    deleted += 1
                    deleted_this_pass += 1
                except Exception:  # noqa: BLE001 — best-effort per-doc; recount is the gate
                    pass

            # 2. Terminal check via count (does NOT rely on this page being
            #    empty — that empty could be a stale view). Only a real zero
            #    count means the term matches nothing left. A missing index →
            #    done (0); any other (transient) count error → -1 so the stall
            #    timer governs whether we keep retrying.
            try:
                remaining = self.count(index, filters=term)
            except Exception as e:  # noqa: BLE001
                remaining = 0 if _is_index_not_found(e) else -1

            if remaining == 0:
                return deleted

            # 3. Progress / stall bookkeeping. "Progress" = we actually deleted
            #    at least one doc THIS pass (count alone can lag). Real progress
            #    resets the no-progress deadline; a pass that deletes nothing and
            #    still sees count > 0 counts against the deadline, so a persistent
            #    search-empty-but-count-nonzero disagreement can't spin forever.
            if deleted_this_pass > 0:
                stall_deadline = time.perf_counter() + _DELETE_VERIFY_STALL_TIMEOUT_S
            elif time.perf_counter() >= stall_deadline:
                log.warning(
                    "delete_by_term(%s=%s) stalled — deleted %d, ~%s still matching after %.0fs "
                    "no-progress; giving up this pass (caller will retry / verify)",
                    field,
                    value,
                    deleted,
                    remaining if remaining >= 0 else "unknown",
                    _DELETE_VERIFY_STALL_TIMEOUT_S,
                )
                return deleted

            time.sleep(_DELETE_VERIFY_POLL_S)

    # ── reads (direct or proxy) ─────────────────────────────────────────

    def knn_search(
        self,
        index: str,
        vector: list[float],
        *,
        top_k: int = 10,
        filters: list[dict] | None = None,
        source: list[str] | bool | None = None,
    ) -> list[dict]:
        """Server-side filtered k-NN. Returns raw hit dicts (``_source`` + ``_score``).

        ``filters`` is a list of leaf clauses (``{"term": {...}}`` /
        ``{"exists": {...}}``) ANDed inside the k-NN ``filter`` — the engine
        returns the true top-``top_k`` within the filtered subset, no over-fetch.
        """
        if len(vector) < self.dimensions:
            vector = list(vector) + [0.0] * (self.dimensions - len(vector))
        knn: dict[str, Any] = {"vector": vector, "k": top_k}
        clause = _filter_clause(filters)
        if clause is not None:
            knn["filter"] = clause
        body: dict[str, Any] = {"size": top_k, "query": {"knn": {"embedding": knn}}}
        if source is not None:
            body["_source"] = source
        resp = self._search(index, body)
        return [{**h.get("_source", {}), "_score": h.get("_score"), "_id": h.get("_id")} for h in _hits(resp)]

    def filter_search(
        self,
        index: str,
        filters: list[dict],
        *,
        size: int = 1000,
        source: list[str] | bool | None = None,
    ) -> list[dict]:
        """Non-vector bool/filter search. Returns raw ``_source`` dicts."""
        body: dict[str, Any] = {"size": size, "query": _filter_clause(filters) or {"match_all": {}}}
        if source is not None:
            body["_source"] = source
        resp = self._search(index, body)
        return [h.get("_source", {}) for h in _hits(resp)]

    def search_raw(self, index: str, body: dict) -> dict:
        """Execute a raw search ``body``; return the raw opensearch response.

        Escape hatch for query shapes the typed helpers don't cover (e.g. a
        hybrid BM25 + k-NN ``bool.should``). Goes through the same transport
        (direct or proxy) as every other search.
        """
        return self._search(index, body)

    def count(self, index: str, *, filters: list[dict] | None = None) -> int:
        """Count docs matching optional filters (a ``size:0`` search)."""
        query = _filter_clause(filters) or {"match_all": {}}
        body = {"size": 0, "query": query, "track_total_hits": True}
        resp = self._search(index, body)
        return resp.get("hits", {}).get("total", {}).get("value", 0)

    def iter_source_field(self, index: str, filters: list[dict], field: str, page: int = 10_000) -> list[Any]:
        """Project one ``field`` across all matching docs.

        Pages with ``search_after`` (AOSS has no scroll API). Sorts on ``field`` (keyword).
        Goes through :meth:`_search`, so a missing index yields ``[]``.
        """
        query = _filter_clause(filters) or {"match_all": {}}
        values: list[Any] = []
        search_after: list[Any] | None = None
        while True:
            body: dict[str, Any] = {
                "size": page,
                "_source": [field],
                "query": query,
                "sort": [{field: "asc"}],
            }
            if search_after is not None:
                body["search_after"] = search_after
            resp = self._search(index, body)
            hits = _hits(resp)
            if not hits:
                break
            for h in hits:
                v = h["_source"].get(field)
                if v is not None:
                    values.append(v)
            if len(hits) < page:
                break
            search_after = hits[-1].get("sort")
            if not search_after:
                break
        return values

    # ── health ──────────────────────────────────────────────────────────

    def health_check(self, index: str) -> dict:
        """Probe liveness by ensuring the index exists.

        AOSS 404s on ``/`` so ``client.info()`` is unusable;
        ensuring the index exists is the cheapest round-trip that works.
        """
        try:
            if self._transport is not None:
                self._transport("health_check", index, {})
            else:
                self.ensure_index(index)
            return {"status": "ok", "index": index}
        except Exception as e:  # noqa: BLE001 — health probe must not raise
            return {"status": "error", "error": str(e)}


def _hits(resp: dict) -> list[dict]:
    return resp.get("hits", {}).get("hits", [])
