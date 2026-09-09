# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Serve-side graphrag-toolkit patch: request ``embedding`` in ``_source``.

Why this exists
---------------
``TopicBeamSearch`` expands the beam by fetching each neighbour topic's *stored*
embedding by id (``topic_beam_search.py:148`` -> ``OpenSearchIndex.get_embeddings``
-> ``get_all_embeddings`` -> ``paginated_search``). The toolkit's
``_to_get_embedding_result`` then reads ``source['embedding']``
(``opensearch_vector_indexes.py:643``) with a bare subscript.

On AOSS **NEXTGEN** collections the ``knn_vector`` field is NOT returned in the
default ``_source`` projection, so that subscript raises ``KeyError: 'embedding'``.
The exception escapes ``get_embeddings`` and is swallowed one level up by
``TopicBeamSearch._get_embeddings``' broad ``except Exception`` (logged only as
``Failed to fetch stored topic embeddings``). Every neighbour is then treated as
"no stored embedding" and skipped, so the beam collapses to its seed set and the
retriever returns ~1 node regardless of how good the seeds are.

Measured on the live sec10q collection: a terms query for known topic ids matches
3/3 docs but ``'embedding' in hit['_source']`` is ``False``; adding an explicit
``_source`` include returns the full 1024-dim vector for 5681/5681 topic docs.

The fix: explicitly project the fields ``_to_get_embedding_result`` consumes. This
is a read-path-only patch (serve never writes vectors), and it leaves the
``ids_only`` branch untouched since that path deliberately suppresses ``_source``.

Reported upstream; remove this module once the toolkit projects the vector field
itself. Applied lazily (never at import) to preserve the botocore sync-only
invariant documented in ``strategies.py``.
"""

from __future__ import annotations

import time

import structlog

logger = structlog.get_logger(__name__)

# Fields ``_to_get_embedding_result`` reads: source['id'], source['value'],
# source['embedding'], source['metadata']['_node_content'].
_EMBEDDING_SOURCE_INCLUDES = ["id", "value", "embedding", "metadata"]

_patched = False
_tls_patched = False


def patch_local_opensearch_transport() -> None:
    """Point the toolkit's OpenSearch clients at a plain-HTTP local node.

    The toolkit hardcodes ``use_ssl=True`` (and the AOSS SigV4 signer) in
    ``create_os_client``/``create_os_async_client`` — correct for AOSS, but the
    local Docker stack runs vanilla OpenSearch over plain HTTP, so every Tier-3
    vector read dies with ``SSL: WRONG_VERSION_NUMBER``. Selected only by
    ``OPENSEARCH_AUTH=none`` (the same switch ``coa_common.opensearch`` honors);
    production defaults keep the signed/TLS clients untouched.

    Idempotent and failure-tolerant: skips with a log if the toolkit internals
    have moved.
    """
    global _tls_patched
    if _tls_patched:
        return
    import os

    if os.getenv("OPENSEARCH_AUTH", "sigv4").lower() != "none":
        return
    try:
        import graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes as ovi
        from opensearchpy import AsyncOpenSearch, OpenSearch
    except ImportError:
        logger.debug("graphrag_opensearch_transport_patch_skipped", reason="module not importable")
        return

    endpoint = os.getenv("OPENSEARCH_ENDPOINT", "")
    if not endpoint:
        logger.warning("graphrag_opensearch_transport_patch_skipped", reason="OPENSEARCH_ENDPOINT unset")
        return

    def _local_client(sync: bool):
        if sync:
            return OpenSearch(
                hosts=[endpoint],
                use_ssl=endpoint.startswith("https://"),
                verify_certs=False,
            )
        return AsyncOpenSearch(hosts=[endpoint], use_ssl=endpoint.startswith("https://"), verify_certs=False)

    ovi.create_os_client = lambda *_, **__: _local_client(sync=True)
    ovi.create_os_async_client = lambda *_, **__: _local_client(sync=False)
    _tls_patched = True
    logger.info("graphrag_opensearch_transport_patch_applied", endpoint=endpoint)


def patch_paginated_search_source() -> None:
    """Make ``paginated_search`` project ``embedding`` into ``_source``.

    Idempotent and failure-tolerant: if the toolkit's internals have moved, this
    logs and returns rather than breaking retrieval (the caller degrades to the
    pre-patch behavior, i.e. a narrow beam, not an error).
    """
    global _patched
    if _patched:
        return
    try:
        import graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes as ovi
        from opensearchpy.exceptions import NotFoundError
    except ImportError:
        logger.debug("graphrag_embedding_source_patch_skipped", reason="module not importable")
        return

    original = getattr(ovi.OpenSearchIndex, "paginated_search", None)
    if original is None:
        logger.warning("graphrag_embedding_source_patch_skipped", reason="paginated_search missing")
        return

    def paginated_search(self, query, page_size=10000, max_pages=None, ids_only=False):
        # ids_only deliberately sets "_source": False upstream; only widen the
        # projection for the embedding-fetch path.
        if ids_only:
            yield from original(self, query, page_size=page_size, max_pages=max_pages, ids_only=True)
            return

        client = self.client._os_client
        if client is None:
            return

        search_after = None
        page = 0
        while True:
            body = {
                "size": page_size,
                "query": query,
                "sort": [{"_id": "asc"}],
                # The one line this patch exists for: AOSS NEXTGEN omits
                # knn_vector from the default projection.
                "_source": {"includes": _EMBEDDING_SOURCE_INCLUDES},
            }
            if search_after:
                body["search_after"] = search_after

            # Preserve upstream's index-not-found retry (a namespace can be queried
            # before its topic index materializes); everything else propagates.
            not_found_retries = 0
            response = None
            while response is None:
                try:
                    response = client.search(index=self.underlying_index_name(), body=body)
                except NotFoundError:
                    not_found_retries += 1
                    if not_found_retries > 3:
                        raise
                    time.sleep(5)

            hits = response["hits"]["hits"]
            if not hits:
                break

            yield hits

            search_after = hits[-1]["sort"]
            page += 1
            if max_pages and page >= max_pages:
                break

    ovi.OpenSearchIndex.paginated_search = paginated_search
    _patched = True
    logger.info("graphrag_embedding_source_patch_applied", includes=_EMBEDDING_SOURCE_INCLUDES)
