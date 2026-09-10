# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Client factory — constructs the default backend set.

Callers should use `build_clients()` and accept a `ClientSet`.
The resolution pipeline never imports concrete implementations directly.

IMPORTANT: build_clients() creates synchronous boto3 clients internally.
Call it during application startup (outside the async event loop) or wrap
in run_in_executor if calling from async context.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import structlog

from .base import ClientSet

if TYPE_CHECKING:
    from ..config import ServiceConfig
    from ..lexical.baseline_retriever import BaselineLexicalRetriever

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Neptune Analytics detection helpers
# ---------------------------------------------------------------------------

_NA_GRAPH_ID_RE = re.compile(r"g-[a-z0-9]{10,}")


def _extract_graph_id(endpoint: str) -> str | None:
    """Extract the Neptune Analytics graph-id (g-…) from an endpoint string.

    Recognises:
      - A bare graph-id: "g-abc1234567"
      - A neptune-graph:// URI: "neptune-graph://g-abc1234567"
      - An NA host: "g-abc1234567.us-east-1.neptune-graph.amazonaws.com"
    """
    m = _NA_GRAPH_ID_RE.search(endpoint)
    return m.group(0) if m else None


def _is_neptune_analytics(endpoint: str) -> bool:
    """Return True if *endpoint* refers to a Neptune Analytics graph.

    The service-host test is anchored to the end of the host so a value such as
    ``neptune-graph.amazonaws.com.other.test`` is not treated as Neptune
    Analytics. This picks which client to construct, not whether to trust the
    endpoint, but keeping the match host-anchored avoids routing traffic to the
    wrong backend on a malformed config value.
    """
    if endpoint.startswith("neptune-graph://"):
        return True
    try:
        host = (urlsplit(endpoint if "://" in endpoint else f"//{endpoint}").hostname or "").lower()
    except ValueError:
        # ``hostname`` raises on an unparseable netloc (stray brackets, or
        # characters that fail NFKC normalization). Such a value cannot be a real
        # NA host, so fall through to the graph-id heuristic rather than failing
        # client construction at startup.
        host = ""
    if host == "neptune-graph.amazonaws.com" or host.endswith(".neptune-graph.amazonaws.com"):
        return True
    # A bare or embedded graph-id (g-…) with no NDB port indicator
    return bool(_extract_graph_id(endpoint) and ":8182" not in endpoint)


def _graph_store_uri(neptune_endpoint: str) -> str:
    """Derive the graphrag-toolkit graph-store URI from the deployment endpoint.

    Local Docker stack: ``GRAPH_STORE_URI`` (set alongside ``GRAPH_AUTH=none``)
    wins outright — the local document graph lives in a Neo4j container
    (``bolt://``), which no URI can be derived from ``NEPTUNE_ENDPOINT`` (the
    local Fuseki host serves SPARQL, not openCypher).

    Neptune Analytics → ``neptune-graph://g-…``
    Neptune DB (default) → ``neptune-db://host:8182``
    """
    override = os.environ.get("GRAPH_STORE_URI", "").strip()
    if override:
        return override
    if _is_neptune_analytics(neptune_endpoint):
        graph_id = _extract_graph_id(neptune_endpoint)
        if graph_id:
            return f"neptune-graph://{graph_id}"
        # Fallback: strip protocol prefix and use as-is
        cleaned = neptune_endpoint.removeprefix("neptune-graph://")
        return f"neptune-graph://{cleaned}"

    # NDB: strip protocol prefixes, extract host, append port
    host = neptune_endpoint.replace("wss://", "").replace("https://", "").rstrip("/")
    host = host.split(":")[0]
    return f"neptune-db://{host}:8182"


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------


def build_clients() -> ClientSet:
    """Construct the client set for the serve component.

    Must be called outside the async event loop (e.g., during app startup).
    """
    logger.info("building_clients")
    return _build_default_clients()


def _build_default_clients() -> ClientSet:
    from .bedrock import BedrockLLMClient
    from .neptune import NeptuneGraphClient
    from .opensearch import OpenSearchVectorClient
    from .source_db import SourceDBQueryExecutor

    return ClientSet(
        graph=NeptuneGraphClient(),
        vector=OpenSearchVectorClient(),
        llm=BedrockLLMClient(),
        # Executes authorized SQL against customer relational databases
        # (PostgreSQL via asyncpg, Athena for cross-source federation).
        query_executor=SourceDBQueryExecutor(),
    )


def build_lexical_retriever(
    config: ServiceConfig, *, timeout_s: float | None = None
) -> BaselineLexicalRetriever | None:
    """Build the lexical baseline retriever (graphrag-toolkit adapter).

    Built on every deployment so a per-request ``options.retrieverStrategy`` can
    trigger graphrag retrieval on-demand, regardless of ``TIER3_STRATEGY``.
    Whether the *default* (no request field) Tier 3 path uses it is decided in
    the orchestrator: ``lexical-baseline`` engages a deployment-default
    strategy, while ``hand-rolled`` only engages graphrag when a request asks
    for it. Construction is cheap and keeps the graphrag import lazy (the engine
    is not built until ``retrieve()`` runs), so the hand-rolled startup path and
    the botocore sync-only invariant are unchanged.

    ``timeout_s`` overrides the per-retrieval timeout. The agentic path passes its
    own per-tool budget here: the ``BaselineLexicalRetriever`` default (15s) is
    below what a graphrag traversal strategy needs, so leaving it at the default
    made every ``strategy:*`` tool time out at 15s even though the agentic budget
    allowed 45s — the tool reported "degraded ... TimeoutError after 15.0s", the
    loop scored it as no-progress, and it burned a step escalating (observed live
    on ``strategy:entity_based`` / ``strategy:chunk_based_semantic``).

    When ``timeout_s`` is None (the STANDARD-mode request path, which does NOT
    pass a per-tool budget), the timeout falls back to the ``LEXICAL_RETRIEVER_TIMEOUT_S``
    env var (default 45s). The 15s retriever default is likewise too low for the
    slower single-shot traversal strategies here: ``topic_beam``'s graph
    beam-traversal exceeds 15s and would time out to an empty result in standard
    mode (observed live on the NTSB corpus), even though the AgentCore request
    ceiling (~180s) leaves ample room. The env var lets a deployment tune this
    without a code change; the per-deployment production default (15s) stays the
    code default on the retriever itself.

    Returns None only when the required store endpoints are unset (nothing for
    graphrag to address).
    """
    if not config.neptune_endpoint or not config.opensearch_endpoint:
        return None

    # Standard-mode path (timeout_s is None): honor LEXICAL_RETRIEVER_TIMEOUT_S so
    # slow traversal strategies (e.g. topic_beam) are not truncated by the 15s
    # retriever default. The agentic path always passes an explicit budget, which
    # takes precedence over this fallback.
    if timeout_s is None:
        timeout_s = 45.0  # standard-mode default (see docstring); env var overrides below
        if env_timeout := os.environ.get("LEXICAL_RETRIEVER_TIMEOUT_S", "").strip():
            try:
                timeout_s = float(env_timeout)
            except ValueError:
                # Keep the 45s default rather than falling through to the
                # retriever's own 15s (the value this fallback exists to avoid).
                logger.warning(
                    "invalid_lexical_retriever_timeout",
                    value=env_timeout,
                    fallback_timeout_s=timeout_s,
                )

    from ..lexical import BaselineLexicalRetriever
    from ..lexical.strategies import DEFAULT_SPEC

    graph_uri = _graph_store_uri(config.neptune_endpoint)
    vector_store_uri = f"aoss://{config.opensearch_endpoint}"

    logger.info(
        "building_lexical_retriever",
        tier3_strategy=config.tier3_strategy,
        graph_store_uri=graph_uri,
        vector_store_uri=vector_store_uri,
    )

    # The instance-default StrategySpec is only a fallback; the orchestrator
    # passes the resolved per-request strategy into ``retrieve()`` whenever the
    # lexical path is engaged.
    return BaselineLexicalRetriever(
        graph_store_uri=graph_uri,
        vector_store_uri=vector_store_uri,
        strategy=DEFAULT_SPEC,
        **({"timeout_s": timeout_s} if timeout_s is not None else {}),
    )
