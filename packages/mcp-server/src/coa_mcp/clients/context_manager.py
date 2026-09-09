# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""HTTP client for invoking the Context Manager AgentCore Runtime.

The MCP server is a thin protocol adapter. Execution tools (query,
translate_sparql, rag_retrieval, graph_traversal) delegate to the Context
Manager via its AgentCore invocations endpoint. This avoids duplicating
DB/Neptune/OpenSearch/Bedrock access in the MCP runtime.

The Context Manager's @app.entrypoint handles:
- {"query": ..., "namespace": ..., "profile": ...} → full tiered resolve
- {"action": "translate", "query": ..., "namespace": ...} → NL-to-SPARQL
- {"action": "kbSearch", "query": ..., "namespace": ...} → vector search
- {"action": "graphTraverse", "startUri": ..., "namespace": ...} → graph

Identity propagation: the caller's JWT is forwarded as-is. The CM validates
it again (same Cognito pool) and resolves roles independently.
"""

from __future__ import annotations

import os
from urllib.parse import quote

import httpx
import structlog
from httpx_sse import aconnect_sse

logger = structlog.get_logger(__name__)

# AgentCore invocation timeout. No API Gateway in this path, so no 30s cap.
# TODO: 120s is BELOW serve's RESOLVE_TIMEOUT_S (170s), so this aborts while serve
# still works — agentic p50 is ~141s. Raising it needs an infra value + MCP retest.
_INVOKE_TIMEOUT_S = int(os.environ.get("CM_INVOKE_TIMEOUT_S", "120"))


class ContextManagerClient:
    """Async HTTP client for invoking the Context Manager via AgentCore."""

    def __init__(self, runtime_arn: str, region: str):
        """Initialize the client and build the AgentCore invocation URL.

        Args:
            runtime_arn: ARN of the Context Manager AgentCore runtime.
            region: AWS region hosting the runtime.
        """
        self._region = region
        self._runtime_arn = runtime_arn
        # Local Docker stack: CM_DIRECT_URL points at the context-manager
        # container directly (plain HTTP, no AgentCore). When unset, the
        # production AgentCore invocation URL is constructed as before.
        direct = os.environ.get("CM_DIRECT_URL", "").rstrip("/")
        if direct:
            self._url = f"{direct}/invocations"
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=2.0,
                    read=_INVOKE_TIMEOUT_S,
                    write=_INVOKE_TIMEOUT_S,
                    pool=_INVOKE_TIMEOUT_S,
                )
            )
            logger.info("cm_client_initialized_direct", url=self._url)
            return
        encoded_arn = quote(runtime_arn, safe="")
        self._url = (
            f"https://bedrock-agentcore.{region}.amazonaws.com/runtimes/{encoded_arn}/invocations?qualifier=DEFAULT"
        )
        # Fast connect timeout, long read budget (the CM does full tiered
        # resolution). No retry here: the CM retries its own dependencies, so
        # retrying the whole invocation would stack retries across layers.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=2.0,
                read=_INVOKE_TIMEOUT_S,
                write=_INVOKE_TIMEOUT_S,
                pool=_INVOKE_TIMEOUT_S,
            )
        )
        logger.info(
            "cm_client_initialized",
            runtime_arn=runtime_arn,
            region=region,
        )

    async def invoke(self, payload: dict, bearer_token: str) -> dict:
        """Invoke the Context Manager with the given payload.

        The Context Manager's @app.entrypoint is an async generator. AgentCore
        returns responses as text/event-stream (SSE). We use httpx-sse to parse
        the stream per the SSE spec and return the first event's JSON payload.

        Args:
            payload: JSON body matching InvokeRequest schema.
            bearer_token: The caller's JWT (forwarded as-is for identity propagation).

        Returns:
            Parsed JSON response from the Context Manager.

        Raises:
            ContextManagerError: On HTTP errors or timeouts.
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        }
        try:
            async with aconnect_sse(self._client, "POST", self._url, json=payload, headers=headers) as event_source:
                if event_source.response.status_code >= 400:
                    body = await event_source.response.aread()
                    logger.warning(
                        "cm_invoke_error",
                        status=event_source.response.status_code,
                        body=body.decode()[:200],
                    )
                    raise ContextManagerError(
                        f"Context Manager returned {event_source.response.status_code}: {body.decode()[:200]}"
                    )

                async for sse in event_source.aiter_sse():
                    return sse.json()

                raise ContextManagerError("Context Manager returned empty SSE stream")

        except httpx.TimeoutException as exc:
            logger.error("cm_invoke_timeout", timeout_s=_INVOKE_TIMEOUT_S)
            raise ContextManagerError(f"Context Manager request timed out ({_INVOKE_TIMEOUT_S}s)") from exc
        except httpx.HTTPError as exc:
            logger.error("cm_invoke_http_error", error=str(exc))
            raise ContextManagerError(f"HTTP error invoking Context Manager: {exc}") from exc

    async def close(self) -> None:
        """Close the underlying HTTP client, releasing pooled connections."""
        await self._client.aclose()


class ContextManagerError(Exception):
    """Raised when the Context Manager invocation fails."""
