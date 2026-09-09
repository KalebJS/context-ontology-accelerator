# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Ollama LLM client — LLMClient implementation for the local Docker stack.

Implements the same ``LLMClient`` Protocol as
:class:`coa_serve.clients.bedrock.BedrockLLMClient` (converse, converse_stream,
embed, health_check) against the host's Ollama daemon via its OpenAI-compatible
API (``/v1/chat/completions``, ``/v1/embeddings``).

Selected at runtime by ``LLM_PROVIDER=ollama`` (see main.py factory). Guardrails
are a Bedrock feature and are not emulated: ``guardrail_id`` is ignored, results
carry ``GuardrailOutcome.NONE`` and never raise
:class:`GuardrailBlockedError`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import structlog

from .base import (
    ConverseResult,
    GuardrailOutcome,
    instrumented,
)

logger = structlog.get_logger(__name__)

_DEFAULT_BASE_URL = "http://host.docker.internal:11434"
_DEFAULT_CHAT_MODEL = "gemma4:latest"
_DEFAULT_EMBED_MODEL = "mxbai-embed-large:latest"

# Ollama streaming reads can idle longer between tokens on CPU hosts.
_STREAM_READ_TIMEOUT_S = 300

_EXECUTOR_MAX = 8


class OllamaLLMClient:
    """Ollama chat + embeddings client (OpenAI-compatible endpoints)."""

    def __init__(
        self,
        model_id: str | None = None,
        embed_model_id: str | None = None,
        base_url: str | None = None,
        timeout_s: int | None = None,
    ):
        """Configure endpoint, models, and timeouts.

        Args:
            model_id: Chat model tag. Defaults to ``OLLAMA_CHAT_MODEL`` then
                ``gemma4:latest``.
            embed_model_id: Embedding model tag. Defaults to
                ``OLLAMA_EMBED_MODEL`` then ``mxbai-embed-large:latest``.
            base_url: Ollama base URL. Defaults to ``OLLAMA_BASE_URL`` then
                ``http://host.docker.internal:11434``.
            timeout_s: Read timeout in seconds. Defaults to ``OLLAMA_TIMEOUT_S``
                then 120.
        """
        self._model_id = model_id or os.environ.get("OLLAMA_CHAT_MODEL", _DEFAULT_CHAT_MODEL)
        self._embed_model_id = embed_model_id or os.environ.get("OLLAMA_EMBED_MODEL", _DEFAULT_EMBED_MODEL)
        self._base_url = (base_url or os.environ.get("OLLAMA_BASE_URL", _DEFAULT_BASE_URL)).rstrip("/")
        self._timeout_s = timeout_s or int(os.environ.get("OLLAMA_TIMEOUT_S", "120"))
        self._client: httpx.AsyncClient | None = None
        self._http_lock = asyncio.Lock()
        logger.info(
            "ollama_client_configured",
            base_url=self._base_url,
            model_id=self._model_id,
            embed_model_id=self._embed_model_id,
        )

    @property
    def model_id(self) -> str:
        """Return the default chat model tag for this client."""
        return self._model_id

    async def _get_http(self) -> httpx.AsyncClient:
        if self._client is None:
            async with self._http_lock:
                if self._client is None:
                    timeout = httpx.Timeout(
                        connect=5.0, read=self._timeout_s, write=self._timeout_s, pool=self._timeout_s
                    )
                    self._client = httpx.AsyncClient(timeout=timeout, base_url=self._base_url)
        return self._client

    async def close(self) -> None:
        """Release the pooled HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @instrumented("ollama")
    async def converse(
        self,
        prompt: str,
        *,
        system: str | None = None,
        guardrail_id: str | None = None,
        max_tokens: int = 4096,
        temperature: float | None = None,
        guard_content: str | None = None,
        model_id: str | None = None,
    ) -> ConverseResult:
        """Single-turn chat completion via ``/v1/chat/completions``.

        ``guardrail_id``/``guard_content`` are accepted for Protocol parity;
        no guardrail evaluation is performed locally.
        """
        effective_model = model_id or self._model_id
        messages: list[dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": system})
        content = prompt if not guard_content else f"{prompt}\n\n{guard_content}"
        messages.append({"role": "user", "content": content})

        payload: dict[str, Any] = {
            "model": effective_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if temperature is not None:
            payload["temperature"] = temperature

        http = await self._get_http()
        start = time.monotonic()
        try:
            resp = await http.post(
                "/v1/chat/completions",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
        except httpx.TimeoutException:
            logger.warning("ollama_converse_timeout", model=effective_model, timeout_s=self._timeout_s)
            raise
        if resp.status_code >= 400:
            raise RuntimeError(f"Ollama chat failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()

        choices = data.get("choices") or []
        if not choices:
            raise ValueError("Ollama chat response contained no choices")
        text = choices[0].get("message", {}).get("content", "")
        finish = choices[0].get("finish_reason", "")
        stop_reason = "max_tokens" if finish == "length" else "end_turn"

        latency_ms = (time.monotonic() - start) * 1000
        if stop_reason == "max_tokens":
            logger.warning(
                "ollama_output_truncated", model=effective_model, max_tokens=max_tokens, text_chars=len(text)
            )
        else:
            logger.info("ollama_converse_ok", model=effective_model, duration_ms=int(latency_ms))
        return ConverseResult(text=text, outcome=GuardrailOutcome.NONE, stop_reason=stop_reason)

    async def converse_stream(
        self,
        prompt: str,
        *,
        system: str | None = None,
        guardrail_id: str | None = None,
        max_tokens: int = 4096,
        guard_content: str | None = None,
        model_id: str | None = None,
    ) -> AsyncIterator[str]:
        """Stream text tokens from ``/v1/chat/completions`` with stream=True."""
        effective_model = model_id or self._model_id
        messages: list[dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": system})
        content = prompt if not guard_content else f"{prompt}\n\n{guard_content}"
        messages.append({"role": "user", "content": content})

        payload: dict[str, Any] = {
            "model": effective_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "stream": True,
        }

        http = await self._get_http()
        req = http.build_request(
            "POST",
            "/v1/chat/completions",
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        response = await http.send(req, stream=True)
        try:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                chunk = line[len("data: ") :]
                if chunk == "[DONE]":
                    break
                try:
                    data = json.loads(chunk)
                except json.JSONDecodeError:
                    continue
                choices = data.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta", {})
                piece = delta.get("content") or ""
                if piece:
                    yield piece
        finally:
            await response.aclose()

    @instrumented("ollama")
    async def embed(self, text: str) -> list[float]:
        """Embed text via the Ollama embeddings endpoint."""
        http = await self._get_http()
        payload = {"model": self._embed_model_id, "input": text}
        resp = await http.post(
            "/v1/embeddings", content=json.dumps(payload), headers={"Content-Type": "application/json"}
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Ollama embeddings failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        embeddings = data.get("data") or []
        if not embeddings:
            raise ValueError("Ollama embeddings response contained no data")
        vector = embeddings[0].get("embedding") or []
        if not vector:
            raise ValueError("Ollama embeddings returned an empty vector")
        return vector

    async def health_check(self) -> dict[str, Any]:
        """Probe Ollama reachability via a test embedding call. Never raises."""
        try:
            vector = await asyncio.wait_for(self.embed("health"), timeout=30)
            return {
                "status": "ok",
                "model": self._model_id,
                "embed_model": self._embed_model_id,
                "dimensions": len(vector),
            }
        except Exception as e:
            return {"status": "error", "detail": f"{type(e).__name__}: {e}"}
