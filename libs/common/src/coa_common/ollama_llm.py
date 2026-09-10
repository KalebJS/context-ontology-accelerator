# Copyright Amazon.com, Inc. or its affiliates. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Ollama chat LLM adapter for graphrag-toolkit (local Docker stack).

``LLM_PROVIDER=ollama`` (see docker/docker-compose.yml ``x-ollama-env``) routes
chat traffic to the host's Ollama daemon instead of Bedrock. Most COA services
consume chat through their own clients (``coa_common.BedrockClient`` routes
``invoke`` to Ollama; serve's ``OllamaLLMClient`` mirrors ``BedrockLLMClient``).
The one place that needs an additional adapter is graphrag-toolkit:
``GraphRAGConfig.extraction_llm`` accepts either a llama-index ``LLM`` instance
or a model-id STRING, and the string path always builds a ``BedrockConverse`` —
there is no provider switch. A llama-index ``CustomLLM`` instance is therefore
the only way to point extraction at Ollama.
"""

from __future__ import annotations

import os
import random
import time
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

_OLLAMA_DEFAULT_BASE_URL = "http://host.docker.internal:11434"
_OLLAMA_DEFAULT_CHAT_MODEL = "gemma4:latest"

# HTTP statuses worth retrying on the Ollama chat path — the same philosophy as
# the kg-build OpenSearch patches (graph_build._PAGINATED_RETRY_STATUS):
# 429 throttle + transient 5xx. In the 2026-09-10 apollo13-transcripts incident
# exactly ONE transient 503 from Ollama's cloud proxy destroyed a 52-minute
# extraction run — zero retries existed anywhere on that path.
_RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})

_OLLAMA_LLAMA_ADAPTER_CLS: Any = None


def make_ollama_llama_index_llm(
    model_id: str | None = None,
    base_url: str | None = None,
    timeout_s: float | None = None,
    max_tokens: int = 16384,
    context_window: int = 32768,
) -> Any:
    """Build a picklable llama-index ``LLM`` backed by Ollama's OpenAI-compatible API.

    Module-level dynamic class (same pattern as ``make_llama_index_embedding`` in
    ``coa_common.embeddings``): graphrag's build pipeline pickles the LLM into
    ``ProcessPoolExecutor`` workers, and a locally-defined class or closure fails
    to pickle. Assigning ``__module__``/``__qualname__`` and stashing the class in
    module globals makes pickle resolve it by qualified name.

    Args:
        model_id: Ollama chat model tag (default ``OLLAMA_CHAT_MODEL``).
        base_url: Ollama base URL (default ``OLLAMA_BASE_URL``).
        timeout_s: Per-request read timeout in seconds; None honors
            ``OLLAMA_TIMEOUT_S`` (default 120).
        max_tokens: Advertised output budget (metadata only; Ollama manages
            generation length server-side unless ``num_predict`` is set).
        context_window: Advertised context window; also sent as Ollama's
            ``num_ctx`` so long extraction prompts are not silently truncated.

    Returns:
        A llama-index ``CustomLLM`` instance.
    """
    global _OLLAMA_LLAMA_ADAPTER_CLS
    if _OLLAMA_LLAMA_ADAPTER_CLS is not None:
        return _OLLAMA_LLAMA_ADAPTER_CLS(
            model_id=model_id or os.environ.get("OLLAMA_CHAT_MODEL", _OLLAMA_DEFAULT_CHAT_MODEL),
            base_url=(base_url or os.environ.get("OLLAMA_BASE_URL", _OLLAMA_DEFAULT_BASE_URL)).rstrip("/"),
        )

    from llama_index.core.base.llms.types import (
        CompletionResponse,
        CompletionResponseGen,
        LLMMetadata,
    )
    from llama_index.core.llms.custom import CustomLLM

    resolved_model = model_id or os.environ.get("OLLAMA_CHAT_MODEL", _OLLAMA_DEFAULT_CHAT_MODEL)
    resolved_base_url = (base_url or os.environ.get("OLLAMA_BASE_URL", _OLLAMA_DEFAULT_BASE_URL)).rstrip("/")
    resolved_timeout = float(os.environ.get("OLLAMA_TIMEOUT_S", "120")) if timeout_s is None else timeout_s
    # Transient-failure retries (429/5xx from a busy proxy, cloud model slot, or
    # upstream; plus connection/timeout errors). A single 503 from Ollama's
    # scheduler otherwise aborts an hours-long GraphRAG build with no retry.
    # Total attempts (including the first), base delay, and the per-delay cap
    # are env-tunable; a well-behaved endpoint never triggers a retry, so
    # production behavior is strictly an improvement over the un-retried path.
    max_attempts = max(1, int(os.environ.get("OLLAMA_MAX_RETRIES", "5")))
    retry_backoff_s = max(0.0, float(os.environ.get("OLLAMA_RETRY_BACKOFF_S", "2")))
    retry_cap_s = max(0.0, float(os.environ.get("OLLAMA_RETRY_CAP_S", "30")))

    def _retry_delay(attempt: int) -> float:
        """Exponential backoff (base * 2^(attempt-1)) with proportional jitter, capped."""
        return min(retry_cap_s, retry_backoff_s * (2 ** (attempt - 1)) + random.uniform(0, retry_backoff_s * 0.25))

    def _chat_completion(prompt: str, temperature: float = 0.0) -> str:
        """Single blocking chat completion against Ollama's /v1/chat/completions."""
        import httpx

        payload: dict[str, Any] = {
            "model": resolved_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "stream": False,
            "options": {"num_ctx": context_window},
        }
        start = time.monotonic()
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = httpx.post(
                    f"{resolved_base_url}/v1/chat/completions",
                    json=payload,
                    timeout=resolved_timeout,
                )
                if resp.status_code in _RETRYABLE_STATUS and attempt < max_attempts:
                    delay = _retry_delay(attempt)
                    logger.warning(
                        "ollama_llm_transient_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        backoff_seconds=delay,
                        elapsed_seconds=round(time.monotonic() - start, 2),
                    )
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                body = resp.json()
                return body["choices"][0]["message"]["content"] or ""
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_error = e
                if attempt >= max_attempts:
                    break
                delay = _retry_delay(attempt)
                logger.warning(
                    "ollama_llm_transient_error",
                    error_type=type(e).__name__,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    backoff_seconds=delay,
                    elapsed_seconds=round(time.monotonic() - start, 2),
                )
                time.sleep(delay)
            except Exception:
                logger.error(
                    "ollama_llm_completion_failed",
                    base_url=resolved_base_url,
                    model=resolved_model,
                    elapsed_seconds=round(time.monotonic() - start, 2),
                    exc_info=True,
                )
                raise
        logger.error(
            "ollama_llm_completion_failed",
            base_url=resolved_base_url,
            model=resolved_model,
            elapsed_seconds=round(time.monotonic() - start, 2),
            exc_info=last_error,
        )
        raise last_error if last_error is not None else RuntimeError("ollama chat completion failed")

    class OllamaLLMLlamaIndex(CustomLLM):
        """Picklable LlamaIndex adapter over Ollama's /v1/chat/completions."""

        model_id: str
        base_url: str
        max_tokens: int = 16384
        context_window: int = 32768

        @property
        def metadata(self) -> LLMMetadata:  # noqa: D102 - llama-index contract
            return LLMMetadata(
                context_window=self.context_window,
                num_output=self.max_tokens,
                is_chat_model=True,
                model_name=self.model_id,
                is_function_calling_model=False,
            )

        def complete(
            self, prompt: str, formatted: bool = False, temperature: float = 0.0, **_: Any
        ) -> CompletionResponse:
            return CompletionResponse(text=_chat_completion(prompt, temperature=temperature))

        def stream_complete(
            self, prompt: str, formatted: bool = False, temperature: float = 0.0, **_: Any
        ) -> CompletionResponseGen:
            # graphrag's extraction path calls predict()/complete() (non-streaming);
            # yield the whole completion as a single chunk.
            def _gen() -> CompletionResponseGen:
                yield CompletionResponse(text=_chat_completion(prompt, temperature=temperature))

            return _gen()

    OllamaLLMLlamaIndex.__module__ = __name__
    OllamaLLMLlamaIndex.__qualname__ = "OllamaLLMLlamaIndex"
    globals()["OllamaLLMLlamaIndex"] = OllamaLLMLlamaIndex
    _OLLAMA_LLAMA_ADAPTER_CLS = OllamaLLMLlamaIndex
    return OllamaLLMLlamaIndex(
        model_id=resolved_model,
        base_url=resolved_base_url,
    )
