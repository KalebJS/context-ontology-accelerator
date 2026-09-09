# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""LLM client for document-based ontology extraction."""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from coa_common import async_boto_config
from coa_common.bedrock import extract_text_blocks
from coa_common.bedrock_metrics import CostTracker

from coa_ontology.inducer.services.grounding import _BEDROCK_MAX_POOL_CONNECTIONS


class LLMClient:
    """Calls Bedrock for text generation.

    The ``generate`` method is synchronous: the underlying
    ``boto3.client.converse`` call is blocking, and wrapping it in an
    ``async def`` would block whatever event loop awaits it without
    gaining any concurrency. Callers that need background execution
    should schedule the call via a thread pool or ``asyncio.to_thread``.

    NOTE: the ``generate`` cost-tracking seam is currently exercised only by the
    document/batch induction paths, which do not yet thread a CostTracker in —
    wiring those callers is tracked as a follow-up (document-path metering).
    """

    def __init__(
        self,
        model_id: str = "us.anthropic.claude-sonnet-4-6",
        region: str = "us-east-1",
        cost_tracker: CostTracker | None = None,
    ):
        """Configure the Bedrock model, region, and optional cost tracker.

        Args:
            model_id: Bedrock model id used for generation.
            region: AWS region for the Bedrock runtime client.
            cost_tracker: Optional tracker that records token usage per generate call.
        """
        self.model_id = model_id
        self.region = region
        self._client = None
        self._cost_tracker = cost_tracker

    @property
    def client(self):
        """Lazily create and cache the Bedrock runtime client."""
        if self._client is None:
            # LLM generation can be slow; keep a generous read budget (like the
            # serve/libs Bedrock clients) while still bounding retries + connect.
            # Same connection-pool cap as the grounding rerank client (see
            # grounding.py:_BEDROCK_MAX_POOL_CONNECTIONS) to prevent a future
            # pool-starvation footgun if this client is ever fanned out.
            self._client = boto3.client(
                "bedrock-runtime",
                region_name=self.region,
                config=async_boto_config(read_timeout=120, max_pool_connections=_BEDROCK_MAX_POOL_CONNECTIONS),
            )
        return self._client

    def generate(self, prompt: str, system: str = "", max_tokens: int = 4096) -> str:
        """Generate text from the configured LLM provider.

        Provider switch: ``LLM_PROVIDER=ollama`` routes to the host's Ollama
        (local Docker stack); default ``bedrock`` keeps the Converse path.

        Args:
            prompt: User prompt sent as the message content.
            system: Optional system prompt; omitted when empty.
            max_tokens: Maximum number of tokens to generate.

        Returns:
            The generated text from the model's response.

        Raises:
            RuntimeError: If the LLM call fails or returns an unexpected shape.
        """
        if os.environ.get("LLM_PROVIDER", "bedrock").lower() == "ollama":
            return self._generate_ollama(prompt, system=system, max_tokens=max_tokens)
        return self._generate_bedrock(prompt, system=system, max_tokens=max_tokens)

    def _generate_ollama(self, prompt: str, *, system: str, max_tokens: int) -> str:
        """Generate via Ollama's OpenAI-compatible chat endpoint (synchronous)."""
        import json

        import httpx

        base_url = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434").rstrip("/")
        model = os.environ.get("OLLAMA_CHAT_MODEL", "gemma4:latest")
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        try:
            with httpx.Client(timeout=httpx.Timeout(connect=5.0, read=180, write=30, pool=180)) as client:
                resp = client.post(
                    f"{base_url}/v1/chat/completions",
                    content=json.dumps(
                        {"model": model, "messages": messages, "max_tokens": max_tokens, "stream": False}
                    ),
                    headers={"Content-Type": "application/json"},
                )
        except (httpx.HTTPError, OSError) as e:
            raise RuntimeError(f"Ollama chat failed for model {model!r}: {e}") from e
        if resp.status_code >= 400:
            raise RuntimeError(f"Ollama chat failed ({resp.status_code}): {resp.text[:300]}")
        try:
            choices = resp.json().get("choices") or []
            text = choices[0].get("message", {}).get("content", "")
            if not text:
                raise ValueError("empty content")
            return text
        except (KeyError, IndexError, TypeError, ValueError) as e:
            raise RuntimeError(f"Unexpected Ollama response shape: {resp.text[:200]!r}") from e

    def _generate_bedrock(self, prompt: str, *, system: str, max_tokens: int) -> str:
        """Generate via Bedrock Converse (unchanged production path)."""
        messages = [{"role": "user", "content": [{"text": prompt}]}]
        sys_list = [{"text": system}] if system else []
        try:
            # No temperature: deprecated on newer models (e.g. Claude Opus 5),
            # which reject the request rather than ignore it; we only ever set 0
            # (the model default), so omit it.
            resp = self.client.converse(
                modelId=self.model_id,
                messages=messages,
                system=sys_list,
                inferenceConfig={"maxTokens": max_tokens},
            )
        except (ClientError, BotoCoreError) as e:
            raise RuntimeError(f"Bedrock converse() failed for model {self.model_id!r}: {e}") from e
        # Record token usage on the success path (before parsing, which may raise
        # on a malformed shape — usage is present regardless of body shape).
        if self._cost_tracker is not None:
            self._cost_tracker.record_from_converse("generate", self.model_id, resp)
        try:
            # Select the text block by type, not position: a reasoning model puts
            # a reasoningContent block (no "text" key) at index 0.
            return extract_text_blocks(resp["output"]["message"]["content"])
        except (KeyError, IndexError, TypeError, ValueError) as e:
            raise RuntimeError(f"Unexpected Bedrock response shape: {resp!r}") from e
