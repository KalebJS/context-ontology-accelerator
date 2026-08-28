# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Tier 3 synthesizer."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from coa_serve.clients.base import ConverseResult
from coa_serve.tier3.graph_traverser import GraphEntity
from coa_serve.tier3.synthesizer import Synthesizer
from coa_serve.tier3.vector_retriever import ChunkResult


@pytest.fixture
def mock_bedrock():
    client = AsyncMock()
    client.converse.return_value = ConverseResult(text="The claims SLA is 10 business days per Policy v2.3.")
    return client


@pytest.fixture
def synthesizer(mock_bedrock):
    return Synthesizer(mock_bedrock, guardrail_id="gr-test-123")


def _make_chunk(chunk_id="c1", text="SLA is 10 days", score=0.9, doc_name="Policy"):
    return ChunkResult(
        chunk_id=chunk_id,
        text=text,
        source_doc="policy.pdf",
        relevance_score=score,
        label=doc_name,
    )


def _make_entity(uri="http://ex.org/Claim", label="Claim"):
    return GraphEntity(uri=uri, label=label, type="owl:Class", relationships=())


@pytest.mark.unit
class TestSynthesizerBasic:
    async def test_calls_bedrock(self, synthesizer, mock_bedrock):
        chunks = [_make_chunk()]
        entities = [_make_entity()]
        result = await synthesizer.synthesize("What is the SLA?", chunks, entities)
        assert result.answer == "The claims SLA is 10 business days per Policy v2.3."
        mock_bedrock.converse.assert_awaited_once()
        assert "guardrail_id" in str(mock_bedrock.converse.call_args)

    async def test_returns_duration(self, synthesizer):
        result = await synthesizer.synthesize("test", [], [])
        assert result.duration_ms >= 0


@pytest.mark.unit
class TestSynthesizerLanguageDirective:
    """Guards the output-language directive: the assembled system instruction must
    instruct the model to answer in the question's language, so a Japanese question
    yields a Japanese answer by design rather than incidentally.

    Work item #901: the directive was strengthened from a buried mid-list bullet to a
    standalone `_LANGUAGE_DIRECTIVE` appended LAST by `_build_system_instruction`, after
    the weak version drifted under sampling (intermittent wrong-language answers). This
    test now guards the strong version and its position.
    """

    def test_language_directive_constant_is_strong_and_question_pinned(self):
        from coa_serve.tier3.synthesizer import _LANGUAGE_DIRECTIVE

        assert "OUTPUT LANGUAGE" in _LANGUAGE_DIRECTIVE
        assert "language of the user's question" in _LANGUAGE_DIRECTIVE
        # Pins to the QUESTION and tells the model to ignore the context's language.
        assert "IGNORE their" in _LANGUAGE_DIRECTIVE

    def test_weak_buried_bullet_removed_from_base(self):
        from coa_serve.tier3.synthesizer import _SYSTEM_INSTRUCTION_BASE

        assert "- Respond in the same language as the user's question" not in _SYSTEM_INSTRUCTION_BASE

    def test_build_system_instruction_appends_directive_last(self):
        instruction = Synthesizer._build_system_instruction(
            Synthesizer.__new__(Synthesizer), nonce="abc123", marker="MK"
        )
        assert "OUTPUT LANGUAGE" in instruction
        # Must be the LAST section — after the anti-injection SECURITY clause.
        assert instruction.index("OUTPUT LANGUAGE") > instruction.index("SECURITY:")


@pytest.mark.unit
class TestSynthesizerConfidence:
    async def test_extracts_llm_confidence(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer here.\n\n[CONFIDENCE: 0.85]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        chunks = [_make_chunk()]
        entities = [_make_entity()]
        result = await synth.synthesize("What is the SLA?", chunks, entities)
        assert result.confidence == 0.85
        assert "[CONFIDENCE:" not in result.answer

    async def test_falls_back_to_heuristic_no_tag(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer without confidence tag.")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        chunks = [_make_chunk(score=0.8)]
        result = await synth.synthesize("q", chunks, [])
        # 0.8 * 0.8 + 0.0 + 0.1 = 0.74
        assert abs(result.confidence - 0.74) < 0.01

    async def test_no_context_confidence(self, synthesizer):
        result = await synthesizer.synthesize("unknown", [], [])
        assert result.confidence == 0.3

    async def test_graph_only_confidence(self, synthesizer):
        entities = [_make_entity()]
        result = await synthesizer.synthesize("q", [], entities)
        # graph-only: 0.3 + 0.1 = 0.4
        assert result.confidence == 0.4

    async def test_confidence_clamped_above_one(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 1.5]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        result = await synth.synthesize("q", [], [])
        assert result.confidence == 1.0

    async def test_negative_confidence_falls_back(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: -0.3]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        result = await synth.synthesize("q", [], [])
        # Negative value doesn't match regex -> falls back to heuristic (no context = 0.3)
        assert result.confidence == 0.3

    async def test_multiple_confidence_tags_first_wins(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer [CONFIDENCE: 0.8] and more [CONFIDENCE: 0.3]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        result = await synth.synthesize("q", [], [])
        assert result.confidence == 0.8


@pytest.mark.unit
class TestSynthesizerPrompt:
    async def test_strips_confidence_tag_from_answer(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Full answer here.\n\n[CONFIDENCE: 0.75]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        result = await synth.synthesize("q", [], [])
        assert "[CONFIDENCE:" not in result.answer
        assert result.answer == "Full answer here."

    async def test_query_truncated_to_max_length(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Short answer. [CONFIDENCE: 0.5]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        long_query = "x" * 5000
        await synth.synthesize(long_query, [], [])
        guard_content_arg = mock_bedrock.converse.call_args.kwargs["guard_content"]
        assert "x" * 2000 in guard_content_arg
        assert "x" * 2001 not in guard_content_arg

    async def test_includes_structured_data(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        structured = [{"policy_id": "P1", "premium": 500}]
        await synth.synthesize("q", [], [], structured_data=structured)
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "<structured_data>" in prompt_arg or "scl_structured_" in prompt_arg
        assert "P1" in prompt_arg

    async def test_includes_catalog_summary(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        catalog = [
            {"name": "monthly_revenue", "description": "Total revenue", "dimensions": ["month", "region"]},
        ]
        await synth.synthesize("q", [], [], catalog_summary=catalog)
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "<catalog_summary>" in prompt_arg
        assert "monthly_revenue" in prompt_arg
        assert "month, region" in prompt_arg

    async def test_includes_conversation_history(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 0.7]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        history = [
            {"role": "user", "content": "What policies do we have?"},
            {"role": "assistant", "content": "There are 3 active policies."},
        ]
        await synth.synthesize("q", [], [], conversation_history=history)
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "<conversation_history>" in prompt_arg
        assert "3 active policies" in prompt_arg

    async def test_includes_document_context(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        chunks = [_make_chunk(text="Important content", doc_name="Doc A")]
        await synth.synthesize("q", chunks, [])
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "scl_context_" in prompt_arg
        assert "Important" in prompt_arg  # datamarked but word still present
        assert "Doc A" in prompt_arg

    async def test_includes_graph_context(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="Answer. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        entities = [_make_entity(label="Order")]
        await synth.synthesize("q", [], entities)
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "scl_graph_" in prompt_arg
        assert "Order" in prompt_arg

    async def test_no_context_prompt(self, mock_bedrock):
        mock_bedrock.converse.return_value = ConverseResult(text="No context. [CONFIDENCE: 0.3]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        await synth.synthesize("q", [], [])
        prompt_arg = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "(No context retrieved)" in prompt_arg


@pytest.mark.unit
class TestSynthesizerStreaming:
    """The confidence marker must never reach the client via streamed tokens."""

    def _stream_bedrock(self, tokens):
        client = AsyncMock()

        async def _gen(*args, **kwargs):
            for t in tokens:
                yield t

        client.converse_stream = _gen
        return client

    async def test_confidence_tag_suppressed_from_stream(self):
        # Marker split across tokens at the end — must not be emitted.
        bedrock = self._stream_bedrock(["Answer text.", "\n\n[CONFID", "ENCE: 0.", "7]"])
        synth = Synthesizer(bedrock, guardrail_id="gr-test")
        emitted: list[str] = []

        async def on_token(t: str) -> None:
            emitted.append(t)

        result = await synth.synthesize_stream("q", [_make_chunk()], [], on_token=on_token)
        streamed = "".join(emitted)
        assert "[CONFIDENCE" not in streamed, f"marker leaked to stream: {streamed!r}"
        assert "Answer text." in streamed
        assert "[CONFIDENCE" not in result.answer
        assert result.confidence == pytest.approx(0.7)

    async def test_real_bracket_link_preserved_in_stream(self):
        bedrock = self._stream_bedrock(["See ", "[docs]", " here. ", "[CONFIDENCE: 0.5]"])
        synth = Synthesizer(bedrock, guardrail_id="gr-test")
        emitted: list[str] = []

        async def on_token(t: str) -> None:
            emitted.append(t)

        await synth.synthesize_stream("q", [_make_chunk()], [], on_token=on_token)
        streamed = "".join(emitted)
        assert "[docs]" in streamed
        assert "[CONFIDENCE" not in streamed

    async def test_text_after_marker_in_same_token_is_preserved(self):
        # Regression: a single token carrying both the complete marker AND
        # trailing prose. The marker regex isn't end-anchored, so clearing the
        # whole buffer would drop " Thanks!". Only the marker span may be removed.
        bedrock = self._stream_bedrock(["Answer text. [CONFIDENCE: 0.8] Thanks!"])
        synth = Synthesizer(bedrock, guardrail_id="gr-test")
        emitted: list[str] = []

        async def on_token(t: str) -> None:
            emitted.append(t)

        result = await synth.synthesize_stream("q", [_make_chunk()], [], on_token=on_token)
        streamed = "".join(emitted)
        assert "[CONFIDENCE" not in streamed, f"marker leaked: {streamed!r}"
        assert "Answer text." in streamed
        assert "Thanks!" in streamed, f"trailing text dropped: {streamed!r}"
        # The streamed text must match the non-streamed answer (no divergence).
        assert result.answer == streamed
        assert result.confidence == pytest.approx(0.8)

    async def test_text_after_marker_at_end_of_stream_is_preserved(self):
        # Same hazard at the end-of-stream flush: marker split across tokens with
        # trailing text arriving in the final token, so the buffer still holds
        # "[CONFIDENCE: 0.9] done" when the stream ends.
        bedrock = self._stream_bedrock(["Body ", "[CONFIDENCE: 0.", "9] done"])
        synth = Synthesizer(bedrock, guardrail_id="gr-test")
        emitted: list[str] = []

        async def on_token(t: str) -> None:
            emitted.append(t)

        result = await synth.synthesize_stream("q", [_make_chunk()], [], on_token=on_token)
        streamed = "".join(emitted)
        assert "[CONFIDENCE" not in streamed, f"marker leaked: {streamed!r}"
        assert "Body " in streamed
        assert "done" in streamed, f"trailing text dropped: {streamed!r}"
        assert result.answer == streamed
        assert result.confidence == pytest.approx(0.9)

    async def test_answer_ending_in_marker_streams_full_body(self):
        # The common well-behaved case (marker last, nothing after it) must
        # still emit the entire answer body and drop only the marker.
        bedrock = self._stream_bedrock(["The filing process is X.", "\n[CONFIDENCE: 0.6]"])
        synth = Synthesizer(bedrock, guardrail_id="gr-test")
        emitted: list[str] = []

        async def on_token(t: str) -> None:
            emitted.append(t)

        result = await synth.synthesize_stream("q", [_make_chunk()], [], on_token=on_token)
        streamed = "".join(emitted)
        assert "[CONFIDENCE" not in streamed
        assert "The filing process is X." in streamed
        assert result.answer == streamed.strip() or result.answer == streamed


@pytest.mark.unit
class TestStructuredRowBudget:
    """TIER3_SYNTH_MAX_ROWS lifts the hardcoded 20-row synthesis budget.

    The fixed cap silently discarded rows beyond the first 20, so a multi-day
    x multi-country result reached synthesis as a sliver and the model
    reported the retrieved remainder as missing data."""

    @pytest.mark.asyncio
    async def test_default_caps_at_twenty(self, mock_bedrock, monkeypatch):
        monkeypatch.delenv("TIER3_SYNTH_MAX_ROWS", raising=False)
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        rows = [{"i": f"row{i:03d}"} for i in range(30)]
        await synth.synthesize("q", [], [], structured_data=rows)
        prompt = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "row019" in prompt and "row020" not in prompt

    @pytest.mark.asyncio
    async def test_env_raises_budget(self, mock_bedrock, monkeypatch):
        monkeypatch.setenv("TIER3_SYNTH_MAX_ROWS", "500")
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        rows = [{"i": f"row{i:03d}"} for i in range(98)]
        await synth.synthesize("q", [], [], structured_data=rows)
        prompt = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "row097" in prompt

    @pytest.mark.asyncio
    async def test_invalid_env_falls_back(self, mock_bedrock, monkeypatch):
        monkeypatch.setenv("TIER3_SYNTH_MAX_ROWS", "banana")
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        rows = [{"i": f"row{i:03d}"} for i in range(30)]
        await synth.synthesize("q", [], [], structured_data=rows)
        prompt = mock_bedrock.converse.call_args.kwargs["prompt"]
        assert "row019" in prompt and "row020" not in prompt


@pytest.mark.unit
class TestSynthesisTokenBudget:
    """TIER3_SYNTH_MAX_TOKENS lifts the synthesis output budget (default 4096).

    Reasoning-class models spend output tokens on reasoning before the visible
    answer, so the client default truncated long tabular answers mid-table."""

    @pytest.mark.asyncio
    async def test_default_passes_4096(self, mock_bedrock, monkeypatch):
        monkeypatch.delenv("TIER3_SYNTH_MAX_TOKENS", raising=False)
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        await synth.synthesize("q", [], [])
        assert mock_bedrock.converse.call_args.kwargs["max_tokens"] == 4096

    @pytest.mark.asyncio
    async def test_env_raises_budget(self, mock_bedrock, monkeypatch):
        monkeypatch.setenv("TIER3_SYNTH_MAX_TOKENS", "12000")
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        await synth.synthesize("q", [], [])
        assert mock_bedrock.converse.call_args.kwargs["max_tokens"] == 12000

    @pytest.mark.asyncio
    async def test_env_cannot_shrink_below_default(self, mock_bedrock, monkeypatch):
        monkeypatch.setenv("TIER3_SYNTH_MAX_TOKENS", "100")
        mock_bedrock.converse.return_value = ConverseResult(text="A. [CONFIDENCE: 0.9]")
        synth = Synthesizer(mock_bedrock, guardrail_id="gr-123")
        await synth.synthesize("q", [], [])
        assert mock_bedrock.converse.call_args.kwargs["max_tokens"] == 4096
