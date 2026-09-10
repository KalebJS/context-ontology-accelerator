"""Unit tests for transient-failure retries in coa_common.ollama_llm."""

from __future__ import annotations

import httpx
import pytest


class _Resp:
    def __init__(self, status: int) -> None:
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(f"{self.status_code}", request=None, response=None)

    def json(self) -> dict:
        return {"choices": [{"message": {"content": "ok"}}]}


def _make(monkeypatch, statuses, base_url="http://x:11434", *, max_retries="3", backoff="0", cap=None):
    """Build the adapter; _chat_completion will see the given responses/exceptions in order.

    Resets the module's cached adapter class so every build re-reads the retry
    env vars (the class is cached process-wide for pickle; the closure captures
    retry knobs at build time).
    """
    import coa_common.ollama_llm as mod

    monkeypatch.setenv("OLLAMA_BASE_URL", base_url)
    monkeypatch.setenv("OLLAMA_CHAT_MODEL", "test-model")
    monkeypatch.setenv("OLLAMA_MAX_RETRIES", max_retries)
    monkeypatch.setenv("OLLAMA_RETRY_BACKOFF_S", backoff)
    if cap is None:
        monkeypatch.delenv("OLLAMA_RETRY_CAP_S", raising=False)
    else:
        monkeypatch.setenv("OLLAMA_RETRY_CAP_S", cap)
    monkeypatch.setattr(mod, "_OLLAMA_LLAMA_ADAPTER_CLS", None)

    calls = {"n": 0}
    sleeps: list[float] = []

    def _post(url, json=None, timeout=None):  # noqa: A002
        calls["n"] += 1
        item = statuses[min(calls["n"] - 1, len(statuses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(httpx, "post", _post)
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    llm = mod.make_ollama_llama_index_llm()
    return llm, calls, sleeps


def test_retries_on_503_then_succeeds(monkeypatch):
    llm, calls, sleeps = _make(monkeypatch, [_Resp(503), _Resp(200)])
    out = llm.complete("hello")
    assert out.text == "ok"
    assert calls["n"] == 2
    assert len(sleeps) == 1


def test_retries_on_429_and_500_and_502_and_504(monkeypatch):
    # Every transient status in the retry set is retried; the final 200 wins.
    llm, calls, sleeps = _make(
        monkeypatch, [_Resp(429), _Resp(500), _Resp(502), _Resp(504), _Resp(200)], max_retries="5"
    )
    out = llm.complete("hello")
    assert out.text == "ok"
    assert calls["n"] == 5
    assert len(sleeps) == 4


def test_exhausts_retries_and_raises_last_error(monkeypatch):
    err = httpx.ConnectTimeout("boom")
    llm, calls, sleeps = _make(monkeypatch, [err, err, err])
    with pytest.raises(httpx.ConnectTimeout):
        llm.complete("hello")
    assert calls["n"] == 3
    assert len(sleeps) == 2  # backoff between attempts, none after the last


def test_exhausts_retries_on_persistent_503(monkeypatch):
    # The incident shape: every attempt gets 503. The FINAL response's error
    # must propagate (raise_for_status on the last response), not be swallowed.
    llm, calls, sleeps = _make(monkeypatch, [_Resp(503), _Resp(503), _Resp(503)])
    with pytest.raises(httpx.HTTPStatusError):
        llm.complete("hello")
    assert calls["n"] == 3
    assert len(sleeps) == 2


def test_no_retry_on_client_error(monkeypatch):
    llm, calls, sleeps = _make(monkeypatch, [_Resp(400)])
    with pytest.raises(httpx.HTTPStatusError):
        llm.complete("hello")
    assert calls["n"] == 1


def test_no_retry_on_429_when_max_attempts_is_one(monkeypatch):
    # OLLAMA_MAX_RETRIES=1 → single attempt; a 429 must raise, not sleep/retry.
    llm, calls, sleeps = _make(monkeypatch, [_Resp(429)], max_retries="1")
    with pytest.raises(httpx.HTTPStatusError):
        llm.complete("hello")
    assert calls["n"] == 1
    assert sleeps == []


def test_backoff_respects_env_knobs(monkeypatch):
    # base=3, cap=7: delays are 3, 6 (uncapped), then capped at 7 (+ jitter on
    # the uncapped component only). Every recorded sleep must respect the cap,
    # and the doubling progression must be visible.
    statuses = [_Resp(503)] * 7 + [_Resp(200)]
    llm, calls, sleeps = _make(monkeypatch, statuses, max_retries="8", backoff="3", cap="7")
    llm.complete("hello")
    assert calls["n"] == 8
    assert len(sleeps) == 7
    assert all(s <= 7.0 for s in sleeps)
    # First retry delay == base exactly (jitter draws from [0, base*0.25) and
    # min(cap, ...) cannot lower it while cap >= base).
    assert sleeps[0] == pytest.approx(3.0, abs=0.75)
    # Cap actually engaged at least once (delays 3,6,7+jitter... the 4th+ would
    # exceed 7 uncapped).
    assert any(s > 6.0 for s in sleeps[2:])


def test_defaults_are_five_attempts_two_second_base(monkeypatch):
    # Unset env → 5 total attempts (2s base / 30s cap defaults; sleep mocked).
    import coa_common.ollama_llm as mod

    monkeypatch.delenv("OLLAMA_MAX_RETRIES", raising=False)
    monkeypatch.delenv("OLLAMA_RETRY_BACKOFF_S", raising=False)
    monkeypatch.delenv("OLLAMA_RETRY_CAP_S", raising=False)
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://x:11434")
    monkeypatch.setenv("OLLAMA_CHAT_MODEL", "test-model")
    monkeypatch.setattr(mod, "_OLLAMA_LLAMA_ADAPTER_CLS", None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    attempts: dict[str, int] = {"n": 0}

    def _post(url, json=None, timeout=None):  # noqa: A002
        attempts["n"] += 1
        return _Resp(503)

    monkeypatch.setattr(httpx, "post", _post)
    llm = mod.make_ollama_llama_index_llm()
    with pytest.raises(httpx.HTTPStatusError):
        llm.complete("hello")
    assert attempts["n"] == 5
