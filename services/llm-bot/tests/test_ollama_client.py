"""Unit tests for app.llm.ollama_client.

OllamaClient wraps an httpx.AsyncClient. Tests replace the underlying client
with AsyncMock to avoid any network calls.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest

from app.llm.ollama_client import RELEVANCE_FLOOR, OllamaClient, build_prompt
from app.rag.pipeline import RetrievedChunk


# ---------------------------------------------------------------------------
# build_prompt — pure function
# ---------------------------------------------------------------------------


def test_build_prompt_includes_relevant_chunks():
    chunks = [
        RetrievedChunk(text="Dovahkiin is dragonborn.", source="skyrim.md", score=0.9),
        RetrievedChunk(text="Geralt hunts monsters.", source="witcher3.md", score=0.8),
    ]
    prompt = build_prompt("Who is the dragonborn?", chunks)
    assert "Dovahkiin is dragonborn." in prompt
    assert "skyrim.md" in prompt
    assert "User question: Who is the dragonborn?" in prompt


def test_build_prompt_excludes_chunks_below_relevance_floor():
    chunks = [
        RetrievedChunk(text="Relevant chunk.", source="skyrim.md", score=RELEVANCE_FLOOR + 0.1),
        RetrievedChunk(text="Irrelevant chunk.", source="fallout4.md", score=RELEVANCE_FLOOR - 0.1),
    ]
    prompt = build_prompt("question", chunks)
    assert "Relevant chunk." in prompt
    assert "Irrelevant chunk." not in prompt


def test_build_prompt_uses_no_context_message_when_all_chunks_irrelevant():
    chunks = [
        RetrievedChunk(text="off-topic text", source="unknown.md", score=0.0),
    ]
    prompt = build_prompt("random question", chunks)
    assert "No relevant information found" in prompt


def test_build_prompt_uses_no_context_message_when_chunks_empty():
    prompt = build_prompt("anything", [])
    assert "No relevant information found" in prompt


def test_build_prompt_includes_system_prompt():
    prompt = build_prompt("hi", [])
    assert "RPG video games" in prompt
    assert "Answer ONLY" in prompt


def test_build_prompt_includes_answer_label():
    prompt = build_prompt("hi", [])
    assert "Answer:" in prompt


def test_build_prompt_respects_exact_relevance_floor_boundary():
    chunks = [RetrievedChunk(text="boundary chunk", source="s.md", score=RELEVANCE_FLOOR)]
    prompt = build_prompt("q", chunks)
    assert "boundary chunk" in prompt


# ---------------------------------------------------------------------------
# OllamaClient.generate — success and retry paths
# ---------------------------------------------------------------------------


@pytest.fixture()
def ollama():
    """OllamaClient with a mocked inner httpx.AsyncClient."""
    with patch("app.llm.ollama_client.httpx.AsyncClient") as mock_cls:
        mock_inner = AsyncMock()
        mock_cls.return_value = mock_inner
        client = OllamaClient()
        client._client = mock_inner
    return client


def _make_response(response_text: str) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"response": response_text}
    resp.raise_for_status = MagicMock()
    return resp


async def test_generate_returns_response_on_success(ollama):
    ollama._client.post = AsyncMock(return_value=_make_response("Skyrim is great!"))

    result = await ollama.generate("tell me about skyrim")

    assert result == "Skyrim is great!"


async def test_generate_strips_whitespace_from_response(ollama):
    ollama._client.post = AsyncMock(return_value=_make_response("  answer  \n"))

    result = await ollama.generate("q")

    assert result == "answer"


async def test_generate_retries_twice_then_succeeds(ollama):
    """Two failures followed by a success should return the successful response."""
    good_resp = _make_response("eventual answer")
    ollama._client.post = AsyncMock(
        side_effect=[
            httpx.ConnectError("timeout"),
            httpx.ConnectError("timeout"),
            good_resp,
        ]
    )

    with patch("asyncio.sleep", new=AsyncMock()):
        result = await ollama.generate("q", trace_id="t1", event_id="e1")

    assert result == "eventual answer"
    assert ollama._client.post.call_count == 3


async def test_generate_returns_fallback_after_three_failures(ollama):
    """Exhausting all 3 retries triggers graceful degradation fallback."""
    ollama._client.post = AsyncMock(side_effect=httpx.ConnectError("down"))

    with patch("asyncio.sleep", new=AsyncMock()):
        result = await ollama.generate("q")

    assert "trouble reaching" in result.lower()
    assert ollama._client.post.call_count == 3


async def test_generate_retries_on_http_status_error(ollama):
    resp_error = MagicMock()
    resp_error.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    resp_good = _make_response("ok after retry")
    ollama._client.post = AsyncMock(side_effect=[resp_error, resp_good])

    with patch("asyncio.sleep", new=AsyncMock()):
        result = await ollama.generate("q")

    assert result == "ok after retry"


async def test_generate_applies_exponential_backoff(ollama):
    """All 3 attempts fail → sleep between attempts only: 2^0=1, 2^1=2 (no sleep after final)."""
    ollama._client.post = AsyncMock(side_effect=httpx.ConnectError("x"))

    sleep_calls = []
    async def fake_sleep(secs):
        sleep_calls.append(secs)

    with patch("asyncio.sleep", new=fake_sleep):
        await ollama.generate("q")

    assert sleep_calls == [1, 2]


async def test_generate_empty_response_field_returns_empty_string(ollama):
    resp = MagicMock()
    resp.json.return_value = {}
    resp.raise_for_status = MagicMock()
    ollama._client.post = AsyncMock(return_value=resp)

    result = await ollama.generate("q")
    assert result == ""


async def test_aclose_delegates_to_inner_client(ollama):
    ollama._client.aclose = AsyncMock()
    await ollama.aclose()
    ollama._client.aclose.assert_awaited_once()
