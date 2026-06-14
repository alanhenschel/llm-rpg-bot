"""Unit tests for app.idempotency.IdempotencyStore.

All Redis calls are intercepted via AsyncMock; no real Redis required.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.idempotency import IdempotencyStore


@pytest.fixture()
def store(mock_redis):
    """IdempotencyStore with a mocked Redis client injected."""
    with patch("app.idempotency.aioredis.from_url", return_value=mock_redis):
        s = IdempotencyStore()
    s._redis = mock_redis
    return s


async def test_claim_returns_true_on_first_call(store, mock_redis):
    mock_redis.set.return_value = True
    result = await store.claim("evt-001")
    assert result is True
    mock_redis.set.assert_awaited_once_with(
        "llmbot:processed:evt-001", "1", nx=True, ex=pytest.approx(86400, abs=1)
    )


async def test_claim_returns_false_on_duplicate(store, mock_redis):
    # Redis SET NX returns None when the key already exists.
    mock_redis.set.return_value = None
    result = await store.claim("evt-001")
    assert result is False


async def test_claim_uses_correct_key_prefix(store, mock_redis):
    mock_redis.set.return_value = True
    await store.claim("abc-123")
    key_used = mock_redis.set.call_args[0][0]
    assert key_used == "llmbot:processed:abc-123"


async def test_release_deletes_correct_key(store, mock_redis):
    await store.release("evt-002")
    mock_redis.delete.assert_awaited_once_with("llmbot:processed:evt-002")


async def test_release_different_event_ids_use_different_keys(store, mock_redis):
    await store.release("A")
    await store.release("B")
    calls = [c[0][0] for c in mock_redis.delete.await_args_list]
    assert "llmbot:processed:A" in calls
    assert "llmbot:processed:B" in calls


async def test_ping_returns_true_when_redis_responds(store, mock_redis):
    mock_redis.ping.return_value = True
    result = await store.ping()
    assert result is True


async def test_ping_returns_false_when_redis_raises(store, mock_redis):
    mock_redis.ping.side_effect = Exception("connection refused")
    result = await store.ping()
    assert result is False


async def test_aclose_delegates_to_redis(store, mock_redis):
    await store.aclose()
    mock_redis.aclose.assert_awaited_once()
