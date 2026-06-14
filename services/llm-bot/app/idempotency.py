"""Redis-backed idempotency guard.

Uses SET key value NX EX ttl: if the key already exists the message was already
processed and we skip it, preventing duplicate LLM responses on Kafka redelivery.
"""
from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings
from app.telemetry.logging import get_logger

logger = get_logger(__name__)


class IdempotencyStore:
    def __init__(self) -> None:
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)

    async def claim(self, event_id: str) -> bool:
        """Return True if this is the first time we see event_id (claim succeeded),
        False if it was already processed."""
        key = f"llmbot:processed:{event_id}"
        # SET NX returns True only if the key did not exist.
        was_set = await self._redis.set(key, "1", nx=True, ex=settings.idempotency_ttl_seconds)
        return bool(was_set)

    async def release(self, event_id: str) -> None:
        """Release a claim (used when processing failed and should be retried)."""
        await self._redis.delete(f"llmbot:processed:{event_id}")

    async def ping(self) -> bool:
        try:
            return await self._redis.ping()
        except Exception:  # noqa: BLE001
            return False

    async def aclose(self) -> None:
        await self._redis.aclose()
