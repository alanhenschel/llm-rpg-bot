"""PostgreSQL access for analytics (asyncpg pool).

Reads the message_logs and whatsapp_connections tables written by the Go gateway.
"""
from __future__ import annotations

import asyncpg

from app.config import settings
from app.logging_setup import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        # asyncpg wants a postgres:// DSN without the +driver suffix.
        dsn = settings.database_url.replace("postgresql+asyncpg", "postgresql")
        self._pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        logger.info("database pool created")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def ping(self) -> bool:
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:  # noqa: BLE001
            return False

    async def messages_per_hour(self) -> list[dict]:
        """Message count grouped by hour for the current day."""
        rows = await self._pool.fetch(
            """
            SELECT date_trunc('hour', created_at) AS hour,
                   direction,
                   count(*) AS count
            FROM message_logs
            WHERE created_at >= date_trunc('day', now())
            GROUP BY 1, 2
            ORDER BY 1
            """
        )
        return [
            {"hour": r["hour"].isoformat(), "direction": r["direction"], "count": r["count"]}
            for r in rows
        ]

    async def messages_bytes_detail(self) -> list[dict]:
        """Per-message byte sizes for today (for the bytes/message chart)."""
        rows = await self._pool.fetch(
            """
            SELECT id, jid, direction, bytes, created_at
            FROM message_logs
            WHERE created_at >= date_trunc('day', now())
            ORDER BY created_at
            LIMIT 1000
            """
        )
        return [
            {
                "id": r["id"],
                "jid": r["jid"],
                "direction": r["direction"],
                "bytes": r["bytes"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]

    async def bytes_today_by_connection(self) -> dict[int, int]:
        """Total bytes transferred today per connection id (for dashboard table)."""
        rows = await self._pool.fetch(
            """
            SELECT connection_id, COALESCE(sum(bytes), 0) AS total
            FROM message_logs
            WHERE created_at >= date_trunc('day', now()) AND connection_id IS NOT NULL
            GROUP BY connection_id
            """
        )
        return {r["connection_id"]: r["total"] for r in rows}

    async def connection_stats(self) -> list[dict]:
        """Connection rows + uptime proxy (last_seen recency) from the DB."""
        rows = await self._pool.fetch(
            """
            SELECT id, COALESCE(jid, '') AS jid, label, COALESCE(pod_id, '') AS pod_id,
                   status, last_seen, created_at,
                   EXTRACT(EPOCH FROM (now() - created_at))::bigint AS age_seconds
            FROM whatsapp_connections
            ORDER BY id
            """
        )
        return [
            {
                "id": r["id"],
                "jid": r["jid"],
                "label": r["label"],
                "pod_id": r["pod_id"],
                "status": r["status"],
                "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                "age_seconds": r["age_seconds"],
            }
            for r in rows
        ]


db = Database()
