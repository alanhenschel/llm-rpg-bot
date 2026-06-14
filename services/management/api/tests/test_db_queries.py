"""Unit tests for app.db.queries.Database.

All asyncpg interactions are mocked via the pool fixture from conftest.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.db.queries import Database

NOW = datetime(2024, 6, 13, 12, 0, 0, tzinfo=timezone.utc)


def _row(**kw):
    r = MagicMock()
    r.__getitem__ = lambda self, k: kw[k]
    for k, v in kw.items():
        setattr(r, k, v)
    return r


# ---------------------------------------------------------------------------
# ping()
# ---------------------------------------------------------------------------


async def test_ping_returns_true_when_pool_responds(db_with_pool):
    database, pool, conn = db_with_pool
    conn.fetchval = AsyncMock(return_value=1)
    result = await database.ping()
    assert result is True


async def test_ping_returns_false_when_pool_is_none():
    database = Database()
    database._pool = None
    result = await database.ping()
    assert result is False


async def test_ping_returns_false_when_db_raises(db_with_pool):
    database, pool, conn = db_with_pool
    conn.fetchval = AsyncMock(side_effect=Exception("pg down"))
    result = await database.ping()
    assert result is False


# ---------------------------------------------------------------------------
# messages_per_hour()
# ---------------------------------------------------------------------------


async def test_messages_per_hour_formats_iso_timestamp(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(hour=NOW, direction="inbound", count=5),
            _row(hour=NOW, direction="outbound", count=3),
        ]
    )
    result = await database.messages_per_hour()

    assert len(result) == 2
    assert result[0]["hour"] == NOW.isoformat()
    assert result[0]["direction"] == "inbound"
    assert result[0]["count"] == 5


async def test_messages_per_hour_returns_empty_list_when_no_rows(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(return_value=[])
    result = await database.messages_per_hour()
    assert result == []


async def test_messages_per_hour_returns_correct_keys(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[_row(hour=NOW, direction="inbound", count=1)]
    )
    result = await database.messages_per_hour()
    assert set(result[0].keys()) == {"hour", "direction", "count"}


# ---------------------------------------------------------------------------
# messages_bytes_detail()
# ---------------------------------------------------------------------------


async def test_messages_bytes_detail_returns_expected_fields(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(id=1, jid="abc@s.net", direction="inbound", bytes=128, created_at=NOW)
        ]
    )
    result = await database.messages_bytes_detail()
    assert result[0] == {
        "id": 1,
        "jid": "abc@s.net",
        "direction": "inbound",
        "bytes": 128,
        "created_at": NOW.isoformat(),
    }


async def test_messages_bytes_detail_empty_returns_empty_list(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(return_value=[])
    result = await database.messages_bytes_detail()
    assert result == []


# ---------------------------------------------------------------------------
# bytes_today_by_connection()
# ---------------------------------------------------------------------------


async def test_bytes_today_by_connection_returns_dict_keyed_by_connection_id(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(connection_id=1, total=1024),
            _row(connection_id=2, total=512),
        ]
    )
    result = await database.bytes_today_by_connection()
    assert result == {1: 1024, 2: 512}


async def test_bytes_today_by_connection_empty_returns_empty_dict(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(return_value=[])
    result = await database.bytes_today_by_connection()
    assert result == {}


# ---------------------------------------------------------------------------
# connection_stats()
# ---------------------------------------------------------------------------


async def test_connection_stats_returns_expected_keys(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(
                id=1,
                jid="555@s.net",
                label="main",
                pod_id="gateway-1",
                status="connected",
                last_seen=NOW,
                created_at=NOW,
                age_seconds=3600,
            )
        ]
    )
    result = await database.connection_stats()
    assert len(result) == 1
    row = result[0]
    assert set(row.keys()) == {"id", "jid", "label", "pod_id", "status", "last_seen", "age_seconds"}


async def test_connection_stats_serializes_last_seen_as_iso(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(
                id=1, jid="j", label="l", pod_id="p", status="s",
                last_seen=NOW, created_at=NOW, age_seconds=0,
            )
        ]
    )
    result = await database.connection_stats()
    assert result[0]["last_seen"] == NOW.isoformat()


async def test_connection_stats_null_last_seen_produces_none(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(
        return_value=[
            _row(
                id=1, jid="j", label="l", pod_id="p", status="s",
                last_seen=None, created_at=NOW, age_seconds=100,
            )
        ]
    )
    result = await database.connection_stats()
    assert result[0]["last_seen"] is None


async def test_connection_stats_empty_returns_empty_list(db_with_pool):
    database, pool, _ = db_with_pool
    pool.fetch = AsyncMock(return_value=[])
    result = await database.connection_stats()
    assert result == []
