"""Shared fixtures for management-api unit tests.

asyncpg, confluent_kafka, and pythonjsonlogger are stubbed at import time so
tests run without any running infrastructure or heavy native extensions.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub native/optional dependencies before any app module is imported.
# ---------------------------------------------------------------------------

_asyncpg_stub = MagicMock()
_asyncpg_stub.Pool = MagicMock
_asyncpg_stub.create_pool = AsyncMock()

_confluent_stub = MagicMock()
_confluent_stub.Producer.return_value = MagicMock()

sys.modules.setdefault("asyncpg", _asyncpg_stub)
sys.modules.setdefault("confluent_kafka", _confluent_stub)

if "pythonjsonlogger" not in sys.modules:
    # Provide a real class (not object) so _SchemaFormatter.__init__ can call super().__init__().
    class _FakeJsonFormatter(logging.Formatter):
        def __init__(self, *a, rename_fields=None, timestamp=None, **kw):
            super().__init__()

        def add_fields(self, log_record, record, message_dict):
            pass

    _jl_mod = MagicMock()
    _jl_mod.JsonFormatter = _FakeJsonFormatter
    sys.modules["pythonjsonlogger"] = MagicMock(jsonlogger=_jl_mod)
    sys.modules["pythonjsonlogger.jsonlogger"] = _jl_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime(2024, 6, 13, 12, 0, 0, tzinfo=timezone.utc)


def _make_row(**kw) -> MagicMock:
    """Create a dict-like asyncpg Record stub."""
    row = MagicMock()
    row.__getitem__ = lambda self, k: kw[k]
    row.get = lambda k, d=None: kw.get(k, d)
    for k, v in kw.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# Pool / connection fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_pool():
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)

    # asyncpg pool.acquire() is an async context manager.
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=acquire_ctx)
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchval = AsyncMock(return_value=None)
    return pool, conn


@pytest.fixture()
def db_with_pool(mock_pool):
    """Database instance with pool injected, bypassing asyncpg.create_pool."""
    from app.db.queries import Database

    pool, conn = mock_pool
    database = Database()
    database._pool = pool
    return database, pool, conn


# ---------------------------------------------------------------------------
# FastAPI TestClient fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def test_client(db_with_pool):
    """FastAPI TestClient that bypasses DB connect/disconnect lifecycle events."""
    database, pool, _ = db_with_pool
    from fastapi.testclient import TestClient

    with patch("app.db.queries.db", database):
        from app.main import app
        with TestClient(app, raise_server_exceptions=True) as client:
            yield client, database
