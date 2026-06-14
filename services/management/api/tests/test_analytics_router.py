"""Unit tests for /api/analytics/* endpoints.

Uses FastAPI TestClient with the Database singleton patched to return
controlled fixtures — no real PostgreSQL required.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

NOW = datetime(2024, 6, 13, 12, 0, 0, tzinfo=timezone.utc)

MESSAGES_FIXTURE = [
    {"hour": NOW.isoformat(), "direction": "inbound", "count": 10},
    {"hour": NOW.isoformat(), "direction": "outbound", "count": 5},
]

BYTES_FIXTURE = [
    {"id": 1, "jid": "a@s.net", "direction": "inbound", "bytes": 256, "created_at": NOW.isoformat()},
]

CONN_STATS_FIXTURE = [
    {
        "id": 1,
        "jid": "a@s.net",
        "label": "main",
        "pod_id": "gw-1",
        "status": "connected",
        "last_seen": NOW.isoformat(),
        "age_seconds": 3600,
    }
]


@pytest.fixture()
def analytics_client(db_with_pool):
    database, pool, _ = db_with_pool
    database.messages_per_hour = AsyncMock(return_value=MESSAGES_FIXTURE)
    database.messages_bytes_detail = AsyncMock(return_value=BYTES_FIXTURE)
    database.connection_stats = AsyncMock(return_value=CONN_STATS_FIXTURE)

    from fastapi.testclient import TestClient
    with patch("app.db.queries.db", database), \
         patch("app.routers.analytics.db", database), \
         patch("app.kafka.producer.command_producer", new_callable=lambda: lambda: AsyncMock()), \
         patch("app.main.CommandProducer", return_value=MagicMock()):
        from app.main import app
        with TestClient(app, raise_server_exceptions=True) as client:
            yield client, database


def test_messages_per_hour_returns_200(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/messages")
    assert resp.status_code == 200


def test_messages_per_hour_response_has_data_key(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/messages")
    body = resp.json()
    assert "data" in body


def test_messages_per_hour_data_matches_fixture(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/messages")
    assert resp.json()["data"] == MESSAGES_FIXTURE


def test_messages_bytes_detail_returns_200(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/bytes")
    assert resp.status_code == 200


def test_messages_bytes_detail_response_has_data_key(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/bytes")
    assert "data" in resp.json()


def test_messages_bytes_detail_data_matches_fixture(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/bytes")
    assert resp.json()["data"] == BYTES_FIXTURE


def test_connection_uptime_returns_200(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/connections")
    assert resp.status_code == 200


def test_connection_uptime_response_has_data_key(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/connections")
    assert "data" in resp.json()


def test_connection_uptime_data_matches_fixture(analytics_client):
    client, _ = analytics_client
    resp = client.get("/api/analytics/connections")
    assert resp.json()["data"] == CONN_STATS_FIXTURE


def test_messages_endpoint_returns_empty_list_when_db_empty(analytics_client):
    client, database = analytics_client
    database.messages_per_hour = AsyncMock(return_value=[])
    resp = client.get("/api/analytics/messages")
    assert resp.json() == {"data": []}
