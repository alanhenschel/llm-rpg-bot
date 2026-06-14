"""Unit tests for /api/connections/* endpoints.

Tests cover:
- GET /api/connections: happy path merging live + DB data
- GET /api/connections: gateway unreachable (gateway_up=False)
- POST /api/connections/{id}/disconnect: success path returns trace_id
- POST /api/connections/{id}/disconnect: 404 when connection not found
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

NOW = datetime(2024, 6, 13, 12, 0, 0, tzinfo=timezone.utc)

DB_CONN_ROWS = [
    {
        "id": 1,
        "jid": "555@s.net",
        "label": "main",
        "pod_id": "gw-1",
        "status": "connected",
        "last_seen": NOW.isoformat(),
        "age_seconds": 100,
    },
    {
        "id": 2,
        "jid": "777@s.net",
        "label": "secondary",
        "pod_id": "gw-2",
        "status": "disconnected",
        "last_seen": None,
        "age_seconds": 9999,
    },
]

GATEWAY_LIVE = {
    "connections": [
        {"id": 1, "jid": "555@s.net", "status": "connected", "bytes_in": 1024, "bytes_out": 512},
    ]
}

BYTES_BY_CONN = {1: 2048, 2: 0}


@pytest.fixture()
def conn_client(db_with_pool):
    database, pool, _ = db_with_pool
    database.connection_stats = AsyncMock(return_value=DB_CONN_ROWS)
    database.bytes_today_by_connection = AsyncMock(return_value=BYTES_BY_CONN)

    mock_producer = MagicMock()
    mock_producer.disconnect = MagicMock(return_value=str(uuid.uuid4()))

    from fastapi.testclient import TestClient
    with patch("app.db.queries.db", database), \
         patch("app.routers.connections.db", database), \
         patch("app.routers.analytics.db", database), \
         patch("app.kafka.producer.command_producer", mock_producer), \
         patch("app.main.CommandProducer", return_value=mock_producer):
        from app.main import app
        with TestClient(app, raise_server_exceptions=True) as client:
            yield client, database, mock_producer


# ---------------------------------------------------------------------------
# GET /api/connections — gateway reachable
# ---------------------------------------------------------------------------


def test_list_connections_returns_200(conn_client):
    client, database, _ = conn_client

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = GATEWAY_LIVE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(return_value=mock_resp)
        resp = client.get("/api/connections")

    assert resp.status_code == 200


def test_list_connections_gateway_up_true_when_gateway_responds(conn_client):
    client, _, _ = conn_client

    mock_resp = MagicMock()
    mock_resp.json.return_value = GATEWAY_LIVE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(return_value=mock_resp)
        body = client.get("/api/connections").json()

    assert body["gateway_up"] is True


def test_list_connections_merges_live_data_with_db(conn_client):
    client, _, _ = conn_client

    mock_resp = MagicMock()
    mock_resp.json.return_value = GATEWAY_LIVE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(return_value=mock_resp)
        body = client.get("/api/connections").json()

    conn1 = next(c for c in body["connections"] if c["id"] == 1)
    # bytes_in/bytes_out come from gateway live data
    assert conn1["bytes_in"] == 1024
    assert conn1["bytes_out"] == 512
    # bytes_today from DB
    assert conn1["bytes_today"] == 2048
    assert conn1["live"] is True


def test_list_connections_marks_db_only_connections_as_not_live(conn_client):
    client, _, _ = conn_client

    mock_resp = MagicMock()
    mock_resp.json.return_value = GATEWAY_LIVE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(return_value=mock_resp)
        body = client.get("/api/connections").json()

    conn2 = next(c for c in body["connections"] if c["id"] == 2)
    assert conn2["live"] is False


def test_list_connections_count_equals_db_rows(conn_client):
    client, _, _ = conn_client

    mock_resp = MagicMock()
    mock_resp.json.return_value = GATEWAY_LIVE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(return_value=mock_resp)
        body = client.get("/api/connections").json()

    assert body["count"] == len(DB_CONN_ROWS)


# ---------------------------------------------------------------------------
# GET /api/connections — gateway unreachable
# ---------------------------------------------------------------------------


def test_list_connections_gateway_up_false_when_gateway_unreachable(conn_client):
    client, _, _ = conn_client

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(
            side_effect=httpx.ConnectError("refused")
        )
        body = client.get("/api/connections").json()

    assert body["gateway_up"] is False


def test_list_connections_still_returns_200_when_gateway_unreachable(conn_client):
    client, _, _ = conn_client

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(
            side_effect=httpx.ConnectError("refused")
        )
        resp = client.get("/api/connections")

    assert resp.status_code == 200


def test_list_connections_falls_back_to_db_status_when_gateway_down(conn_client):
    client, _, _ = conn_client

    with patch("httpx.AsyncClient") as mock_ac:
        mock_ac.return_value.__aenter__ = AsyncMock(return_value=mock_ac.return_value)
        mock_ac.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_ac.return_value.get = AsyncMock(
            side_effect=httpx.ConnectError("refused")
        )
        body = client.get("/api/connections").json()

    # DB status is used when live data is absent
    conn1 = next(c for c in body["connections"] if c["id"] == 1)
    assert conn1["status"] == "connected"


# ---------------------------------------------------------------------------
# POST /api/connections/{id}/disconnect
# ---------------------------------------------------------------------------


def test_disconnect_returns_200_for_existing_connection(conn_client):
    client, _, _ = conn_client
    resp = client.post("/api/connections/1/disconnect")
    assert resp.status_code == 200


def test_disconnect_response_has_status_command_sent(conn_client):
    client, _, _ = conn_client
    body = client.post("/api/connections/1/disconnect").json()
    assert body["status"] == "command_sent"


def test_disconnect_response_includes_connection_id(conn_client):
    client, _, _ = conn_client
    body = client.post("/api/connections/1/disconnect").json()
    assert body["connection_id"] == 1


def test_disconnect_response_includes_trace_id(conn_client):
    client, _, mock_producer = conn_client
    fixed_trace = "trace-abc-123"
    mock_producer.disconnect.return_value = fixed_trace
    body = client.post("/api/connections/1/disconnect").json()
    assert body["trace_id"] == fixed_trace


def test_disconnect_calls_producer_with_correct_args(conn_client):
    client, _, mock_producer = conn_client
    client.post("/api/connections/1/disconnect")
    mock_producer.disconnect.assert_called_once_with(1, "555@s.net")


def test_disconnect_returns_404_for_unknown_connection(conn_client):
    client, _, _ = conn_client
    resp = client.post("/api/connections/9999/disconnect")
    assert resp.status_code == 404


def test_disconnect_404_includes_detail_message(conn_client):
    client, _, _ = conn_client
    body = client.post("/api/connections/9999/disconnect").json()
    assert "connection not found" in body["detail"]
