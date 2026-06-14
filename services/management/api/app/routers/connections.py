"""Connection endpoints: proxy live status from the gateway, merge with DB stats,
and send disconnect commands via Kafka."""
from __future__ import annotations

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.db.queries import db
import app.kafka.producer as _producer_module
from app.logging_setup import get_logger

router = APIRouter(prefix="/api/connections", tags=["connections"])
logger = get_logger(__name__)


@router.get("")
async def list_connections() -> dict:
    """List connections. Live runtime state comes from the gateway's /connections
    endpoint; persisted metadata + today's byte totals come from PostgreSQL."""
    live_by_id: dict[int, dict] = {}
    gateway_up = False
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{settings.gateway_url}/connections")
            resp.raise_for_status()
            data = resp.json()
            gateway_up = True
            for c in data.get("connections", []):
                live_by_id[c["id"]] = c
    except httpx.HTTPError as exc:
        logger.warning("gateway /connections unreachable", extra={"error": str(exc)})

    db_rows = await db.connection_stats()
    bytes_by_conn = await db.bytes_today_by_connection()

    merged = []
    for row in db_rows:
        live = live_by_id.get(row["id"], {})
        merged.append(
            {
                "id": row["id"],
                "label": row["label"],
                "jid": live.get("jid") or row["jid"],
                "status": live.get("status") or row["status"],
                "pod_id": row["pod_id"],
                "last_seen": row["last_seen"],
                "bytes_today": bytes_by_conn.get(row["id"], 0),
                "bytes_in": live.get("bytes_in", 0),
                "bytes_out": live.get("bytes_out", 0),
                "live": row["id"] in live_by_id,
            }
        )
    return {"gateway_up": gateway_up, "connections": merged, "count": len(merged)}


class CreateConnectionRequest(BaseModel):
    label: str


@router.post("")
async def create_connection(req: CreateConnectionRequest) -> dict:
    """Ask the gateway to create a new phone slot and start QR pairing immediately."""
    if not req.label.strip():
        raise HTTPException(status_code=400, detail="label must not be empty")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.gateway_url}/connections",
                json={"label": req.label.strip()},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        logger.error("gateway /connections POST failed", extra={"error": str(exc)})
        raise HTTPException(status_code=502, detail="gateway unreachable") from exc


@router.get("/{connection_id}/qr")
async def get_connection_qr(connection_id: int) -> dict:
    """Proxy the live QR string from the gateway for a pending connection."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{settings.gateway_url}/connections/{connection_id}/qr"
            )
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="no qr available")
            resp.raise_for_status()
            return resp.json()
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        logger.warning("gateway qr unreachable", extra={"error": str(exc)})
        raise HTTPException(status_code=502, detail="gateway unreachable") from exc


@router.post("/{connection_id}/disconnect")
async def disconnect_connection(connection_id: int) -> dict:
    """Publish a disconnect command. The owning gateway pod will act on it."""
    rows = await db.connection_stats()
    match = next((r for r in rows if r["id"] == connection_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail="connection not found")
    trace_id = _producer_module.command_producer.disconnect(connection_id, match["jid"])
    return {"status": "command_sent", "connection_id": connection_id, "trace_id": trace_id}
