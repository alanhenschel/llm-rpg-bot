"""Analytics endpoints powered by message_logs aggregations."""
from __future__ import annotations

from fastapi import APIRouter

from app.db.queries import db

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/messages")
async def messages_per_hour() -> dict:
    """Message count per hour for today, split by direction."""
    rows = await db.messages_per_hour()
    return {"data": rows}


@router.get("/bytes")
async def messages_bytes_detail() -> dict:
    """Bytes transferred per message today."""
    rows = await db.messages_bytes_detail()
    return {"data": rows}


@router.get("/connections")
async def connection_uptime() -> dict:
    """Connection uptime/age stats."""
    rows = await db.connection_stats()
    return {"data": rows}
