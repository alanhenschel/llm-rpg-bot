"""Management API entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db.queries import db
import app.kafka.producer as _producer_module
from app.kafka.producer import CommandProducer
from app.logging_setup import configure, get_logger
from app.routers import analytics, connections

configure(settings.service_name, settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    _producer_module.command_producer = CommandProducer()
    logger.info("management-api ready")
    yield
    await db.close()
    if _producer_module.command_producer:
        _producer_module.command_producer.close()
    logger.info("management-api stopped")


app = FastAPI(title="WhatsApp Pipeline Management API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_origins.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(connections.router)
app.include_router(analytics.router)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


@app.get("/readyz")
async def readyz() -> dict:
    return {"database": await db.ping()}
