"""LLM bot entrypoint: FastAPI (health/inspection) + gRPC server (hot path).

The gRPC server handles Bot.Process calls from the whatsapp-gateway directly,
replacing the Kafka consumer for the message response path. Kafka still receives
inbound events published by the gateway for analytics.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from grpc import aio as grpc_aio
from fastapi import FastAPI

from app.config import settings
from app.grpc import bot_pb2_grpc
from app.grpc.servicer import BotServicer
from app.idempotency import IdempotencyStore
from app.llm.ollama_client import OllamaClient
from app.rag.pipeline import RagPipeline
from app.telemetry.logging import configure, get_logger

configure(settings.service_name, settings.log_level)
logger = get_logger(__name__)

_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("llm-bot starting up")

    rag = RagPipeline()
    rag.seed()  # idempotent — only embeds if collection is empty
    llm = OllamaClient()
    asyncio.create_task(llm.ensure_model())
    idem = IdempotencyStore()

    # gRPC server — shares the asyncio event loop with uvicorn.
    grpc_server = grpc_aio.server()
    bot_pb2_grpc.add_BotServicer_to_server(BotServicer(rag=rag, llm=llm, idem=idem), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{settings.grpc_port}")
    await grpc_server.start()

    _state.update(rag=rag, llm=llm, idem=idem, grpc_server=grpc_server)
    logger.info("llm-bot ready", extra={"grpc_port": settings.grpc_port})

    yield

    logger.info("llm-bot shutting down")
    await grpc_server.stop(grace=5)
    if _state.get("llm"):
        await _state["llm"].aclose()
    if _state.get("idem"):
        await _state["idem"].aclose()
    logger.info("llm-bot stopped")


app = FastAPI(title="LLM RPG Bot", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


@app.get("/readyz")
async def readyz() -> dict:
    rag: RagPipeline | None = _state.get("rag")
    idem: IdempotencyStore | None = _state.get("idem")
    return {
        "rag_seeded": bool(rag and rag.is_seeded()),
        "redis": bool(idem and await idem.ping()),
    }


@app.get("/rag/search")
async def rag_search(q: str, k: int = 4) -> dict:
    """Inspection endpoint to test retrieval directly."""
    rag: RagPipeline | None = _state.get("rag")
    if not rag:
        return {"error": "rag not ready"}
    chunks = rag.retrieve(q, top_k=k)
    return {
        "query": q,
        "results": [{"source": c.source, "score": round(c.score, 4), "text": c.text} for c in chunks],
    }
