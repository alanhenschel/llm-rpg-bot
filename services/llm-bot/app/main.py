"""LLM bot entrypoint: FastAPI app (health/inspection) + background Kafka consumer.

The Kafka consumer loop runs as an asyncio task started on FastAPI startup and stopped
gracefully on shutdown.
"""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.idempotency import IdempotencyStore
from app.kafka.bus import InboundConsumer, OutboundProducer
from app.llm.ollama_client import OllamaClient
from app.processor import MessageProcessor
from app.rag.pipeline import RagPipeline
from app.telemetry.logging import configure, get_logger

configure(settings.service_name, settings.log_level)
logger = get_logger(__name__)

# Component handles populated on startup.
_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("llm-bot starting up")
    rag = RagPipeline()
    rag.seed()  # idempotent — only embeds if collection is empty
    llm = OllamaClient()
    # ensure_model is best-effort; runs in background so startup isn't blocked for minutes.
    asyncio.create_task(llm.ensure_model())
    producer = OutboundProducer()
    idem = IdempotencyStore()
    processor = MessageProcessor(rag, llm, producer, idem)
    consumer = InboundConsumer(processor.handle)
    consumer_task = asyncio.create_task(consumer.run())
    _state.update(
        rag=rag, llm=llm, producer=producer, idem=idem,
        consumer=consumer, consumer_task=consumer_task,
    )
    logger.info("llm-bot ready")
    yield
    logger.info("llm-bot shutting down")
    consumer_inst: InboundConsumer | None = _state.get("consumer")
    if consumer_inst:
        consumer_inst.stop()
    task: asyncio.Task | None = _state.get("consumer_task")
    if task:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)
    if _state.get("producer"):
        _state["producer"].flush()
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
