"""gRPC servicer: implements Bot.Process — the hot path for WhatsApp message handling.

The gateway calls this directly instead of publishing to Kafka. Kafka still receives
inbound events from the gateway for analytics, but the bot response path is gRPC-only.
"""
from __future__ import annotations

import grpc

from app.grpc import bot_pb2, bot_pb2_grpc  # stubs generated at build time
from app.idempotency import IdempotencyStore
from app.llm.ollama_client import OllamaClient, build_prompt
from app.rag.pipeline import RagPipeline
from app.telemetry.logging import get_logger, log_extra

logger = get_logger(__name__)


class BotServicer(bot_pb2_grpc.BotServicer):
    def __init__(self, rag: RagPipeline, llm: OllamaClient, idem: IdempotencyStore) -> None:
        self._rag = rag
        self._llm = llm
        self._idem = idem

    async def Process(self, request: bot_pb2.InboundMessage, context: grpc.aio.ServicerContext):
        event_id = request.event_id
        trace_id = request.trace_id
        body = request.body.strip()
        jid = request.jid
        connection_id = request.connection_id

        log = log_extra(trace_id, event_id, jid=jid, connection_id=connection_id)

        if not event_id or not body:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "event_id and body are required")
            return

        first_time = await self._idem.claim(event_id)
        if not first_time:
            logger.info("duplicate event; returning empty reply", extra=log)
            yield bot_pb2.ReplyChunk(text="", done=True)
            return

        try:
            logger.info("processing via gRPC", extra={**log, "body_preview": body[:80]})

            chunks = self._rag.retrieve(body)
            prompt = build_prompt(body, chunks)

            sources = [c.source for c in chunks]
            total = 0

            async for token in self._llm.stream(prompt, trace_id=trace_id, event_id=event_id):
                if token:
                    total += len(token)
                    yield bot_pb2.ReplyChunk(text=token, done=False)

            yield bot_pb2.ReplyChunk(text="", done=True)

            logger.info(
                "gRPC reply streamed",
                extra={**log, "sources": sources, "answer_chars": total},
            )
        except Exception as exc:
            await self._idem.release(event_id)
            logger.error("BotServicer.Process failed", extra={**log, "error": str(exc)})
            await context.abort(grpc.StatusCode.INTERNAL, "internal error")
