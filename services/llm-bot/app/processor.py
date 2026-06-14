"""The core message processor: idempotency check -> RAG retrieve -> LLM generate ->
publish response to the outbound topic."""
from __future__ import annotations

from app.idempotency import IdempotencyStore
from app.kafka.bus import OutboundProducer
from app.llm.ollama_client import OllamaClient, build_prompt
from app.rag.pipeline import RagPipeline
from app.telemetry.logging import get_logger, log_extra

logger = get_logger(__name__)


class MessageProcessor:
    def __init__(
        self,
        rag: RagPipeline,
        llm: OllamaClient,
        producer: OutboundProducer,
        idempotency: IdempotencyStore,
    ) -> None:
        self._rag = rag
        self._llm = llm
        self._producer = producer
        self._idem = idempotency

    async def handle(self, message: dict, trace_id: str) -> None:
        event_id = message.get("event_id", "")
        body = (message.get("body") or "").strip()
        jid = message.get("jid", "")
        connection_id = int(message.get("connection_id", 0))

        log = log_extra(trace_id, event_id, jid=jid, connection_id=connection_id)

        if not event_id or not body:
            logger.warning("dropping message without event_id/body", extra=log)
            return

        # Idempotency: claim the event. If already processed, skip silently.
        first_time = await self._idem.claim(event_id)
        if not first_time:
            logger.info("duplicate event; skipping", extra=log)
            return

        try:
            logger.info("processing inbound message", extra={**log, "body_preview": body[:80]})

            chunks = self._rag.retrieve(body)
            prompt = build_prompt(body, chunks)
            answer = await self._llm.generate(prompt, trace_id=trace_id, event_id=event_id)

            self._producer.send(
                connection_id=connection_id,
                jid=jid,
                body=answer,
                trace_id=trace_id,
                event_id=event_id,
            )
            self._producer.flush()
            logger.info(
                "response published",
                extra={**log, "sources": [c.source for c in chunks], "answer_chars": len(answer)},
            )
        except Exception:
            # Release the idempotency claim so a retry can reprocess this event.
            await self._idem.release(event_id)
            raise
