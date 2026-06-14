"""Kafka consumer (inbound) + producer (outbound) wrappers using confluent-kafka.

The consumer runs the blocking poll loop in a thread executor so it cooperates with
the asyncio event loop driving the LLM/Ollama calls.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Awaitable, Callable

from confluent_kafka import Consumer, Producer

from app.config import settings
from app.telemetry.logging import get_logger

logger = get_logger(__name__)

InboundHandler = Callable[[dict, str], Awaitable[None]]


def _trace_from_headers(headers) -> str:
    if not headers:
        return ""
    for k, v in headers:
        if k == "trace_id" and v is not None:
            return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
    return ""


class OutboundProducer:
    """Idempotent producer for send commands to the outbound topic."""

    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_brokers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
            }
        )

    def send(self, *, connection_id: int, jid: str, body: str, trace_id: str, event_id: str) -> None:
        payload = {
            "event_id": event_id,
            "trace_id": trace_id,
            "connection_id": connection_id,
            "jid": jid,
            "body": body,
            "command": "send",
            "timestamp": int(__import__("time").time() * 1000),
        }
        self._producer.produce(
            settings.topic_outbound,
            key=jid.encode(),
            value=json.dumps(payload).encode(),
            headers=[("trace_id", trace_id.encode())],
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)


class InboundConsumer:
    """Consumer-group reader for the inbound topic."""

    def __init__(self, handler: InboundHandler) -> None:
        self._handler = handler
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_brokers,
                "group.id": settings.consumer_group,
                "auto.offset.reset": "earliest",
                # Manual commit after successful handling = at-least-once; Redis
                # idempotency makes reprocessing safe.
                "enable.auto.commit": False,
            }
        )
        self._running = False

    async def run(self) -> None:
        self._consumer.subscribe([settings.topic_inbound])
        self._running = True
        loop = asyncio.get_running_loop()
        logger.info("inbound consumer started", extra={"topic": settings.topic_inbound})
        try:
            while self._running:
                # Poll in an executor so we don't block the event loop.
                msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("kafka consume error", extra={"error": str(msg.error())})
                    continue
                trace_id = _trace_from_headers(msg.headers()) or str(uuid.uuid4())
                try:
                    data = json.loads(msg.value())
                except (ValueError, TypeError) as exc:
                    logger.error("poison inbound message; skipping", extra={"error": str(exc)})
                    self._consumer.commit(msg, asynchronous=False)
                    continue
                try:
                    await self._handler(data, trace_id)
                    self._consumer.commit(msg, asynchronous=False)
                except Exception as exc:  # noqa: BLE001
                    # Do not commit -> message will be redelivered.
                    logger.error(
                        "handler failed; will retry",
                        extra={"error": str(exc), "trace_id": trace_id},
                    )
        finally:
            self._consumer.close()
            logger.info("inbound consumer stopped")

    def stop(self) -> None:
        self._running = False
