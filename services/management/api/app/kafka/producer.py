"""Idempotent producer for control commands (e.g. disconnect a connection)."""
from __future__ import annotations

import json
import time
import uuid

from confluent_kafka import Producer

from app.config import settings
from app.logging_setup import get_logger

logger = get_logger(__name__)


class CommandProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_brokers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def disconnect(self, connection_id: int, jid: str = "") -> str:
        trace_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "trace_id": trace_id,
            "connection_id": connection_id,
            "jid": jid,
            "body": "",
            "command": "disconnect",
            "timestamp": int(time.time() * 1000),
        }
        self._producer.produce(
            settings.topic_outbound,
            key=str(connection_id).encode(),
            value=json.dumps(payload).encode(),
            headers=[("trace_id", trace_id.encode())],
        )
        self._producer.flush(5)
        logger.info(
            "disconnect command published",
            extra={"trace_id": trace_id, "event_id": event_id, "connection_id": connection_id},
        )
        return trace_id

    def close(self) -> None:
        self._producer.flush(5)


command_producer: "CommandProducer | None" = None
