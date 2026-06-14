"""Structured JSON logging (shared schema: service, level, timestamp, trace_id)."""
from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger

_CONFIGURED = False


class _SchemaFormatter(jsonlogger.JsonFormatter):
    def __init__(self, service_name: str) -> None:
        super().__init__(
            "%(timestamp)s %(level)s %(service)s %(message)s",
            rename_fields={"levelname": "level", "asctime": "timestamp"},
            timestamp=True,
        )
        self._service_name = service_name

    def add_fields(self, log_record, record, message_dict):  # noqa: ANN001
        super().add_fields(log_record, record, message_dict)
        log_record["service"] = self._service_name
        log_record["level"] = record.levelname.lower()


def configure(service_name: str, level: str = "INFO") -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_SchemaFormatter(service_name))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
