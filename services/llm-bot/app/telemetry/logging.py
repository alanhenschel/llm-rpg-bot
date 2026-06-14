"""Structured JSON logging matching the gateway's field schema.

Every log line carries: service, level, timestamp, and (where applicable) trace_id
and event_id. This keeps Loki/Grafana queries identical across Go and Python services.
"""
from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger

_CONFIGURED = False


class _SchemaFormatter(jsonlogger.JsonFormatter):
    """Rename/standardize fields to the shared pipeline schema."""

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
        if "level" not in log_record:
            log_record["level"] = record.levelname.lower()
        else:
            log_record["level"] = str(log_record["level"]).lower()


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


def log_extra(trace_id: str | None = None, event_id: str | None = None, **kw) -> dict:
    """Build the `extra` dict so trace_id/event_id land as top-level JSON fields."""
    extra: dict = dict(kw)
    if trace_id:
        extra["trace_id"] = trace_id
    if event_id:
        extra["event_id"] = event_id
    return extra
