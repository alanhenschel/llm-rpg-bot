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
        # Use a minimal format string; all fields are injected in add_fields.
        super().__init__("%(message)s", timestamp=True)
        self._service_name = service_name

    def add_fields(self, log_record, record, message_dict):  # noqa: ANN001
        super().add_fields(log_record, record, message_dict)
        log_record["service"] = self._service_name
        # Normalize level: pythonjsonlogger or grpc may emit records without
        # the standard `levelname` attribute — guard defensively.
        raw_level = getattr(record, "levelname", None) or log_record.get("levelname", "INFO")
        log_record["level"] = str(raw_level).lower()
        log_record.pop("levelname", None)
        # Normalize timestamp field name.
        if "asctime" in log_record:
            log_record["timestamp"] = log_record.pop("asctime")


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
