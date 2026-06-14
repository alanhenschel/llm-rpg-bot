"""Standalone RAG seeding script. Run inside the container or locally.

Usage: python seed_rag.py [--force]
"""
from __future__ import annotations

import sys

from app.config import settings
from app.rag.pipeline import RagPipeline
from app.telemetry.logging import configure, get_logger


def main() -> int:
    configure(settings.service_name, settings.log_level)
    logger = get_logger("seed_rag")
    force = "--force" in sys.argv
    rag = RagPipeline()
    count = rag.seed(force=force)
    logger.info("seeding finished", extra={"chunks_indexed": count, "force": force})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
