"""Shared fixtures for llm-bot unit tests.

All external dependencies (Redis, ChromaDB, Ollama, Kafka) are mocked here so
tests run without any running infrastructure.
"""
from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub heavy optional dependencies before any app module imports them.
# chromadb and sentence_transformers are not installed in the test venv;
# inject lightweight stubs so app.rag.pipeline can be imported.
# ---------------------------------------------------------------------------

_chroma_stub = MagicMock()
_collection_stub = MagicMock()
_collection_stub.count.return_value = 0
_collection_stub.query.return_value = {"documents": [[]], "metadatas": [[]], "distances": [[]]}
_chroma_stub.PersistentClient.return_value.get_or_create_collection.return_value = _collection_stub

_st_stub = MagicMock()
_st_stub.SentenceTransformer.return_value.encode.return_value = MagicMock(tolist=lambda: [[0.1] * 384])

_confluent_stub = MagicMock()
_consumer_stub = MagicMock()
_producer_stub = MagicMock()
_confluent_stub.Consumer.return_value = _consumer_stub
_confluent_stub.Producer.return_value = _producer_stub

sys.modules.setdefault("chromadb", _chroma_stub)
sys.modules.setdefault("chromadb.config", MagicMock())
sys.modules.setdefault("sentence_transformers", _st_stub)
sys.modules.setdefault("confluent_kafka", _confluent_stub)

# pythonjsonlogger may not be installed; stub it out as well.
if "pythonjsonlogger" not in sys.modules:
    _jsonlogger_stub = MagicMock()
    _jsonlogger_stub.jsonlogger.JsonFormatter = object
    sys.modules["pythonjsonlogger"] = _jsonlogger_stub
    sys.modules["pythonjsonlogger.jsonlogger"] = _jsonlogger_stub.jsonlogger


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    """Async Redis client mock."""
    client = AsyncMock()
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.ping = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    return client


@pytest.fixture()
def mock_chroma_collection():
    """Reusable ChromaDB collection mock."""
    col = MagicMock()
    col.count.return_value = 5
    col.query.return_value = {
        "documents": [["chunk text A", "chunk text B"]],
        "metadatas": [[{"source": "skyrim.md"}, {"source": "witcher3.md"}]],
        "distances": [[0.1, 0.3]],
    }
    col.upsert = MagicMock()
    return col


@pytest.fixture()
def mock_httpx_response():
    """Factory that creates a mock httpx.Response."""
    def _make(status_code: int = 200, json_data: dict | None = None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.raise_for_status = MagicMock()
        if status_code >= 400:
            import httpx
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error", request=MagicMock(), response=resp
            )
        return resp
    return _make


@pytest.fixture()
def mock_kafka_producer():
    """confluent_kafka.Producer mock."""
    prod = MagicMock()
    prod.produce = MagicMock()
    prod.poll = MagicMock(return_value=0)
    prod.flush = MagicMock(return_value=0)
    return prod
