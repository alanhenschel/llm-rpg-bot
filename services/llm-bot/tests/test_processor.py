"""Unit tests for app.processor.MessageProcessor.handle().

All four injected collaborators (rag, llm, producer, idempotency) are mocked.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.processor import MessageProcessor
from app.rag.pipeline import RetrievedChunk


@pytest.fixture()
def mock_rag():
    rag = MagicMock()
    rag.retrieve.return_value = [
        RetrievedChunk(text="lore text", source="skyrim.md", score=0.85)
    ]
    return rag


@pytest.fixture()
def mock_llm():
    llm = AsyncMock()
    llm.generate = AsyncMock(return_value="Here is your answer.")
    return llm


@pytest.fixture()
def mock_producer():
    prod = MagicMock()
    prod.send = MagicMock()
    prod.flush = MagicMock()
    return prod


@pytest.fixture()
def mock_idem():
    idem = AsyncMock()
    idem.claim = AsyncMock(return_value=True)
    idem.release = AsyncMock()
    return idem


@pytest.fixture()
def processor(mock_rag, mock_llm, mock_producer, mock_idem):
    return MessageProcessor(
        rag=mock_rag,
        llm=mock_llm,
        producer=mock_producer,
        idempotency=mock_idem,
    )


# ---------------------------------------------------------------------------
# Early-exit conditions
# ---------------------------------------------------------------------------


async def test_handle_drops_message_without_event_id(processor, mock_idem, mock_rag):
    msg = {"body": "hello", "jid": "123@s.whatsapp.net", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_idem.claim.assert_not_called()
    mock_rag.retrieve.assert_not_called()


async def test_handle_drops_message_with_empty_event_id(processor, mock_idem, mock_rag):
    msg = {"event_id": "", "body": "hello", "jid": "j", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_idem.claim.assert_not_called()


async def test_handle_drops_message_without_body(processor, mock_idem, mock_rag):
    msg = {"event_id": "evt-1", "body": "", "jid": "j", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_idem.claim.assert_not_called()


async def test_handle_drops_message_with_whitespace_only_body(processor, mock_idem, mock_rag):
    msg = {"event_id": "evt-1", "body": "   \n", "jid": "j", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_idem.claim.assert_not_called()


async def test_handle_drops_message_with_missing_body_key(processor, mock_idem, mock_rag):
    msg = {"event_id": "evt-1", "jid": "j", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_idem.claim.assert_not_called()


# ---------------------------------------------------------------------------
# Idempotency / duplicate detection
# ---------------------------------------------------------------------------


async def test_handle_skips_when_idempotency_returns_false(
    processor, mock_idem, mock_rag, mock_llm
):
    mock_idem.claim.return_value = False
    msg = {"event_id": "dup-event", "body": "hi", "jid": "j", "connection_id": 1}
    await processor.handle(msg, trace_id="t1")
    mock_rag.retrieve.assert_not_called()
    mock_llm.generate.assert_not_called()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_handle_happy_path_calls_all_collaborators(
    processor, mock_rag, mock_llm, mock_producer, mock_idem
):
    msg = {"event_id": "evt-ok", "body": "What is Skyrim?", "jid": "555@s.net", "connection_id": 7}
    await processor.handle(msg, trace_id="trace-abc")

    mock_idem.claim.assert_awaited_once_with("evt-ok")
    mock_rag.retrieve.assert_called_once_with("What is Skyrim?")
    mock_llm.generate.assert_awaited_once()
    mock_producer.send.assert_called_once()
    mock_producer.flush.assert_called_once()


async def test_handle_passes_correct_send_args(
    processor, mock_producer, mock_idem, mock_llm
):
    mock_llm.generate.return_value = "Skyrim is a RPG."
    msg = {
        "event_id": "evt-send",
        "body": "Tell me about skyrim",
        "jid": "999@s.whatsapp.net",
        "connection_id": 3,
    }
    await processor.handle(msg, trace_id="tr-xyz")

    send_kwargs = mock_producer.send.call_args[1]
    assert send_kwargs["connection_id"] == 3
    assert send_kwargs["jid"] == "999@s.whatsapp.net"
    assert send_kwargs["body"] == "Skyrim is a RPG."
    assert send_kwargs["trace_id"] == "tr-xyz"
    assert send_kwargs["event_id"] == "evt-send"


async def test_handle_passes_connection_id_as_int(processor, mock_producer):
    msg = {
        "event_id": "evt-int",
        "body": "test",
        "jid": "j",
        "connection_id": "5",  # string in raw message
    }
    await processor.handle(msg, trace_id="t")
    send_kwargs = mock_producer.send.call_args[1]
    assert isinstance(send_kwargs["connection_id"], int)
    assert send_kwargs["connection_id"] == 5


# ---------------------------------------------------------------------------
# Exception handling / idempotency release
# ---------------------------------------------------------------------------


async def test_handle_releases_idempotency_on_exception(
    processor, mock_idem, mock_llm
):
    """If LLM raises, the idempotency claim must be released so a retry can reprocess."""
    mock_llm.generate.side_effect = RuntimeError("ollama down")
    msg = {"event_id": "evt-fail", "body": "hello", "jid": "j", "connection_id": 1}

    with pytest.raises(RuntimeError):
        await processor.handle(msg, trace_id="t")

    mock_idem.release.assert_awaited_once_with("evt-fail")


async def test_handle_releases_idempotency_on_rag_exception(
    processor, mock_idem, mock_rag
):
    mock_rag.retrieve.side_effect = RuntimeError("chroma error")
    msg = {"event_id": "evt-rag-fail", "body": "question", "jid": "j", "connection_id": 1}

    with pytest.raises(RuntimeError):
        await processor.handle(msg, trace_id="t")

    mock_idem.release.assert_awaited_once_with("evt-rag-fail")


async def test_handle_releases_idempotency_on_producer_exception(
    processor, mock_idem, mock_producer
):
    mock_producer.send.side_effect = RuntimeError("kafka down")
    msg = {"event_id": "evt-prod-fail", "body": "hi", "jid": "j", "connection_id": 1}

    with pytest.raises(RuntimeError):
        await processor.handle(msg, trace_id="t")

    mock_idem.release.assert_awaited_once_with("evt-prod-fail")


async def test_handle_does_not_release_on_duplicate(mock_idem, mock_rag, mock_llm, mock_producer):
    """If event is a duplicate (claim returns False), release must NOT be called."""
    mock_idem.claim.return_value = False
    proc = MessageProcessor(mock_rag, mock_llm, mock_producer, mock_idem)
    msg = {"event_id": "dup", "body": "hi", "jid": "j", "connection_id": 1}
    await proc.handle(msg, trace_id="t")
    mock_idem.release.assert_not_called()
