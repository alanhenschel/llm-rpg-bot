"""Unit tests for app.rag.pipeline.

RagPipeline's __init__ loads a SentenceTransformer and touches ChromaDB;
both are mocked via conftest stubs. Individual tests further override the
collection mock to control return values.
"""
from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from app.rag.pipeline import RagPipeline, RetrievedChunk, _chunk_text


# ---------------------------------------------------------------------------
# _chunk_text — pure function, no mocking needed
# ---------------------------------------------------------------------------


def test_chunk_text_empty_string_returns_empty_list():
    assert _chunk_text("", 500, 50) == []


def test_chunk_text_whitespace_only_returns_empty_list():
    assert _chunk_text("   \n  ", 500, 50) == []


def test_chunk_text_shorter_than_size_returns_single_chunk():
    text = "Short text"
    result = _chunk_text(text, 500, 50)
    assert result == ["Short text"]


def test_chunk_text_exact_size_returns_single_chunk():
    text = "x" * 500
    result = _chunk_text(text, 500, 50)
    assert result == [text]


def test_chunk_text_larger_than_size_produces_multiple_chunks():
    text = "a" * 1000
    result = _chunk_text(text, 500, 50)
    assert len(result) > 1


def test_chunk_text_overlap_means_chunks_share_content():
    text = "abcdefghij"
    # size=6, overlap=2 → step=4; chunks at offsets: 0, 4, 8 → ["abcdef", "efghij", "ij"]
    result = _chunk_text(text, 6, 2)
    # Adjacent chunks share `overlap` characters at their boundary
    assert result[0][-2:] == result[1][:2]  # "ef" appears at end of chunk0 and start of chunk1


def test_chunk_text_zero_overlap_no_shared_content():
    text = "abcdefghij"
    result = _chunk_text(text, 5, 0)
    assert result == ["abcde", "fghij"]


def test_chunk_text_strips_leading_trailing_whitespace():
    text = "  hello world  "
    result = _chunk_text(text, 500, 50)
    assert result == ["hello world"]


def test_chunk_text_each_chunk_at_most_size_chars():
    text = "x" * 2000
    result = _chunk_text(text, 300, 30)
    for chunk in result:
        assert len(chunk) <= 300


# ---------------------------------------------------------------------------
# RagPipeline — mock ChromaDB and SentenceTransformer
# ---------------------------------------------------------------------------


@pytest.fixture()
def rag(mock_chroma_collection):
    """RagPipeline with all heavy deps mocked."""
    with (
        patch("app.rag.pipeline.SentenceTransformer") as mock_st,
        patch("app.rag.pipeline.chromadb.PersistentClient") as mock_chroma,
        patch("os.makedirs"),
    ):
        mock_st.return_value.encode.return_value = MagicMock(
            tolist=MagicMock(return_value=[[0.1] * 384])
        )
        mock_chroma.return_value.get_or_create_collection.return_value = mock_chroma_collection
        pipeline = RagPipeline()
    return pipeline


def test_is_seeded_returns_true_when_collection_has_items(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 3
    assert rag.is_seeded() is True


def test_is_seeded_returns_false_when_collection_empty(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 0
    assert rag.is_seeded() is False


def test_is_seeded_returns_false_on_exception(rag, mock_chroma_collection):
    mock_chroma_collection.count.side_effect = Exception("chroma down")
    assert rag.is_seeded() is False


def test_seed_skips_when_already_seeded_and_not_forced(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 10
    result = rag.seed(docs_path="/nonexistent", force=False)
    # Should return early with count; upsert must NOT be called.
    mock_chroma_collection.upsert.assert_not_called()
    assert result == 10


def test_seed_returns_zero_when_docs_path_missing(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 0
    result = rag.seed(docs_path="/absolutely/does/not/exist")
    assert result == 0


def test_seed_calls_upsert_with_correct_structure(rag, mock_chroma_collection):
    mock_chroma_collection.count.side_effect = [0, 1]

    with tempfile.TemporaryDirectory() as tmpdir:
        md_path = os.path.join(tmpdir, "test.md")
        with open(md_path, "w") as f:
            f.write("Some RPG lore content for testing the seed function.")

        rag._embedder.encode.return_value = MagicMock(tolist=MagicMock(return_value=[[0.1] * 384]))
        rag.seed(docs_path=tmpdir, force=True)

    mock_chroma_collection.upsert.assert_called_once()
    call_kwargs = mock_chroma_collection.upsert.call_args[1]
    assert "ids" in call_kwargs
    assert "documents" in call_kwargs
    assert "embeddings" in call_kwargs
    assert "metadatas" in call_kwargs


def test_seed_ignores_non_markdown_files(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "notes.txt"), "w") as f:
            f.write("plain text file")
        with open(os.path.join(tmpdir, "image.png"), "w") as f:
            f.write("fake image")

        result = rag.seed(docs_path=tmpdir, force=True)

    assert result == 0
    mock_chroma_collection.upsert.assert_not_called()


def test_retrieve_returns_empty_list_when_collection_empty(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 0
    result = rag.retrieve("tell me about skyrim")
    assert result == []


def test_retrieve_maps_cosine_distance_to_score(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 2
    mock_chroma_collection.query.return_value = {
        "documents": [["chunk A", "chunk B"]],
        "metadatas": [[{"source": "skyrim.md"}, {"source": "witcher3.md"}]],
        "distances": [[0.1, 0.4]],
    }
    rag._embedder.encode.return_value = MagicMock(tolist=MagicMock(return_value=[[0.0] * 384]))

    results = rag.retrieve("dragonborn", top_k=2)

    assert len(results) == 2
    # score = 1.0 - distance
    assert abs(results[0].score - 0.9) < 1e-6
    assert abs(results[1].score - 0.6) < 1e-6


def test_retrieve_populates_source_from_metadata(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 1
    mock_chroma_collection.query.return_value = {
        "documents": [["some text"]],
        "metadatas": [[{"source": "fallout4.md"}]],
        "distances": [[0.2]],
    }
    rag._embedder.encode.return_value = MagicMock(tolist=MagicMock(return_value=[[0.0] * 384]))

    results = rag.retrieve("vault", top_k=1)

    assert results[0].source == "fallout4.md"
    assert results[0].text == "some text"


def test_retrieve_uses_fallback_source_when_metadata_missing(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 1
    mock_chroma_collection.query.return_value = {
        "documents": [["text"]],
        "metadatas": [[None]],
        "distances": [[0.0]],
    }
    rag._embedder.encode.return_value = MagicMock(tolist=MagicMock(return_value=[[0.0] * 384]))

    results = rag.retrieve("query")
    assert results[0].source == "?"


def test_retrieve_returns_correct_chunk_type(rag, mock_chroma_collection):
    mock_chroma_collection.count.return_value = 1
    mock_chroma_collection.query.return_value = {
        "documents": [["text"]],
        "metadatas": [[{"source": "dark_souls.md"}]],
        "distances": [[0.05]],
    }
    rag._embedder.encode.return_value = MagicMock(tolist=MagicMock(return_value=[[0.0] * 384]))

    results = rag.retrieve("bonfire")
    assert isinstance(results[0], RetrievedChunk)
