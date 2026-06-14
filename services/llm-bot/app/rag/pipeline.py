"""RAG pipeline: chunk seed docs, embed with sentence-transformers, store in ChromaDB,
and retrieve top-k context for grounding the LLM.

The grounding guarantee: the LLM only ever sees retrieved chunks plus a strict system
prompt instructing it to answer ONLY from that context, so it cannot wander off-topic.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

import chromadb
from chromadb.config import Settings as ChromaSettings
from sentence_transformers import SentenceTransformer

from app.config import settings
from app.telemetry.logging import get_logger

logger = get_logger(__name__)


@dataclass
class RetrievedChunk:
    text: str
    source: str
    score: float


def _chunk_text(text: str, size: int, overlap: int) -> list[str]:
    """Sliding-window character chunking with overlap.

    Character-based (not token-based) keeps the dependency surface small. For a bounded
    RPG knowledge base the precision difference vs. token chunking is negligible.
    """
    text = text.strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]
    chunks: list[str] = []
    start = 0
    step = max(1, size - overlap)
    while start < len(text):
        chunks.append(text[start : start + size])
        start += step
    return chunks


class RagPipeline:
    """Owns the embedding model and the ChromaDB collection."""

    def __init__(self) -> None:
        logger.info("loading embedding model", extra={"model": settings.embedding_model})
        self._embedder = SentenceTransformer(settings.embedding_model)
        os.makedirs(settings.chroma_path, exist_ok=True)
        self._client = chromadb.PersistentClient(
            path=settings.chroma_path,
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        self._collection = self._client.get_or_create_collection(
            name=settings.rag_collection,
            metadata={"hnsw:space": "cosine"},
        )

    def _embed(self, texts: list[str]) -> list[list[float]]:
        return self._embedder.encode(texts, normalize_embeddings=True).tolist()

    def is_seeded(self) -> bool:
        try:
            return self._collection.count() > 0
        except Exception:  # noqa: BLE001
            return False

    def seed(self, docs_path: str | None = None, force: bool = False) -> int:
        """Embed and index every markdown file under docs_path. Idempotent: chunk ids
        are content-hash based so re-seeding upserts rather than duplicating."""
        docs_path = docs_path or settings.rag_docs_path
        if self.is_seeded() and not force:
            logger.info("collection already seeded; skipping", extra={"count": self._collection.count()})
            return self._collection.count()

        ids: list[str] = []
        documents: list[str] = []
        metadatas: list[dict] = []

        if not os.path.isdir(docs_path):
            logger.warning("rag docs path missing", extra={"path": docs_path})
            return 0

        for fname in sorted(os.listdir(docs_path)):
            if not fname.endswith(".md"):
                continue
            full = os.path.join(docs_path, fname)
            with open(full, "r", encoding="utf-8") as fh:
                content = fh.read()
            for chunk in _chunk_text(content, settings.chunk_size, settings.chunk_overlap):
                chunk_id = hashlib.sha256(f"{fname}:{chunk}".encode()).hexdigest()[:32]
                ids.append(chunk_id)
                documents.append(chunk)
                metadatas.append({"source": fname})

        if not documents:
            logger.warning("no documents to seed", extra={"path": docs_path})
            return 0

        embeddings = self._embed(documents)
        # upsert is idempotent on id, so re-running seed is safe.
        self._collection.upsert(
            ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas
        )
        count = self._collection.count()
        logger.info("rag seed complete", extra={"chunks": len(documents), "total": count})
        return count

    def retrieve(self, query: str, top_k: int | None = None) -> list[RetrievedChunk]:
        """Return the top-k most similar chunks for the query."""
        top_k = top_k or settings.rag_top_k
        if self._collection.count() == 0:
            return []
        q_emb = self._embed([query])
        res = self._collection.query(
            query_embeddings=q_emb,
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )
        out: list[RetrievedChunk] = []
        docs = (res.get("documents") or [[]])[0]
        metas = (res.get("metadatas") or [[]])[0]
        dists = (res.get("distances") or [[]])[0]
        for doc, meta, dist in zip(docs, metas, dists):
            # cosine distance -> similarity score
            out.append(RetrievedChunk(text=doc, source=(meta or {}).get("source", "?"), score=1.0 - dist))
        return out
