"""Async Ollama HTTP client with retries and the RAG-grounded prompt builder."""
from __future__ import annotations

import asyncio

import httpx

from app.config import settings
from app.rag.pipeline import RetrievedChunk
from app.telemetry.logging import get_logger, log_extra

logger = get_logger(__name__)

# Hard system prompt: this is the anti-hallucination guard. The model MUST only use
# the provided context and must gracefully decline anything outside the RPG knowledge base.
SYSTEM_PROMPT = (
    "You are a friendly and knowledgeable assistant specialized exclusively in RPG video "
    "games (such as Skyrim, Fallout 4, Fallout: New Vegas, The Witcher 3, and Dark Souls).\n"
    "Rules you MUST follow:\n"
    "1. Answer ONLY using the information in the CONTEXT section below.\n"
    "2. If the context does not contain the answer, say you can only help with the RPG games "
    "in your knowledge base and suggest what you can talk about. Never invent facts.\n"
    "3. Stay in character as a helpful RPG guide. Keep answers concise and chat-friendly "
    "(this is a WhatsApp conversation).\n"
    "4. Do not discuss topics unrelated to these RPG games."
)

# Floor below which retrieved context is considered irrelevant.
RELEVANCE_FLOOR = 0.2


def build_prompt(user_message: str, chunks: list[RetrievedChunk]) -> str:
    """Compose the final prompt injecting retrieved context."""
    relevant = [c for c in chunks if c.score >= RELEVANCE_FLOOR]
    if relevant:
        context = "\n\n".join(f"[Source: {c.source}]\n{c.text}" for c in relevant)
    else:
        context = (
            "(No relevant information found in the RPG knowledge base for this question.)"
        )
    return (
        f"{SYSTEM_PROMPT}\n\n"
        f"=== CONTEXT ===\n{context}\n=== END CONTEXT ===\n\n"
        f"User question: {user_message}\n\n"
        f"Answer:"
    )


class OllamaClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.ollama_base_url,
            timeout=settings.ollama_timeout_seconds,
        )

    async def generate(self, prompt: str, *, trace_id: str = "", event_id: str = "") -> str:
        """Call Ollama /api/generate with exponential-backoff retries."""
        payload = {
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3},
        }
        last_err: Exception | None = None
        for attempt in range(1, 4):
            try:
                resp = await self._client.post("/api/generate", json=payload)
                resp.raise_for_status()
                data = resp.json()
                return (data.get("response") or "").strip()
            except (httpx.HTTPError, ValueError) as exc:  # network or json error
                last_err = exc
                backoff = 2 ** (attempt - 1)
                logger.warning(
                    "ollama generate failed; retrying",
                    extra=log_extra(trace_id, event_id, attempt=attempt, backoff=backoff, error=str(exc)),
                )
                if attempt < 3:
                    await asyncio.sleep(backoff)
        logger.error(
            "ollama generate exhausted retries",
            extra=log_extra(trace_id, event_id, error=str(last_err)),
        )
        # Graceful degradation: return a safe fallback rather than crashing the consumer.
        return (
            "Sorry, I'm having trouble reaching my knowledge engine right now. "
            "Please try again in a moment."
        )

    async def ensure_model(self) -> bool:
        """Best-effort check/pull of the configured model. Non-fatal on failure."""
        try:
            tags = await self._client.get("/api/tags")
            tags.raise_for_status()
            names = {m.get("name", "") for m in tags.json().get("models", [])}
            if any(settings.ollama_model in n for n in names):
                return True
            logger.info("pulling ollama model", extra={"model": settings.ollama_model})
            # Pull can take a while; stream=False blocks until done.
            pull = await self._client.post(
                "/api/pull", json={"name": settings.ollama_model, "stream": False},
                timeout=None,
            )
            pull.raise_for_status()
            return True
        except httpx.HTTPError as exc:
            logger.warning("could not ensure ollama model", extra={"error": str(exc)})
            return False

    async def aclose(self) -> None:
        await self._client.aclose()
