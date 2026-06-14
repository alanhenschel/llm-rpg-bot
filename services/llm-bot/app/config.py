"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Identity / logging
    service_name: str = "llm-bot"
    log_level: str = "INFO"

    # Kafka
    kafka_brokers: str = "kafka:9092"
    topic_inbound: str = "whatsapp.messages.inbound"
    topic_outbound: str = "whatsapp.messages.outbound"
    consumer_group: str = "llm-bot"

    # Redis (idempotency)
    redis_url: str = "redis://redis:6379/0"
    idempotency_ttl_seconds: int = 60 * 60 * 24  # 24h

    # Ollama
    ollama_base_url: str = "http://ollama:11434"
    ollama_model: str = "llama3"
    ollama_timeout_seconds: float = 120.0

    # RAG
    chroma_path: str = "/data/chroma"
    rag_docs_path: str = "/app/data/rpg_docs"
    embedding_model: str = "all-MiniLM-L6-v2"
    rag_collection: str = "rpg_knowledge"
    rag_top_k: int = 4
    chunk_size: int = 500
    chunk_overlap: int = 50

    # gRPC server (hot path from gateway)
    grpc_port: int = 50051

    # API
    http_host: str = "0.0.0.0"
    http_port: int = 8000

    @property
    def brokers_list(self) -> list[str]:
        return [b.strip() for b in self.kafka_brokers.split(",") if b.strip()]


settings = Settings()
