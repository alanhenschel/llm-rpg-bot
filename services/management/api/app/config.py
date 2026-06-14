"""Management API configuration."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "management-api"
    log_level: str = "INFO"

    database_url: str = "postgresql://app:app@postgres:5432/whatsapp"

    kafka_brokers: str = "kafka:9092"
    topic_outbound: str = "whatsapp.messages.outbound"

    # whatsapp-gateway base URL for the /connections proxy.
    gateway_url: str = "http://whatsapp-gateway:8080"

    http_host: str = "0.0.0.0"
    http_port: int = 9000

    cors_origins: str = "*"


settings = Settings()
