from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "payment-service"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/payment_db"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "payment-service-group"

    # Demo failure injection: any amount at or above this "declines" so the
    # compensation path is easy to trigger and demo without extra config.
    decline_amount_threshold: float = 1000.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
