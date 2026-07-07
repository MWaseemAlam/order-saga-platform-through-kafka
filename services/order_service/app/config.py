from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "order-service"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/order_db"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "order-service-group"


@lru_cache
def get_settings() -> Settings:
    return Settings()
