from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import get_settings
from app.handlers import register_handlers, sent_notifications
from shared.event_bus import EventBus, InMemoryEventBus, KafkaEventBus
from shared.observability import configure_logging, instrument_app

settings = get_settings()
logger = configure_logging(settings.service_name)

_bus: EventBus = (
    InMemoryEventBus()
    if os.getenv("USE_FAKE_BUS") == "1"
    else KafkaEventBus(settings.kafka_bootstrap_servers, settings.kafka_group_id)
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    register_handlers(_bus)
    await _bus.start()
    logger.info("%s started", settings.service_name)
    yield
    await _bus.stop()


app = FastAPI(title="Notification Service", lifespan=lifespan)
instrument_app(app, settings.service_name)


@app.get("/notifications")
async def list_notifications() -> list[dict]:
    return sent_notifications


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": settings.service_name}
