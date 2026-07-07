from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session, init_db
from app.handlers import register_handlers, seed_stock
from app.models import StockItem
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
    await init_db()
    await seed_stock()
    register_handlers(_bus)
    await _bus.start()
    logger.info("%s started", settings.service_name)
    yield
    await _bus.stop()


app = FastAPI(title="Inventory Service", lifespan=lifespan)
instrument_app(app, settings.service_name)


@app.get("/stock")
async def list_stock(session: AsyncSession = Depends(get_session)) -> list[dict]:
    """Read-only endpoint so you can see reservation/release happen live while testing the saga."""
    result = await session.execute(select(StockItem))
    return [
        {"sku": s.sku, "available": s.available_quantity, "reserved": s.reserved_quantity}
        for s in result.scalars().all()
    ]


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": settings.service_name}
