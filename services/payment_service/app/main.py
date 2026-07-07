from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session, init_db
from app.handlers import register_handlers
from app.models import PaymentTransaction
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
    register_handlers(_bus)
    await _bus.start()
    logger.info("%s started", settings.service_name)
    yield
    await _bus.stop()


app = FastAPI(title="Payment Service", lifespan=lifespan)
instrument_app(app, settings.service_name)


@app.get("/transactions/{order_id}")
async def get_transaction(order_id: str, session: AsyncSession = Depends(get_session)) -> dict:
    result = await session.execute(select(PaymentTransaction).where(PaymentTransaction.order_id == order_id))
    tx = result.scalar_one_or_none()
    if tx is None:
        return {"order_id": order_id, "status": "not_found"}
    return {"order_id": tx.order_id, "amount": str(tx.amount), "status": tx.status}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": settings.service_name}
