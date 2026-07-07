from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session, init_db
from app.handlers import register_handlers
from app.models import Order
from app.schemas import CreateOrderRequest, OrderResponse
from shared.event_bus import EventBus, InMemoryEventBus, KafkaEventBus
from shared.events import OrderCreated, Topic
from shared.observability import configure_logging, instrument_app

settings = get_settings()
logger = configure_logging(settings.service_name)

# USE_FAKE_BUS=1 runs against the in-memory bus (used by the test suite and
# for quick local smoke-testing without a Kafka broker running).
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


app = FastAPI(title="Order Service", lifespan=lifespan)
instrument_app(app, settings.service_name)


def get_bus() -> EventBus:
    return _bus


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(
    request: CreateOrderRequest,
    session: AsyncSession = Depends(get_session),
    bus: EventBus = Depends(get_bus),
) -> Order:
    order = Order(
        customer_id=request.customer_id,
        sku=request.sku,
        quantity=request.quantity,
        amount=request.amount,
    )
    session.add(order)
    await session.commit()
    await session.refresh(order)

    await bus.publish(
        Topic.ORDER_CREATED,
        OrderCreated(
            order_id=order.id,
            customer_id=order.customer_id,
            sku=order.sku,
            quantity=order.quantity,
            amount=order.amount,
        ),
    )
    logger.info("Order %s created, awaiting saga completion", order.id)
    return order


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str, session: AsyncSession = Depends(get_session)) -> Order:
    order = await session.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": settings.service_name}
