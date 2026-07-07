from __future__ import annotations

import logging

from sqlalchemy import select

from app.db import SessionLocal
from app.models import StockItem
from shared.event_bus import EventBus
from shared.events import (
    InventoryFailed,
    InventoryReleaseRequested,
    InventoryReserved,
    OrderCreated,
    Topic,
)

logger = logging.getLogger("inventory_service.handlers")


async def seed_stock() -> None:
    """Demo seed data so the saga has something to reserve against out of the box."""
    demo_stock = {"SKU-WIDGET": 50, "SKU-GADGET": 10, "SKU-OUT-OF-STOCK": 0}
    async with SessionLocal() as session:
        for sku, qty in demo_stock.items():
            existing = await session.get(StockItem, sku)
            if existing is None:
                session.add(StockItem(sku=sku, available_quantity=qty, reserved_quantity=0))
        await session.commit()


def register_handlers(bus: EventBus) -> None:
    async def on_order_created(event: OrderCreated) -> None:
        async with SessionLocal() as session:
            result = await session.execute(select(StockItem).where(StockItem.sku == event.sku).with_for_update())
            item = result.scalar_one_or_none()

            if item is None or item.available_quantity < event.quantity:
                await bus.publish(
                    Topic.INVENTORY_FAILED,
                    InventoryFailed(
                        order_id=event.order_id,
                        sku=event.sku,
                        quantity=event.quantity,
                        reason="insufficient stock" if item else "unknown sku",
                    ),
                )
                logger.info("Order %s: inventory reservation failed for %s", event.order_id, event.sku)
                return

            item.available_quantity -= event.quantity
            item.reserved_quantity += event.quantity
            await session.commit()

        await bus.publish(
            Topic.INVENTORY_RESERVED,
            InventoryReserved(order_id=event.order_id, sku=event.sku, quantity=event.quantity, amount=event.amount),
        )
        logger.info("Order %s: reserved %d x %s", event.order_id, event.quantity, event.sku)

    async def on_release_requested(event: InventoryReleaseRequested) -> None:
        """Compensating transaction: a later saga step failed after we'd
        already reserved stock, so undo the reservation."""
        async with SessionLocal() as session:
            result = await session.execute(select(StockItem).where(StockItem.sku == event.sku).with_for_update())
            item = result.scalar_one_or_none()
            if item is None:
                return
            item.reserved_quantity = max(0, item.reserved_quantity - event.quantity)
            item.available_quantity += event.quantity
            await session.commit()

        logger.info("Order %s: released %d x %s back to stock (%s)", event.order_id, event.quantity, event.sku, event.reason)

    bus.subscribe(Topic.ORDER_CREATED, on_order_created)
    bus.subscribe(Topic.INVENTORY_RELEASE_REQUESTED, on_release_requested)
