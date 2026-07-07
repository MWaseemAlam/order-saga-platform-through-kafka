from __future__ import annotations

import logging

from app.db import SessionLocal
from app.models import Order, OrderStatus
from shared.event_bus import EventBus
from shared.events import (
    InventoryFailed,
    InventoryReleaseRequested,
    OrderCancelled,
    OrderCompleted,
    PaymentCompleted,
    PaymentFailed,
    Topic,
)

logger = logging.getLogger("order_service.handlers")


def register_handlers(bus: EventBus) -> None:
    async def on_inventory_failed(event: InventoryFailed) -> None:
        """Nothing was reserved, so no compensation needed - just cancel."""
        async with SessionLocal() as session:
            order = await session.get(Order, event.order_id)
            if order is None:
                return
            order.status = OrderStatus.CANCELLED
            order.cancellation_reason = f"inventory unavailable: {event.reason}"
            await session.commit()

        await bus.publish(Topic.ORDER_CANCELLED, OrderCancelled(order_id=event.order_id, reason=order.cancellation_reason))
        logger.info("Order %s cancelled (inventory failed)", event.order_id)

    async def on_payment_completed(event: PaymentCompleted) -> None:
        async with SessionLocal() as session:
            order = await session.get(Order, event.order_id)
            if order is None:
                return
            order.status = OrderStatus.COMPLETED
            await session.commit()

        await bus.publish(Topic.ORDER_COMPLETED, OrderCompleted(order_id=event.order_id))
        logger.info("Order %s completed", event.order_id)

    async def on_payment_failed(event: PaymentFailed) -> None:
        """
        Payment failed AFTER inventory was already reserved - this is the
        compensating-transaction step of the saga: we have to explicitly
        undo the reservation rather than just marking our own order failed,
        because the inventory service has no way of knowing on its own that
        a later step failed.
        """
        async with SessionLocal() as session:
            order = await session.get(Order, event.order_id)
            if order is None:
                return
            order.status = OrderStatus.CANCELLED
            order.cancellation_reason = f"payment failed: {event.reason}"
            sku, quantity = order.sku, order.quantity
            await session.commit()

        await bus.publish(
            Topic.INVENTORY_RELEASE_REQUESTED,
            InventoryReleaseRequested(order_id=event.order_id, sku=sku, quantity=quantity, reason="payment_failed"),
        )
        await bus.publish(Topic.ORDER_CANCELLED, OrderCancelled(order_id=event.order_id, reason=order.cancellation_reason))
        logger.info("Order %s cancelled (payment failed), compensation dispatched", event.order_id)

    bus.subscribe(Topic.INVENTORY_FAILED, on_inventory_failed)
    bus.subscribe(Topic.PAYMENT_COMPLETED, on_payment_completed)
    bus.subscribe(Topic.PAYMENT_FAILED, on_payment_failed)
