from __future__ import annotations

import logging
import uuid

from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.models import PaymentTransaction
from shared.event_bus import EventBus
from shared.events import InventoryReserved, PaymentCompleted, PaymentFailed, Topic

logger = logging.getLogger("payment_service.handlers")
settings = get_settings()


def register_handlers(bus: EventBus) -> None:
    async def on_inventory_reserved(event: InventoryReserved) -> None:
        async with SessionLocal() as session:
            # Idempotency guard: Kafka delivers at-least-once, so a consumer
            # restart/rebalance can redeliver a message we already handled.
            # Without this check we'd double-charge the same order.
            existing = await session.execute(
                select(PaymentTransaction).where(PaymentTransaction.order_id == event.order_id)
            )
            if existing.scalar_one_or_none() is not None:
                logger.info("Order %s: payment already processed, skipping duplicate event", event.order_id)
                return

            # amount flows through from the original order (OrderCreated ->
            # InventoryReserved -> here), so payment always charges what the
            # customer actually agreed to, not a value re-derived locally.
            amount = event.amount

            success = amount < settings.decline_amount_threshold
            transaction = PaymentTransaction(
                order_id=event.order_id,
                amount=amount,
                status="COMPLETED" if success else "FAILED",
            )
            session.add(transaction)
            await session.commit()

        if success:
            await bus.publish(
                Topic.PAYMENT_COMPLETED,
                PaymentCompleted(order_id=event.order_id, amount=amount, transaction_id=transaction.id),
            )
            logger.info("Order %s: payment completed ($%s)", event.order_id, amount)
        else:
            await bus.publish(
                Topic.PAYMENT_FAILED,
                PaymentFailed(order_id=event.order_id, amount=amount, reason="declined: amount over threshold (demo rule)"),
            )
            logger.info("Order %s: payment declined ($%s)", event.order_id, amount)

    bus.subscribe(Topic.INVENTORY_RESERVED, on_inventory_reserved)
