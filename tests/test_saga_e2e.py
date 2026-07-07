from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.event_bus import InMemoryEventBus  # noqa: E402
from shared.events import OrderCreated, Topic  # noqa: E402


def load_service(service_name: str, db_url: str) -> dict:
    """
    Every service's internal package is named `app`, which is fine in
    production (each runs in its own process/container) but collides if
    imported side-by-side in one test process. This loads one service's
    `app` package fresh, grabs references to what the test needs, then
    clears the cache so the next service gets its own clean `app` package.
    Already-grabbed references keep working via their own module globals.
    """
    service_dir = ROOT / "services" / service_name
    for mod_name in list(sys.modules):
        if mod_name == "app" or mod_name.startswith("app."):
            del sys.modules[mod_name]
    if db_url:
        os.environ["DATABASE_URL"] = db_url
    sys.path.insert(0, str(service_dir))
    try:
        import app.handlers  # noqa: F401 - import chain pulls in db/models/config too
        return {
            name: sys.modules[f"app.{name}"]
            for name in ("config", "db", "models", "handlers")
            if f"app.{name}" in sys.modules
        }
    finally:
        sys.path.remove(str(service_dir))


async def _bootstrap_saga(tmp_path, decline_threshold: float = 1000.0) -> tuple[InMemoryEventBus, dict, dict, dict, dict]:
    bus = InMemoryEventBus()

    order_ns = load_service("order_service", f"sqlite+aiosqlite:///{tmp_path}/order.db")
    await order_ns["db"].init_db()
    order_ns["handlers"].register_handlers(bus)

    inv_ns = load_service("inventory_service", f"sqlite+aiosqlite:///{tmp_path}/inventory.db")
    await inv_ns["db"].init_db()
    await inv_ns["handlers"].seed_stock()
    inv_ns["handlers"].register_handlers(bus)

    os.environ["DECLINE_AMOUNT_THRESHOLD"] = str(decline_threshold)
    pay_ns = load_service("payment_service", f"sqlite+aiosqlite:///{tmp_path}/payment.db")
    await pay_ns["db"].init_db()
    pay_ns["handlers"].register_handlers(bus)
    del os.environ["DECLINE_AMOUNT_THRESHOLD"]

    notif_ns = load_service("notification_service", "")
    notif_ns["handlers"].register_handlers(bus)

    return bus, order_ns, inv_ns, pay_ns, notif_ns


async def _create_order(order_ns: dict, sku: str, quantity: int, amount: Decimal, bus: InMemoryEventBus):
    order = order_ns["models"].Order(customer_id="cust-1", sku=sku, quantity=quantity, amount=amount)
    async with order_ns["db"].SessionLocal() as session:
        session.add(order)
        await session.commit()
        await session.refresh(order)

    await bus.publish(
        Topic.ORDER_CREATED,
        OrderCreated(order_id=order.id, customer_id=order.customer_id, sku=order.sku, quantity=order.quantity, amount=order.amount),
    )
    return order


@pytest.mark.asyncio
async def test_full_saga_happy_path(tmp_path):
    bus, order_ns, inv_ns, pay_ns, notif_ns = await _bootstrap_saga(tmp_path)

    order = await _create_order(order_ns, "SKU-WIDGET", 2, Decimal("50.00"), bus)

    async with order_ns["db"].SessionLocal() as session:
        refreshed = await session.get(order_ns["models"].Order, order.id)
        assert refreshed.status == order_ns["models"].OrderStatus.COMPLETED

    async with inv_ns["db"].SessionLocal() as session:
        item = await session.get(inv_ns["models"].StockItem, "SKU-WIDGET")
        assert item.reserved_quantity == 2
        assert item.available_quantity == 48

    async with pay_ns["db"].SessionLocal() as session:
        result = await session.execute(
            select(pay_ns["models"].PaymentTransaction).where(pay_ns["models"].PaymentTransaction.order_id == order.id)
        )
        tx = result.scalar_one()
        assert tx.status == "COMPLETED"

    assert any(n["order_id"] == order.id for n in notif_ns["handlers"].sent_notifications)


@pytest.mark.asyncio
async def test_payment_failure_triggers_compensation(tmp_path):
    """The core saga concept: payment fails AFTER stock was reserved, so the
    reservation must be explicitly rolled back (compensating transaction)."""
    bus, order_ns, inv_ns, pay_ns, notif_ns = await _bootstrap_saga(tmp_path, decline_threshold=100.0)

    order = await _create_order(order_ns, "SKU-WIDGET", 3, Decimal("2000.00"), bus)

    async with order_ns["db"].SessionLocal() as session:
        refreshed = await session.get(order_ns["models"].Order, order.id)
        assert refreshed.status == order_ns["models"].OrderStatus.CANCELLED
        assert "payment failed" in refreshed.cancellation_reason

    # stock must be back to its original state - this is the compensation check
    async with inv_ns["db"].SessionLocal() as session:
        item = await session.get(inv_ns["models"].StockItem, "SKU-WIDGET")
        assert item.reserved_quantity == 0
        assert item.available_quantity == 50

    assert any("cancelled" in n["message"].lower() for n in notif_ns["handlers"].sent_notifications)


@pytest.mark.asyncio
async def test_insufficient_stock_cancels_without_touching_payment(tmp_path):
    bus, order_ns, inv_ns, pay_ns, notif_ns = await _bootstrap_saga(tmp_path)

    order = await _create_order(order_ns, "SKU-GADGET", 999, Decimal("10.00"), bus)

    async with order_ns["db"].SessionLocal() as session:
        refreshed = await session.get(order_ns["models"].Order, order.id)
        assert refreshed.status == order_ns["models"].OrderStatus.CANCELLED
        assert "inventory unavailable" in refreshed.cancellation_reason

    # payment should never have been attempted for this order
    async with pay_ns["db"].SessionLocal() as session:
        result = await session.execute(
            select(pay_ns["models"].PaymentTransaction).where(pay_ns["models"].PaymentTransaction.order_id == order.id)
        )
        assert result.scalar_one_or_none() is None
