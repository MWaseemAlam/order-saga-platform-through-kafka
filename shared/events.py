from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


def new_id() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Topic(str, Enum):
    ORDER_CREATED = "order.created"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_FAILED = "inventory.failed"
    INVENTORY_RELEASE_REQUESTED = "inventory.release_requested"
    PAYMENT_COMPLETED = "payment.completed"
    PAYMENT_FAILED = "payment.failed"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCELLED = "order.cancelled"
    NOTIFICATION_SENT = "notification.sent"


class BaseEvent(BaseModel):
    """
    Every event carries its own id, the originating order id (so every
    service can correlate events belonging to the same saga), and a
    timestamp. Keeping this consistent across services is what makes
    distributed tracing/log correlation possible later.
    """

    event_id: str = Field(default_factory=new_id)
    order_id: str
    occurred_at: datetime = Field(default_factory=utcnow)


class OrderCreated(BaseEvent):
    customer_id: str
    sku: str
    quantity: int
    amount: Decimal


class InventoryReserved(BaseEvent):
    sku: str
    quantity: int
    amount: Decimal


class InventoryFailed(BaseEvent):
    sku: str
    quantity: int
    reason: str


class InventoryReleaseRequested(BaseEvent):
    sku: str
    quantity: int
    reason: str


class PaymentCompleted(BaseEvent):
    amount: Decimal
    transaction_id: str


class PaymentFailed(BaseEvent):
    amount: Decimal
    reason: str


class OrderCompleted(BaseEvent):
    pass


class OrderCancelled(BaseEvent):
    reason: str


class NotificationSent(BaseEvent):
    channel: str
    message: str


EVENT_REGISTRY: dict[Topic, type[BaseEvent]] = {
    Topic.ORDER_CREATED: OrderCreated,
    Topic.INVENTORY_RESERVED: InventoryReserved,
    Topic.INVENTORY_FAILED: InventoryFailed,
    Topic.INVENTORY_RELEASE_REQUESTED: InventoryReleaseRequested,
    Topic.PAYMENT_COMPLETED: PaymentCompleted,
    Topic.PAYMENT_FAILED: PaymentFailed,
    Topic.ORDER_COMPLETED: OrderCompleted,
    Topic.ORDER_CANCELLED: OrderCancelled,
    Topic.NOTIFICATION_SENT: NotificationSent,
}
