from __future__ import annotations

import logging

from shared.event_bus import EventBus
from shared.events import NotificationSent, OrderCancelled, OrderCompleted, Topic

logger = logging.getLogger("notification_service.handlers")

# In-memory record of sent notifications, exposed via a debug endpoint so
# you can see the saga's final step complete without needing a real email/
# SMS provider. Swap `_send` for a real provider call (SES, Twilio, etc.)
# in production - nothing else here would need to change.
sent_notifications: list[dict] = []


async def _send(order_id: str, message: str) -> None:
    sent_notifications.append({"order_id": order_id, "message": message})
    logger.info("Notification sent for order %s: %s", order_id, message)


def register_handlers(bus: EventBus) -> None:
    async def on_order_completed(event: OrderCompleted) -> None:
        message = f"Your order {event.order_id} has been completed. Thank you!"
        await _send(event.order_id, message)
        await bus.publish(Topic.NOTIFICATION_SENT, NotificationSent(order_id=event.order_id, channel="email", message=message))

    async def on_order_cancelled(event: OrderCancelled) -> None:
        message = f"Your order {event.order_id} was cancelled: {event.reason}"
        await _send(event.order_id, message)
        await bus.publish(Topic.NOTIFICATION_SENT, NotificationSent(order_id=event.order_id, channel="email", message=message))

    bus.subscribe(Topic.ORDER_COMPLETED, on_order_completed)
    bus.subscribe(Topic.ORDER_CANCELLED, on_order_cancelled)
