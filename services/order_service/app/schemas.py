from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from app.models import OrderStatus


class CreateOrderRequest(BaseModel):
    customer_id: str
    sku: str
    quantity: int = Field(gt=0)
    amount: Decimal = Field(gt=0)


class OrderResponse(BaseModel):
    id: str
    customer_id: str
    sku: str
    quantity: int
    amount: Decimal
    status: OrderStatus
    cancellation_reason: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
