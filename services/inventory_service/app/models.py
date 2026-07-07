from __future__ import annotations

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StockItem(Base):
    __tablename__ = "stock_items"

    sku: Mapped[str] = mapped_column(String, primary_key=True)
    available_quantity: Mapped[int] = mapped_column(nullable=False, default=0)
    reserved_quantity: Mapped[int] = mapped_column(nullable=False, default=0)
