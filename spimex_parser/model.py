from typing import Any
from datetime import date, datetime
from sqlalchemy import DateTime, String
from sqlalchemy.orm import mapped_column, Mapped, DeclarativeBase


class Base(DeclarativeBase):
    pass


class TradeReportEntity(Base):

    __tablename__ = "trade_report_entities"

    id: Mapped[int] = mapped_column(primary_key=True)

    exchange_product_id: Mapped[str] = mapped_column(String(length=11))

    exchange_product_name: Mapped[str] = mapped_column(String(length=200))

    delivery_basis_name: Mapped[str] = mapped_column(String(length=50))

    volume: Mapped[int]

    total: Mapped[int]

    count: Mapped[int]

    date: Mapped[date]

    oil_id: Mapped[str] = mapped_column(String(length=4))

    delivery_basis_id: Mapped[str] = mapped_column(String(length=3))

    delivery_type_id: Mapped[str] = mapped_column(String(length=1))

    created_on: Mapped[datetime] = mapped_column(DateTime, default=datetime.now())

    updated_on: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now(), onupdate=datetime.now()
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.oil_id = self.exchange_product_id[:4]
        self.delivery_basis_id = self.exchange_product_id[4:7]
        self.delivery_type_id = self.exchange_product_id[-1]
