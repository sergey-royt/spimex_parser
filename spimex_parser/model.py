from typing import Annotated, Any
from decimal import Decimal
from sqlalchemy import Numeric, DateTime
from sqlalchemy.orm import mapped_column, Mapped
from datetime import date, datetime
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


intpk = Annotated[int, mapped_column(primary_key=True)]
currency = Annotated[Decimal, mapped_column(Numeric(20, 2))]


class TradeReportEntity(Base):

    __tablename__ = "trade_report_entities"

    id: Mapped[intpk]

    exchange_product_id: Mapped[str]

    exchange_product_name: Mapped[str]

    delivery_basis_name: Mapped[str]

    volume: Mapped[int]

    total: Mapped[currency]

    count: Mapped[int]

    date: Mapped[date]

    oil_id: Mapped[str]

    delivery_basis_id: Mapped[str]

    delivery_type_id: Mapped[str]

    created_on: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    updated_on: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.oil_id = self.exchange_product_id[:4]
        self.delivery_basis_id = self.exchange_product_id[4:7]
        self.delivery_type_id = self.exchange_product_id[-1]
