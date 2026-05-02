from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, DateTime, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from common.time import utcnow

from .database import Base


class LedgerPost(Base):
    __tablename__ = "ledger_posts"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),
    )

    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    source_transaction_id: Mapped[str] = mapped_column(String(36), nullable=False)

    status: Mapped[str] = mapped_column(String(64), nullable=False)

    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    debit_account_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    credit_account_ref: Mapped[str] = mapped_column(String(128), nullable=False)

    upstream_reference: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "source_system",
            "source_event_id",
            name="uq_ledger_post_source_event",
        ),
        UniqueConstraint(
            "source_system",
            "source_transaction_id",
            name="uq_ledger_post_source_transaction",
        ),
        Index(
            "ix_ledger_posts_source_transaction_id",
            "source_transaction_id",
        ),
    )
