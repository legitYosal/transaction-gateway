from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.time import utcnow

from .enums import OutboxStatus
from .models import TransactionLedgerOutboxMessage, TransactionRequestJournal


class TransactionRequestJournalRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_merchant_idempotency_key(
        self,
        *,
        merchant_id: str,
        idempotency_key: str,
    ) -> TransactionRequestJournal | None:
        return self.db.scalar(
            select(TransactionRequestJournal).where(
                TransactionRequestJournal.merchant_id == merchant_id,
                TransactionRequestJournal.idempotency_key == idempotency_key,
            )
        )

    def get_by_transaction_id(
        self,
        *,
        transaction_id: str,
    ) -> TransactionRequestJournal | None:
        return self.db.scalar(
            select(TransactionRequestJournal).where(
                TransactionRequestJournal.transaction_id == transaction_id,
            )
        )

    def add(self, journal: TransactionRequestJournal) -> None:
        self.db.add(journal)


class TransactionLedgerOutboxRepository:
    def __init__(self, db: Session):
        self.db = db

    def add(self, message: TransactionLedgerOutboxMessage) -> None:
        self.db.add(message)

    def get_by_id(self, message_id: str) -> TransactionLedgerOutboxMessage | None:
        return self.db.get(TransactionLedgerOutboxMessage, message_id)

    def get_next_due_for_update(self) -> TransactionLedgerOutboxMessage | None:
        return self.db.scalar(
            select(TransactionLedgerOutboxMessage)
            .where(
                TransactionLedgerOutboxMessage.status.in_(
                    [
                        OutboxStatus.PENDING.value,
                        OutboxStatus.FAILED.value,
                    ]
                ),
                TransactionLedgerOutboxMessage.next_retry_at <= utcnow(),
            )
            .order_by(TransactionLedgerOutboxMessage.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )

    def get_stale_processing_for_update(
        self,
        *,
        older_than: datetime,
        limit: int = 20,
    ) -> list[TransactionLedgerOutboxMessage]:
        return list(
            self.db.scalars(
                select(TransactionLedgerOutboxMessage)
                .where(
                    TransactionLedgerOutboxMessage.status == OutboxStatus.PROCESSING.value,
                    TransactionLedgerOutboxMessage.updated_at <= older_than,
                )
                .order_by(TransactionLedgerOutboxMessage.updated_at)
                .with_for_update(skip_locked=True)
                .limit(limit)
            ).all()
        )
