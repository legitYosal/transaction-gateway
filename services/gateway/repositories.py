from sqlalchemy import select
from sqlalchemy.orm import Session

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

    def get_by_id(
        self,
        journal_id: str,
    ) -> TransactionRequestJournal | None:
        return self.db.get(TransactionRequestJournal, journal_id)

    def add(self, journal: TransactionRequestJournal) -> None:
        self.db.add(journal)


class TransactionLedgerOutboxRepository:
    def __init__(self, db: Session):
        self.db = db

    def add(self, message: TransactionLedgerOutboxMessage) -> None:
        self.db.add(message)
