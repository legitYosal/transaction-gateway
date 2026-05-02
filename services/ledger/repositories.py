from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import LedgerPost


class LedgerPostRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_source_event(
        self,
        *,
        source_system: str,
        source_event_id: str,
    ) -> LedgerPost | None:
        return self.db.scalar(
            select(LedgerPost).where(
                LedgerPost.source_system == source_system,
                LedgerPost.source_event_id == source_event_id,
            )
        )

    def get_by_source_transaction(
        self,
        *,
        source_system: str,
        source_transaction_id: str,
    ) -> LedgerPost | None:
        return self.db.scalar(
            select(LedgerPost).where(
                LedgerPost.source_system == source_system,
                LedgerPost.source_transaction_id == source_transaction_id,
            )
        )

    def add(self, post: LedgerPost) -> None:
        self.db.add(post)
