import logging
import time
from datetime import timedelta

from common.db import DatabaseConflictError, commit_or_raise_conflict
from common.time import utcnow

from .clients import LedgerClient
from .database import SessionLocal
from .enums import LedgerDispatchStatus, OutboxStatus
from .models import TransactionLedgerOutboxMessage, TransactionRequestJournal
from .repositories import TransactionLedgerOutboxRepository
from .settings import get_settings


logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2
MAX_RETRIES = 5


def backoff_seconds(retry_count: int) -> int:
    if retry_count <= 1:
        return 5

    if retry_count == 2:
        return 15

    if retry_count == 3:
        return 60

    return 300


class TransactionLedgerOutboxWorker:
    def __init__(self) -> None:
        settings = get_settings()
        self.ledger_client = LedgerClient(settings.ledger_service_url)

    def process_once(self) -> bool:
        message_id = self._claim_next_message()

        if message_id is None:
            return False

        try:
            self._publish_message(message_id)
        except Exception:
            logger.exception("unexpected outbox worker error", extra={"message_id": message_id})
            return True

        return True

    def _claim_next_message(self) -> str | None:
        with SessionLocal() as db:
            outbox_repo = TransactionLedgerOutboxRepository(db)

            message = outbox_repo.get_next_due_for_update()

            if message is None:
                return None

            journal = db.get(TransactionRequestJournal, message.transaction_journal_id)

            message.status = OutboxStatus.PROCESSING.value

            if journal is not None:
                journal.ledger_dispatch_status = LedgerDispatchStatus.PROCESSING.value

            message_id = message.id

            try:
                commit_or_raise_conflict(
                    db,
                    message="failed to claim outbox message",
                )
            except DatabaseConflictError:
                logger.exception("failed to claim outbox message")
                return None

            return message_id

    def _publish_message(self, message_id: str) -> None:
        with SessionLocal() as db:
            message = db.get(TransactionLedgerOutboxMessage, message_id)

            if message is None:
                logger.warning("outbox message disappeared", extra={"message_id": message_id})
                return

            payload = message.payload

        try:
            self.ledger_client.post_ledger(payload)
        except Exception as exc:
            self._mark_failed(message_id=message_id, error=exc)
            return

        self._mark_published(message_id=message_id)

    def _mark_published(self, *, message_id: str) -> None:
        with SessionLocal() as db:
            message = db.get(TransactionLedgerOutboxMessage, message_id)

            if message is None:
                logger.warning("outbox message disappeared", extra={"message_id": message_id})
                return

            journal = db.get(TransactionRequestJournal, message.transaction_journal_id)

            message.status = OutboxStatus.PUBLISHED.value
            message.published_at = utcnow()
            message.last_error = None

            if journal is not None:
                journal.ledger_dispatch_status = LedgerDispatchStatus.POSTED.value

            try:
                commit_or_raise_conflict(
                    db,
                    message="failed to mark outbox message as published",
                )
            except DatabaseConflictError:
                logger.exception(
                    "failed to mark outbox message as published",
                    extra={"message_id": message_id},
                )

    def _mark_failed(self, *, message_id: str, error: Exception) -> None:
        with SessionLocal() as db:
            message = db.get(TransactionLedgerOutboxMessage, message_id)

            if message is None:
                logger.warning("outbox message disappeared", extra={"message_id": message_id})
                return

            journal = db.get(TransactionRequestJournal, message.transaction_journal_id)

            message.retry_count += 1
            message.last_error = str(error)

            if message.retry_count >= MAX_RETRIES:
                message.status = OutboxStatus.DLQ.value

                if journal is not None:
                    journal.ledger_dispatch_status = LedgerDispatchStatus.DLQ.value
            else:
                message.status = OutboxStatus.FAILED.value
                message.next_retry_at = utcnow() + timedelta(
                    seconds=backoff_seconds(message.retry_count)
                )

                if journal is not None:
                    journal.ledger_dispatch_status = LedgerDispatchStatus.FAILED.value

            try:
                commit_or_raise_conflict(
                    db,
                    message="failed to update failed outbox message",
                )
            except DatabaseConflictError:
                logger.exception(
                    "failed to update failed outbox message",
                    extra={"message_id": message_id},
                )


def run_forever() -> None:
    logging.basicConfig(level=logging.INFO)

    worker = TransactionLedgerOutboxWorker()

    logger.info("transaction ledger outbox worker started")

    while True:
        did_work = worker.process_once()

        if not did_work:
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_forever()
