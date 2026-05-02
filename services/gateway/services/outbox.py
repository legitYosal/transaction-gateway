import logging
from datetime import timedelta

from common.db import DatabaseConflictError, commit_or_raise_conflict
from common.time import utcnow

from ..clients import LedgerClient
from ..database import SessionLocal
from ..enums import LedgerDispatchStatus, OutboxStatus
from ..models import TransactionLedgerOutboxMessage, TransactionRequestJournal
from ..repositories import TransactionLedgerOutboxRepository


logger = logging.getLogger(__name__)

MAX_RETRIES = 5
PROCESSING_TIMEOUT_SECONDS = 300


def backoff_seconds(retry_count: int) -> int:
    if retry_count <= 1:
        return 5

    if retry_count == 2:
        return 15

    if retry_count == 3:
        return 60

    return 300


class TransactionLedgerOutboxService:
    def __init__(self, *, ledger_client: LedgerClient):
        self.ledger_client = ledger_client

    def process_once(self) -> bool:
        self._recover_stale_processing_messages()

        message_id = self._claim_next_message()

        if message_id is None:
            return False

        self._publish_message(message_id)

        return True

    def _recover_stale_processing_messages(self) -> None:
        older_than = utcnow() - timedelta(seconds=PROCESSING_TIMEOUT_SECONDS)

        with SessionLocal() as db:
            outbox_repo = TransactionLedgerOutboxRepository(db)

            stale_messages = outbox_repo.get_stale_processing_for_update(
                older_than=older_than,
            )

            if not stale_messages:
                return

            for message in stale_messages:
                journal = db.get(TransactionRequestJournal, message.transaction_journal_id)

                message.status = OutboxStatus.FAILED.value
                message.last_error = "processing timeout; returned to retry queue"
                message.next_retry_at = utcnow()

                if journal is not None:
                    journal.ledger_dispatch_status = LedgerDispatchStatus.FAILED.value

                logger.warning(
                    "recovered stale processing outbox message message_id=%s",
                    message.id,
                )

            try:
                commit_or_raise_conflict(
                    db,
                    message="failed to recover stale processing outbox messages",
                )
            except DatabaseConflictError:
                logger.exception("failed to recover stale processing outbox messages")

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
                logger.exception("failed to claim outbox message message_id=%s", message_id)
                return None

            logger.info("claimed outbox message message_id=%s", message_id)

            return message_id

    def _publish_message(self, message_id: str) -> None:
        logger.info("publishing outbox message message_id=%s", message_id)

        with SessionLocal() as db:
            message = db.get(TransactionLedgerOutboxMessage, message_id)

            if message is None:
                logger.warning("outbox message disappeared message_id=%s", message_id)
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
                logger.warning("outbox message disappeared message_id=%s", message_id)
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
                    "failed to mark outbox message as published message_id=%s",
                    message_id,
                )
                return

            logger.info("published outbox message message_id=%s", message_id)

    def _mark_failed(self, *, message_id: str, error: Exception) -> None:
        with SessionLocal() as db:
            message = db.get(TransactionLedgerOutboxMessage, message_id)

            if message is None:
                logger.warning("outbox message disappeared message_id=%s", message_id)
                return

            journal = db.get(TransactionRequestJournal, message.transaction_journal_id)

            message.retry_count += 1
            message.last_error = str(error)

            if message.retry_count >= MAX_RETRIES:
                logger.error("moving outbox message to dlq message_id=%s error=%s", message_id, str(error))
                message.status = OutboxStatus.DLQ.value

                if journal is not None:
                    journal.ledger_dispatch_status = LedgerDispatchStatus.DLQ.value

                log_message = "moved outbox message to dlq message_id=%s error=%s"
            else:
                message.status = OutboxStatus.FAILED.value
                message.next_retry_at = utcnow() + timedelta(
                    seconds=backoff_seconds(message.retry_count)
                )

                if journal is not None:
                    journal.ledger_dispatch_status = LedgerDispatchStatus.FAILED.value

                log_message = "failed outbox message message_id=%s retry_count=%s error=%s"

            try:
                commit_or_raise_conflict(
                    db,
                    message="failed to update failed outbox message",
                )
            except DatabaseConflictError:
                logger.exception(
                    "failed to update failed outbox message message_id=%s",
                    message_id,
                )
                return

            if message.status == OutboxStatus.DLQ.value:
                logger.error(log_message, message_id, str(error))
            else:
                logger.warning(log_message, message_id, message.retry_count, str(error))
