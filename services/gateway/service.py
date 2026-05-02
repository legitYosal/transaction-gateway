from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy.orm import Session

from common.db import DatabaseConflictError, commit_or_raise_conflict
from common.hashing import stable_json_hash

from .clients import FakeAuthClient, FakeUpstreamVerifierClient
from .enums import (
    LedgerDispatchStatus,
    OutboxEventType,
    OutboxStatus,
    TransactionJournalStatus,
    UpstreamTransactionStatus,
)
from .models import TransactionLedgerOutboxMessage, TransactionRequestJournal
from .repositories import (
    TransactionLedgerOutboxRepository,
    TransactionRequestJournalRepository,
)
from .schemas import CompleteTransactionRequest, CompleteTransactionResponse


class CompleteTransactionRequestService:
    def __init__(self):
        self.auth_client = FakeAuthClient()
        self.upstream_client = FakeUpstreamVerifierClient()

    def complete(
        self,
        *,
        db: Session,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        idempotency_key: str,
        authorization_header: str | None,
    ) -> CompleteTransactionResponse:
        request_hash = self._build_request_hash(
            transaction_id=transaction_id,
            payload=payload,
        )

        journal = self._reserve_or_replay(
            db=db,
            transaction_id=transaction_id,
            payload=payload,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

        try:
            self.auth_client.authorize(authorization_header)
        except PermissionError:
            journal.status = TransactionJournalStatus.FAILED.value
            commit_or_raise_conflict(db, message="failed to update unauthorized journal")
            raise HTTPException(status_code=401, detail="unauthorized")

        upstream_result = self.upstream_client.complete_transaction(
            transaction_id=transaction_id,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            account_id=payload.account_id,
            merchant_id=payload.merchant_id,
            external_reference=payload.external_reference,
        )

        if upstream_result.status == UpstreamTransactionStatus.DECLINED:
            return self._finalize_declined(
                db=db,
                journal=journal,
                transaction_id=transaction_id,
                upstream_response=upstream_result.raw_response,
            )

        return self._finalize_approved(
            db=db,
            journal=journal,
            transaction_id=transaction_id,
            payload=payload,
            upstream_reference=upstream_result.upstream_reference,
            upstream_response=upstream_result.raw_response,
        )

    def _reserve_or_replay(
        self,
        *,
        db: Session,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        idempotency_key: str,
        request_hash: str,
    ) -> TransactionRequestJournal:
        journal_repo = TransactionRequestJournalRepository(db)

        existing_by_key = journal_repo.get_by_merchant_idempotency_key(
            merchant_id=payload.merchant_id,
            idempotency_key=idempotency_key,
        )

        if existing_by_key:
            return self._handle_existing_journal(
                existing=existing_by_key,
                request_hash=request_hash,
            )

        existing_by_transaction = journal_repo.get_by_transaction_id(
            transaction_id=transaction_id,
        )

        if existing_by_transaction:
            return self._handle_existing_journal(
                existing=existing_by_transaction,
                request_hash=request_hash,
            )

        journal = TransactionRequestJournal(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            account_id=payload.account_id,
            merchant_id=payload.merchant_id,
            external_reference=payload.external_reference,
            status=TransactionJournalStatus.RECEIVED.value,
            upstream_status=None,
            upstream_reference=None,
            upstream_response=None,
            response_status_code=None,
            response_body=None,
            ledger_dispatch_status=LedgerDispatchStatus.NOT_REQUIRED.value,
            request_id=f"req_{uuid4()}",
        )

        journal_repo.add(journal)

        try:
            commit_or_raise_conflict(
                db,
                message="duplicate transaction completion request",
            )
        except DatabaseConflictError:
            # Another request probably inserted the same transaction_id or
            # merchant_id/idempotency_key between our read and insert.
            raise HTTPException(
                status_code=409,
                detail="transaction completion is already being processed",
            )

        return journal

    def _handle_existing_journal(
        self,
        *,
        existing: TransactionRequestJournal,
        request_hash: str,
    ) -> TransactionRequestJournal:
        if existing.request_hash != request_hash:
            raise HTTPException(
                status_code=409,
                detail="same idempotency key or transaction used with different payload",
            )

        if existing.response_body is None:
            raise HTTPException(
                status_code=409,
                detail="transaction completion is already being processed",
            )

        response = CompleteTransactionResponse.model_validate(existing.response_body)

        # Raise a special HTTPException with the response as detail? No.
        # Better: return through normal service path is awkward here,
        # so we use a small internal exception.
        raise IdempotentReplay(response)

    def _finalize_declined(
        self,
        *,
        db: Session,
        journal: TransactionRequestJournal,
        transaction_id: str,
        upstream_response: dict,
    ) -> CompleteTransactionResponse:
        response = CompleteTransactionResponse(
            transaction_id=transaction_id,
            status=UpstreamTransactionStatus.DECLINED,
            upstream_reference=None,
            ledger_dispatch_status=LedgerDispatchStatus.NOT_REQUIRED,
            request_id=journal.request_id,
        )

        journal.status = TransactionJournalStatus.UPSTREAM_DECLINED.value
        journal.upstream_status = UpstreamTransactionStatus.DECLINED.value
        journal.upstream_reference = None
        journal.upstream_response = upstream_response
        journal.response_status_code = 200
        journal.response_body = response.model_dump(mode="json")
        journal.ledger_dispatch_status = LedgerDispatchStatus.NOT_REQUIRED.value

        try:
            commit_or_raise_conflict(db, message="failed to finalize declined transaction")
        except DatabaseConflictError:
            raise HTTPException(
                status_code=409,
                detail="failed to finalize declined transaction",
            )

        return response

    def _finalize_approved(
        self,
        *,
        db: Session,
        journal: TransactionRequestJournal,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        upstream_reference: str | None,
        upstream_response: dict,
    ) -> CompleteTransactionResponse:
        response = CompleteTransactionResponse(
            transaction_id=transaction_id,
            status=UpstreamTransactionStatus.APPROVED,
            upstream_reference=upstream_reference,
            ledger_dispatch_status=LedgerDispatchStatus.PENDING,
            request_id=journal.request_id,
        )

        journal.status = TransactionJournalStatus.UPSTREAM_APPROVED.value
        journal.upstream_status = UpstreamTransactionStatus.APPROVED.value
        journal.upstream_reference = upstream_reference
        journal.upstream_response = upstream_response
        journal.response_status_code = 200
        journal.response_body = response.model_dump(mode="json")
        journal.ledger_dispatch_status = LedgerDispatchStatus.PENDING.value

        outbox_repo = TransactionLedgerOutboxRepository(db)

        outbox = TransactionLedgerOutboxMessage(
            transaction_journal_id=journal.id,
            event_type=OutboxEventType.LEDGER_POST_REQUESTED.value,
            status=OutboxStatus.PENDING.value,
            payload={
                "source_system": "transaction-gateway",
                "source_event_id": journal.id,
                "source_transaction_id": transaction_id,
                "amount_minor": payload.amount_minor,
                "currency": payload.currency,
                "account_id": payload.account_id,
                "merchant_id": payload.merchant_id,
                "upstream_reference": upstream_reference,
            },
        )

        outbox_repo.add(outbox)

        try:
            # This commit atomically saves:
            #   1. final journal response
            #   2. ledger outbox message
            commit_or_raise_conflict(db, message="failed to finalize approved transaction")
        except DatabaseConflictError:
            raise HTTPException(
                status_code=409,
                detail="failed to finalize approved transaction",
            )

        return response

    def _build_request_hash(
        self,
        *,
        transaction_id: str,
        payload: CompleteTransactionRequest,
    ) -> str:
        return stable_json_hash(
            {
                "transaction_id": transaction_id,
                "amount_minor": payload.amount_minor,
                "currency": payload.currency,
                "account_id": payload.account_id,
                "merchant_id": payload.merchant_id,
                "external_reference": payload.external_reference,
            }
        )


class IdempotentReplay(Exception):
    def __init__(self, response: CompleteTransactionResponse):
        self.response = response
