from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy.orm import Session

from common.db import DatabaseConflictError, commit_or_raise_conflict
from common.hashing import stable_json_hash

from ..clients import FakeAuthClient, FakeUpstreamVerifierClient
from ..enums import (
    LedgerDispatchStatus,
    OutboxEventType,
    OutboxStatus,
    TransactionJournalStatus,
    UpstreamTransactionStatus,
)
from ..models import TransactionLedgerOutboxMessage, TransactionRequestJournal
from ..repositories import (
    TransactionLedgerOutboxRepository,
    TransactionRequestJournalRepository,
)
from ..schemas import CompleteTransactionRequest, CompleteTransactionResponse


class CompleteTransactionRequestService:
    def __init__(self):
        self.upstream_client = FakeUpstreamVerifierClient()

    def complete(
        self,
        *,
        db: Session,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        idempotency_key: str,
    ) -> CompleteTransactionResponse:

        request_hash = self._build_request_hash(
            transaction_id=transaction_id,
            payload=payload,
        )

        existing_response = self._get_existing_response_or_raise_conflict(
            db=db,
            transaction_id=transaction_id,
            merchant_id=payload.merchant_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

        if existing_response is not None:
            return existing_response

        upstream_result = self.upstream_client.complete_transaction(
            transaction_id=transaction_id,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            account_id=payload.account_id,
            merchant_id=payload.merchant_id,
            external_reference=payload.external_reference,
        )

        if upstream_result.status == UpstreamTransactionStatus.DECLINED:
            response = CompleteTransactionResponse(
                transaction_id=transaction_id,
                status=UpstreamTransactionStatus.DECLINED,
                upstream_reference=None,
                ledger_dispatch_status=LedgerDispatchStatus.NOT_REQUIRED,
                request_id=f"req_{uuid4()}",
            )

            return self._save_declined_or_replay_after_race(
                db=db,
                transaction_id=transaction_id,
                payload=payload,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                upstream_response=upstream_result.raw_response,
                response=response,
            )

        response = CompleteTransactionResponse(
            transaction_id=transaction_id,
            status=UpstreamTransactionStatus.APPROVED,
            upstream_reference=upstream_result.upstream_reference,
            ledger_dispatch_status=LedgerDispatchStatus.PENDING,
            request_id=f"req_{uuid4()}",
        )

        return self._save_approved_or_replay_after_race(
            db=db,
            transaction_id=transaction_id,
            payload=payload,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            upstream_reference=upstream_result.upstream_reference,
            upstream_response=upstream_result.raw_response,
            response=response,
        )

    def _get_existing_response_or_raise_conflict(
        self,
        *,
        db: Session,
        transaction_id: str,
        merchant_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> CompleteTransactionResponse | None:
        journal_repo = TransactionRequestJournalRepository(db)

        existing_by_key = journal_repo.get_by_merchant_idempotency_key(
            merchant_id=merchant_id,
            idempotency_key=idempotency_key,
        )

        if existing_by_key is not None:
            return self._response_from_existing_or_conflict(
                existing=existing_by_key,
                request_hash=request_hash,
            )

        existing_by_transaction = journal_repo.get_by_transaction_id(
            transaction_id=transaction_id,
        )

        if existing_by_transaction is not None:
            return self._response_from_existing_or_conflict(
                existing=existing_by_transaction,
                request_hash=request_hash,
            )

        return None

    def _response_from_existing_or_conflict(
        self,
        *,
        existing: TransactionRequestJournal,
        request_hash: str,
    ) -> CompleteTransactionResponse:
        if existing.request_hash != request_hash:
            raise HTTPException(
                status_code=409,
                detail="same idempotency key or transaction used with different payload",
            )

        if existing.response_body is None:
            raise HTTPException(
                status_code=409,
                detail="transaction completion exists but has no saved response",
            )

        return CompleteTransactionResponse.model_validate(existing.response_body)

    def _save_declined_or_replay_after_race(
        self,
        *,
        db: Session,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        idempotency_key: str,
        request_hash: str,
        upstream_response: dict,
        response: CompleteTransactionResponse,
    ) -> CompleteTransactionResponse:
        journal_repo = TransactionRequestJournalRepository(db)

        journal = TransactionRequestJournal(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            account_id=payload.account_id,
            merchant_id=payload.merchant_id,
            external_reference=payload.external_reference,
            status=TransactionJournalStatus.UPSTREAM_DECLINED.value,
            upstream_status=UpstreamTransactionStatus.DECLINED.value,
            upstream_reference=None,
            upstream_response=upstream_response,
            response_status_code=200,
            response_body=response.model_dump(mode="json"),
            ledger_dispatch_status=LedgerDispatchStatus.NOT_REQUIRED.value,
            request_id=response.request_id,
        )

        journal_repo.add(journal)

        try:
            commit_or_raise_conflict(
                db,
                message="duplicate declined transaction completion",
            )
        except DatabaseConflictError:
            return self._replay_after_unique_conflict(
                db=db,
                transaction_id=transaction_id,
                merchant_id=payload.merchant_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )

        return response

    def _save_approved_or_replay_after_race(
        self,
        *,
        db: Session,
        transaction_id: str,
        payload: CompleteTransactionRequest,
        idempotency_key: str,
        request_hash: str,
        upstream_reference: str | None,
        upstream_response: dict,
        response: CompleteTransactionResponse,
    ) -> CompleteTransactionResponse:
        journal_repo = TransactionRequestJournalRepository(db)
        outbox_repo = TransactionLedgerOutboxRepository(db)

        journal = TransactionRequestJournal(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            account_id=payload.account_id,
            merchant_id=payload.merchant_id,
            external_reference=payload.external_reference,
            status=TransactionJournalStatus.UPSTREAM_APPROVED.value,
            upstream_status=UpstreamTransactionStatus.APPROVED.value,
            upstream_reference=upstream_reference,
            upstream_response=upstream_response,
            response_status_code=200,
            response_body=response.model_dump(mode="json"),
            ledger_dispatch_status=LedgerDispatchStatus.PENDING.value,
            request_id=response.request_id,
        )

        journal_repo.add(journal)
        db.flush()

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
            #   1. final journal
            #   2. ledger outbox message
            commit_or_raise_conflict(
                db,
                message="duplicate approved transaction completion",
            )
        except DatabaseConflictError:
            return self._replay_after_unique_conflict(
                db=db,
                transaction_id=transaction_id,
                merchant_id=payload.merchant_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )

        return response

    def _replay_after_unique_conflict(
        self,
        *,
        db: Session,
        transaction_id: str,
        merchant_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> CompleteTransactionResponse:
        response = self._get_existing_response_or_raise_conflict(
            db=db,
            transaction_id=transaction_id,
            merchant_id=merchant_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

        if response is None:
            raise HTTPException(
                status_code=409,
                detail="duplicate transaction completion request",
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
