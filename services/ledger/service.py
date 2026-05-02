from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .enums import LedgerPostStatus
from .models import LedgerPost
from .repositories import LedgerPostRepository
from .schemas import LedgerPostRequest, LedgerPostResponse


class LedgerPostingService:
    def post(
        self,
        *,
        db: Session,
        payload: LedgerPostRequest,
    ) -> LedgerPostResponse:
        post_repo = LedgerPostRepository(db)

        existing = post_repo.get_by_source_event(
            source_system=payload.source_system,
            source_event_id=payload.source_event_id,
        )

        if existing is not None:
            return self._duplicate_response(existing)

        existing_by_transaction = post_repo.get_by_source_transaction(
            source_system=payload.source_system,
            source_transaction_id=payload.source_transaction_id,
        )

        if existing_by_transaction is not None:
            return self._duplicate_response(existing_by_transaction)

        post = LedgerPost(
            source_system=payload.source_system,
            source_event_id=payload.source_event_id,
            source_transaction_id=payload.source_transaction_id,
            status=LedgerPostStatus.POSTED.value,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            debit_account_ref=f"customer_account:{payload.account_id}",
            credit_account_ref=f"merchant_payable:{payload.merchant_id}",
            upstream_reference=payload.upstream_reference,
        )

        post_repo.add(post)

        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            return self._response_after_race(
                db=db,
                payload=payload,
            )

        return LedgerPostResponse(
            ledger_post_id=post.id,
            status=LedgerPostStatus.POSTED,
            duplicate=False,
        )

    def _response_after_race(
        self,
        *,
        db: Session,
        payload: LedgerPostRequest,
    ) -> LedgerPostResponse:
        post_repo = LedgerPostRepository(db)

        existing = post_repo.get_by_source_event(
            source_system=payload.source_system,
            source_event_id=payload.source_event_id,
        )

        if existing is not None:
            return self._duplicate_response(existing)

        existing_by_transaction = post_repo.get_by_source_transaction(
            source_system=payload.source_system,
            source_transaction_id=payload.source_transaction_id,
        )

        if existing_by_transaction is not None:
            return self._duplicate_response(existing_by_transaction)

        raise HTTPException(
            status_code=409,
            detail="ledger post conflict",
        )

    def _duplicate_response(
        self,
        post: LedgerPost,
    ) -> LedgerPostResponse:
        return LedgerPostResponse(
            ledger_post_id=post.id,
            status=LedgerPostStatus.POSTED,
            duplicate=True,
        )
