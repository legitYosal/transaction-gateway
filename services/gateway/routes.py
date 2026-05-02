from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session

from .database import get_db
from .schemas import CompleteTransactionRequest, CompleteTransactionResponse
from .service import CompleteTransactionRequestService, IdempotentReplay


router = APIRouter()

service = CompleteTransactionRequestService()


@router.post(
    "/transactions/{transaction_id}/complete",
    response_model=CompleteTransactionResponse,
)
def complete_transaction(
    transaction_id: str,
    payload: CompleteTransactionRequest,
    db: Session = Depends(get_db),
    idempotency_key: str = Header(alias="Idempotency-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> CompleteTransactionResponse:
    try:
        return service.complete(
            db=db,
            transaction_id=transaction_id,
            payload=payload,
            idempotency_key=idempotency_key,
            authorization_header=authorization,
        )
    except IdempotentReplay as replay:
        return replay.response
