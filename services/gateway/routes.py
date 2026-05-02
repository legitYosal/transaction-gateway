from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from .clients import FakeAuthClient
from .database import get_db
from .schemas import CompleteTransactionRequest, CompleteTransactionResponse
from .services.transaction import CompleteTransactionRequestService


router = APIRouter()

service = CompleteTransactionRequestService()
auth_client = FakeAuthClient()

bearer_scheme = HTTPBearer()


@router.post(
    "/transactions/{transaction_id}/complete",
    response_model=CompleteTransactionResponse,
)
def complete_transaction(
    transaction_id: str,
    payload: CompleteTransactionRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> CompleteTransactionResponse:
    authorization_header = f"{credentials.scheme} {credentials.credentials}"
    try:
        auth_client.authorize(authorization_header)
    except PermissionError:
        raise HTTPException(status_code=401, detail="unauthorized")

    return service.complete(
        db=db,
        transaction_id=transaction_id,
        payload=payload,
        idempotency_key=idempotency_key,
    )
