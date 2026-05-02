from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .database import get_db
from .schemas import LedgerPostRequest, LedgerPostResponse
from .service import LedgerPostingService


router = APIRouter()

service = LedgerPostingService()


@router.post(
    "/internal/ledger/posts",
    response_model=LedgerPostResponse,
)
def post_ledger(
    payload: LedgerPostRequest,
    db: Session = Depends(get_db),
) -> LedgerPostResponse:
    return service.post(
        db=db,
        payload=payload,
    )
