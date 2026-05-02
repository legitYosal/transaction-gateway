from pydantic import BaseModel, ConfigDict, Field

from .enums import LedgerDispatchStatus, UpstreamTransactionStatus


class CompleteTransactionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_minor: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    account_id: str = Field(min_length=1, max_length=128)
    merchant_id: str = Field(min_length=1, max_length=128)
    external_reference: str | None = Field(default=None, max_length=128)


class CompleteTransactionResponse(BaseModel):
    transaction_id: str
    status: UpstreamTransactionStatus
    upstream_reference: str | None
    ledger_dispatch_status: LedgerDispatchStatus
    request_id: str
