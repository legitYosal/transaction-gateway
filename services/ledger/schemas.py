from pydantic import BaseModel, ConfigDict, Field

from .enums import LedgerPostStatus


class LedgerPostRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_system: str = Field(min_length=1, max_length=64)
    source_event_id: str = Field(min_length=1, max_length=36)
    source_transaction_id: str = Field(min_length=1, max_length=36)

    amount_minor: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)

    account_id: str = Field(min_length=1, max_length=128)
    merchant_id: str = Field(min_length=1, max_length=128)

    upstream_reference: str | None = Field(default=None, max_length=128)


class LedgerPostResponse(BaseModel):
    ledger_post_id: str
    status: LedgerPostStatus
    duplicate: bool
