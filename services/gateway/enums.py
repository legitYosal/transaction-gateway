from enum import Enum


class TransactionJournalStatus(str, Enum):
    UPSTREAM_APPROVED = "upstream_approved"
    UPSTREAM_DECLINED = "upstream_declined"


class UpstreamTransactionStatus(str, Enum):
    APPROVED = "approved"
    DECLINED = "declined"


class LedgerDispatchStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    PROCESSING = "processing"
    POSTED = "posted"
    FAILED = "failed"
    DLQ = "dlq"


class OutboxStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    PUBLISHED = "published"
    FAILED = "failed"
    DLQ = "dlq"


class OutboxEventType(str, Enum):
    LEDGER_POST_REQUESTED = "ledger.post_requested"
