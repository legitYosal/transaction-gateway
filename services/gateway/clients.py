from dataclasses import dataclass
from uuid import uuid4

import httpx

from .enums import UpstreamTransactionStatus


@dataclass(frozen=True)
class AuthorizedPrincipal:
    actor_id: str
    actor_type: str


class FakeAuthClient:
    def authorize(self, authorization_header: str | None) -> AuthorizedPrincipal:
        if not authorization_header.startswith('Bearer'):
            raise PermissionError("invalid token")

        return AuthorizedPrincipal(
            actor_id="merchant-api-client",
            actor_type="merchant",
        )


@dataclass(frozen=True)
class UpstreamResult:
    status: UpstreamTransactionStatus
    upstream_reference: str | None
    raw_response: dict


class FakeUpstreamVerifierClient:
    def complete_transaction(
        self,
        *,
        transaction_id: str,
        amount_minor: int,
        currency: str,
        account_id: str,
        merchant_id: str,
        external_reference: str | None,
    ) -> UpstreamResult:
        if external_reference == "decline":
            return UpstreamResult(
                status=UpstreamTransactionStatus.DECLINED,
                upstream_reference=None,
                raw_response={
                    "transaction_id": transaction_id,
                    "status": UpstreamTransactionStatus.DECLINED.value,
                    "reason": "upstream declined this transaction",
                },
            )

        upstream_reference = f"up_{uuid4()}"

        return UpstreamResult(
            status=UpstreamTransactionStatus.APPROVED,
            upstream_reference=upstream_reference,
            raw_response={
                "transaction_id": transaction_id,
                "status": UpstreamTransactionStatus.APPROVED.value,
                "upstream_reference": upstream_reference,
            },
        )


class LedgerClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def post_ledger(self, payload: dict) -> dict:
        response = httpx.post(
            f"{self.base_url}/internal/ledger/posts",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
