# Verified Transaction Completion Gateway

A small FastAPI sample project that simulates a fintech-style transaction completion flow.

It contains two microservices:

- `transaction-gateway`
- `ledger-service`

The gateway receives a transaction completion request, calls a fake upstream verifier, saves a local journal, and dispatches approved completions to the ledger service through an outbox worker.

---

## What it simulates

```text
Client
  -> Transaction Gateway
  -> Fake Upstream Verifier
  -> Gateway Journal
  -> Gateway Outbox
  -> Gateway Worker
  -> Ledger Service
```

The gateway does not own real transactions, balances, accounts, or ledger data.

The gateway owns only technical processing state:

```text
transaction_request_journals
transaction_ledger_outbox_messages
```

The ledger service owns posted ledger records:

```text
ledger_posts
```

---

## Outbox pattern

The outbox is used to avoid losing ledger dispatch work.

When upstream approves a transaction, the gateway saves:

```text
journal row + outbox message
```

in the same database transaction.

The API returns quickly with:

```text
ledger_dispatch_status = pending
```

Then the worker reads pending outbox messages and posts them to the ledger service.

If ledger is unavailable, the worker retries with backoff. After too many failures, the message moves to DLQ.

---

## API docs

Gateway Swagger:

```text
http://localhost:8000/docs
```

Ledger Swagger:

```text
http://localhost:8001/docs
```

---

## Bring up with Docker Compose

Copy compose env:

```bash
cp .env.compose.example .env.compose
```

Build:

```bash
docker compose --env-file .env.compose build
```

Start databases:

```bash
docker compose --env-file .env.compose up -d postgres-gateway postgres-ledger
```

Run gateway migrations:

```bash
docker compose --env-file .env.compose run --rm gateway-api \
  alembic -c services/gateway/alembic.ini upgrade head
```

Run ledger migrations:

```bash
docker compose --env-file .env.compose run --rm ledger-service \
  alembic -c services/ledger/alembic.ini upgrade head
```

Start all services:

```bash
docker compose --env-file .env.compose up
```

Or detached:

```bash
docker compose --env-file .env.compose up -d
```

Logs:

```bash
docker compose --env-file .env.compose logs -f gateway-api
docker compose --env-file .env.compose logs -f gateway-worker
docker compose --env-file .env.compose logs -f ledger-service
```

Stop:

```bash
docker compose --env-file .env.compose down
```

Remove database volumes too:

```bash
docker compose --env-file .env.compose down -v
```

---

## Test the full flow

Call the gateway:

```bash
curl -X POST \
  'http://localhost:8000/transactions/tx-100/complete' \
  -H 'Authorization: Bearer valid-token' \
  -H 'Idempotency-Key: key-100' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount_minor": 1000,
    "currency": "USD",
    "account_id": "acc_123",
    "merchant_id": "mer_456",
    "external_reference": "order_100"
  }'
```

Expected response:

```json
{
  "transaction_id": "tx-100",
  "status": "approved",
  "upstream_reference": "up_...",
  "ledger_dispatch_status": "pending",
  "request_id": "req_..."
}
```

The gateway worker should then publish the outbox message to the ledger service.

Expected worker logs:

```text
claimed outbox message message_id=...
publishing outbox message message_id=...
published outbox message message_id=...
```

---

## Test idempotency

Call the same request again:

```bash
curl -X POST \
  'http://localhost:8000/transactions/tx-100/complete' \
  -H 'Authorization: Bearer valid-token' \
  -H 'Idempotency-Key: key-100' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount_minor": 1000,
    "currency": "USD",
    "account_id": "acc_123",
    "merchant_id": "mer_456",
    "external_reference": "order_100"
  }'
```

Expected behavior:

```text
The gateway returns the saved response.
The ledger service does not create a duplicate post.
```

---

## Notes

The fake auth client accepts only:

```text
Bearer valid-token
```

The fake upstream verifier approves everything except:

```text
external_reference = decline
```

The ledger service is idempotent, so gateway worker retries are safe.
