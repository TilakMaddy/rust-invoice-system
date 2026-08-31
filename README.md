# rust-invoice-system

An invoicing service that raises invoices against customers, charges them through a payment
service, and tells interested parties what happened over signed webhooks. Three Rust services and
a Postgres, wired together by Docker Compose.

| Service | Port | What it is |
| --- | --- | --- |
| **`business`** | 3001 | The system proper. Owns every row, serves the API, charges invoices, signs and delivers webhooks. |
| **`mock-payment-service`** | 3000 | A stand-in PSP with scripted test cards, including a 30 s timeout and a mid-charge connection drop. |
| **`webhook-receiver`** | 3002 | A reference receiver that verifies signatures in full, so the webhook contract is executable rather than described. |
| `postgres` | 5432 | Postgres 18. `business` migrates it at startup. |

## Run it

```sh
docker compose up -d --build      # or: just up
curl -s localhost:3001/health     # {"status":"ok"}
open http://localhost:3001/docs   # browsable API reference
```

The API reference is served by the service itself: **[`/docs`](http://localhost:3001/docs)** is
Swagger UI and **`/openapi.yaml`** is the spec behind it. Both are embedded in the binary — the
runtime image is distroless and holds no files — so they work with no network access at all.

The operator endpoints are gated on `X-API-Token`, which the dev stack sets to `dev-token`:

```sh
export TOKEN=dev-token
```

## Walkthrough

Four calls: create a customer, raise an invoice against them, finalize it, then charge it. Every
response below is real output from the running stack.

The blocks paste in order into one shell, each capturing the id the next one needs, so no id has
to be copied by hand. They need `jq`, and the `TOKEN` exported above.

### 1. Create a customer

Emails are `UNIQUE`, so a literal address answers `409 email_already_exists` the second time
through — the `+$(date +%s)-$RANDOM` keeps it fresh:

```sh
CUSTOMER_ID=$(curl -sX POST localhost:3001/customers \
  -H "X-API-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"name\": \"Acme Corp\", \"email\": \"billing+$(date +%s)-$RANDOM@acme.test\"}" | jq -r .id)
```

The body that `jq` read the id out of:

```json
{ "id": "01a05295-b10f-79a7-9d1b-bd33ace910e2",
  "name": "Acme Corp",
  "email": "billing+1788091937-23347@acme.test" }
```

### 2. Raise an invoice

You send line items; the server totals them. There is no way to supply a total directly, so the
number stored can never disagree with the lines it came from. An invoice is always born `draft`.

```sh
INVOICE_ID=$(curl -sX POST localhost:3001/invoices \
  -H "X-API-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"customer_id\": \"$CUSTOMER_ID\",
       \"due_date\": \"2026-10-01\",
       \"line_items\": [
         {\"description\": \"Consulting\", \"quantity\": 2, \"unit_amount_cents\": 50000},
         {\"description\": \"Support\",    \"quantity\": 1, \"unit_amount_cents\":  2500}
       ]}" | jq -r .id)
```

```json
{ "id": "01a05295-b120-74c8-b972-b83b983f932c",
  "customer_id": "01a05295-b10f-79a7-9d1b-bd33ace910e2",
  "total_cents": 102500,
  "due_date": "2026-10-01",
  "state": "draft" }
```

Finalize it before charging, since only a `ready` invoice is payable:

```sh
curl -sX POST localhost:3001/invoices/$INVOICE_ID/ready -H "X-API-Token: $TOKEN"
```

```json
{ "id": "01a05295-b120-74c8-b972-b83b983f932c", "…": "…", "state": "ready" }
```

### 3. Charge it — success

`POST /pay` is the **payer's** endpoint, so it takes no `X-API-Token`. It does require an
`Idempotency-Key`: a charge with no key is a charge nobody can safely retry.

A key stands for **one intended charge, forever**, so a literal one answers `400
idempotency_key_reused` on a second run through this walkthrough. Hold it in a variable instead —
it has to differ between runs, and the retry below has to send the *same* one:

```sh
KEY=acme-oct-$(date +%s)

curl -sX POST localhost:3001/invoices/$INVOICE_ID/pay \
  -H 'content-type: application/json' -H "Idempotency-Key: $KEY" \
  -d '{"card_token": "tok_success"}'
```

```json
{ "payment_intent_id": "pi_bdd37a0bf8554b27924180437a6c6c46" }   # HTTP 200
```

The invoice is now `processed`. Send the **same key** again and you get that response back byte
for byte, with nothing charged — one key, one response, forever:

```sh
# identical 200 and identical body, and the payment service is never called
curl -sX POST localhost:3001/invoices/$INVOICE_ID/pay \
  -H 'content-type: application/json' -H "Idempotency-Key: $KEY" \
  -d '{"card_token": "tok_success"}'
```

### 4. Charge it — a declined card

That invoice is `processed` now, and a processed invoice is not payable — so this needs a fresh
one. The customer is not consumed by a charge, though: Acme is still in `$CUSTOMER_ID`, so only
the invoice has to be raised again.

```sh
INVOICE_ID=$(curl -sX POST localhost:3001/invoices \
  -H "X-API-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"customer_id\": \"$CUSTOMER_ID\",
       \"due_date\": \"2026-11-15\",
       \"line_items\": [
         {\"description\": \"Consulting\", \"quantity\": 1, \"unit_amount_cents\": 50000}
       ]}" | jq -r .id)

curl -sX POST localhost:3001/invoices/$INVOICE_ID/ready -H "X-API-Token: $TOKEN" > /dev/null
```

A **declined card** answers `402`, and returns the invoice to `ready` so it can be charged again
with a different card. The body says the charge was declined and hands back the intent id, but
not *why* — the PSP separates `insufficient_funds` from `card_declined` and this API answers
`charge_declined` to both, so no caller ends up depending on the PSP's vocabulary:

```sh
curl -sX POST localhost:3001/invoices/$INVOICE_ID/pay \
  -H 'content-type: application/json' -H "Idempotency-Key: acme-nov-$(date +%s)" \
  -d '{"card_token": "tok_card_declined"}'
```

```json
{ "error": "charge_declined",
  "payment_intent_id": "pi_d8b9a61131704bf39e75a060cd4d67bf" }   # HTTP 402
```

Everything else answers `{"error": "<code>"}`:

| Situation | Status | Body |
| --- | --- | --- |
| The invoice is not `ready` — still a draft, already paid, voided, or being charged right now | `409` | `{"error":"invoice_not_payable"}` |
| The same key sent with a different body | `400` | `{"error":"idempotency_key_reused"}` |
| No `Idempotency-Key` at all | `400` | `{"error":"idempotency_key_missing"}` |
| An operator endpoint without a token | `401` | `{"error":"unauthorized"}` |

`just verify-payments` drives all five modes end to end.

## Test cards

| Token | Takes | Result |
| --- | --- | --- |
| `tok_success` | ~100 ms | `succeeded` |
| `tok_insufficient_funds` | ~100 ms | `failed` |
| `tok_card_declined` | ~100 ms | `failed` |
| `tok_timeout` | 30 s | `succeeded` — outlives the 5 s client timeout |
| `tok_network_error` | none | charge lands, connection drops, caller never finds out |

## Tests

Every test runs against a real Postgres. `#[sqlx::test]` gives each its own freshly migrated
database, so they are order-independent and run concurrently.

```sh
just test    # starts postgres, then cargo test in all three crates
just lint    # fmt --check plus clippy -D warnings, across all three crates
```

```sh
docker compose up -d postgres
cd business
DATABASE_URL=postgres://postgres:postgres@localhost:5432/business cargo test
```
## Demo Video

https://bit.ly/4qI6O6h

## Documentation

| Where | What |
| --- | --- |
| [`business/openapi.yaml`](business/openapi.yaml) | The API: every endpoint, request and response shape, and the error format. Served live at `/openapi.yaml`, rendered at [`/docs`](http://localhost:3001/docs). |
| [`DESIGN.md`](DESIGN.md) | Data model, invoice state machine, payment correctness and failure modes, webhooks, what was cut, and the production gaps. |
| [`CONCURRENCY_TEST.md`](CONCURRENCY_TEST.md) | The payment safety tests, and the mutation runs proving they catch the bug. |
| [`business/README.md`](business/README.md) | The service in detail: config, migrations, endpoints, webhooks, background jobs. |
| [`mock-payment-service/README.md`](mock-payment-service/README.md) | The PSP's contract and its test cards. |
| [`webhook-receiver/README.md`](webhook-receiver/README.md) | The signature verification recipe, implemented. |

## Configuration

`business` refuses to start without the first three — every fallback would be a guess at which
database to write, which payment service to charge, or a token everybody knows.

| Variable | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | — | Postgres connection string. |
| `PAYMENT_SERVICE_URL` | — | The PSP's base URL. |
| `BUSINESS_API_KEY` | — | The token `X-API-Token` is checked against. Empty is refused too. |
| `WEBHOOK_ENDPOINTS` | `[]` | JSON array of `{url, secret}`. No endpoints means no deliveries; events are still recorded. |
| `HOST` | `127.0.0.1` | The image overrides this to `0.0.0.0`. |
| `PORT` | `3001` | |
| `RUST_LOG` | `warn,business=debug` | Standard `tracing` filter. |

Webhook endpoints are **configuration, not an API** — there is nothing to create one with over
HTTP, so adding a receiver is a change to `compose.yaml` and a restart.
