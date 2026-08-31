# mock-payment-service

An axum mock of a payment service provider, for developing and testing the invoice system
against realistic payment behavior — including the failure modes that are hard to exercise
against a real PSP — with no network access, credentials, or real money.

```sh
cargo run          # http://127.0.0.1:3000
cargo test
```

## Configuration

| Variable | Default | Notes |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Loopback by default, so a locally run mock is not exposed to the network. |
| `PORT` | `3000` | |

A malformed value is fatal rather than silently ignored — a service that quietly binds the
wrong interface is much harder to diagnose than one that refuses to start.

## Docker

```sh
docker build -t mock-payment-service .
docker run --rm -p 3000:3000 mock-payment-service
```

The image sets `HOST=0.0.0.0` so the service listens on all interfaces *inside the
container*, which is what makes the port published by `-p` reachable. Binding loopback there
would leave the published port refusing connections.

A `HEALTHCHECK` polls `/health` every 10s, so `docker ps` reports the container as healthy
and Compose can gate dependents on it:

```yaml
depends_on:
  mock-payment-service:
    condition: service_healthy
```

The probe is the binary itself — `mock-payment-service --health` opens a loopback connection,
issues `GET /health`, and exits non-zero unless it gets a `200`. That keeps the image free of
an HTTP client, so the runtime is `gcr.io/distroless/cc-debian12:nonroot`: glibc and the
binary, with no shell, no package manager, and no utilities. It runs as uid 65532.

`--health` works outside Docker too, against whatever `PORT` is set to.

## Guarantees

**An intent is charged at most once.** Every payment intent accepts exactly one pay
attempt that reaches the charge stage. Any later attempt is rejected with `409`, whatever
card token it carries. Read `GET /payment_intents/{id}` to learn the outcome.

**An intent is never `pending` for long.** A payment intent is guaranteed to reach a terminal
state (`succeeded` or `failed`) within 24 hours of creation. A client polling for status never
needs to poll longer than that; an intent still `pending` after 24 hours may be treated as
`failed`.

> This mock does **not** enforce the 24 hour bound — there is no expiry sweeper and no
> timestamps are stored. It is a statement of the contract a real implementation would honor.

## State

Held in memory only, and lost on restart. There is no database and nothing is written to
disk. The only thing retained per intent is its state — no timestamps, no attempt log, no
amounts. That much is unavoidable: "never charge this intent twice" and "report the status of
an intent created by an earlier request" are both statements about memory across requests.

## Endpoints

### `POST /payment_intents`

No request body. -> `201`

```json
{ "payment_intent_id": "pi_9f2c4a...", "status": "pending" }
```

### `POST /payment_intents/{id}/pay`

```json
{ "card_token": "tok_success" }
```

| Situation | Response |
| --- | --- |
| Intent un-attempted | `200` with the terminal result |
| Intent unknown | `404 {"error":"payment_intent_not_found"}` |
| `card_token` unrecognized | `400 {"error":"unknown_card_token"}` |
| Charge already in flight | `409 {"error":"payment_in_progress"}` |
| Intent already terminal | `409 {"error":"payment_intent_already_paid"}` |

An unrecognized token is rejected *before* the charge stage, so a typo does not burn the
intent's one attempt — the intent stays `pending` and remains payable.

Terminal results:

```json
{ "payment_intent_id": "pi_9f2c4a...", "status": "succeeded" }
{ "payment_intent_id": "pi_9f2c4a...", "status": "failed", "code": "card_declined" }
```

### `GET /payment_intents/{id}`

-> `200` with `status` of `pending`, `succeeded`, or `failed`; `code` is present only when
`failed`. Unknown id -> `404`.

This endpoint stays responsive while a charge is in flight, including throughout a 30 second
`tok_timeout` charge.

### `GET /health`

-> `200 {"status":"ok"}`

## Test cards

| Token | Takes | Result |
| --- | --- | --- |
| `tok_success` | ~100ms | `{"status":"succeeded"}` |
| `tok_insufficient_funds` | ~100ms | `{"status":"failed","code":"insufficient_funds"}` |
| `tok_card_declined` | ~100ms | `{"status":"failed","code":"card_declined"}` |
| `tok_timeout` | 30s | `{"status":"succeeded"}` |
| `tok_network_error` | none | connection dropped — no usable response |

Settlement is independent of the caller. Every charge runs on its own task, so a client that
times out and disconnects mid-charge still leaves the intent settled — it does not strand
part-way, which would leave it pending forever and impossible to pay. This matters most for
the 30 second `tok_timeout`, but holds for every card.

`tok_network_error` records the charge as **succeeded** and then drops the connection. This is
the realistic ambiguous failure: the money moved and the caller never found out. Recovering
means reading `GET /payment_intents/{id}` rather than assuming the payment failed; retrying
the payment returns `409 payment_intent_already_paid`.

The drop is implemented as a response body stream that errors on its first poll. Hyper
abandons the response and closes the connection before flushing anything, so the client never
receives a status line — `curl` reports `Empty reply from server` (exit 52) and `reqwest`
fails with a transport error.

## Example

```sh
ID=$(curl -sX POST localhost:3000/payment_intents | jq -r .payment_intent_id)

curl -sX POST localhost:3000/payment_intents/$ID/pay \
  -H 'content-type: application/json' -d '{"card_token":"tok_success"}'
# {"payment_intent_id":"pi_...","status":"succeeded"}

curl -sX POST localhost:3000/payment_intents/$ID/pay \
  -H 'content-type: application/json' -d '{"card_token":"tok_card_declined"}'
# {"error":"payment_intent_already_paid"}   -- and the status is still succeeded

curl -s localhost:3000/payment_intents/$ID
# {"payment_intent_id":"pi_...","status":"succeeded"}
```
