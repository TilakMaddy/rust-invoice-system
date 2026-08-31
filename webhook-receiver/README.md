# webhook-receiver

A reference receiver for the invoice system's webhooks, and the worked example of the
verification recipe `business`'s README describes. It exists so that "signed, so receivers can
verify" is something the stack demonstrates rather than asserts.

```sh
WEBHOOK_SECRETS='[{"path": "/webhooks", "secret": "whsec_dev_healthy"}]' cargo run
cargo test
```

## Configuration

| Variable | Default | Notes |
| --- | --- | --- |
| `WEBHOOK_SECRETS` | `[]` | JSON array of `{"path": "...", "secret": "..."}`. One secret per delivery path, because the business signs each endpoint with its own key. Unset means every delivery fails verification, which is the honest default: a receiver with no key cannot tell a real webhook from anything else that can reach it. |
| `HOST` | `127.0.0.1` | Loopback by default, so a locally run receiver is not exposed to the network. |
| `PORT` | `3002` | Sits next to business on 3001 and mock-payment-service on 3000. |

The secrets are the same literals `compose.yaml` hands the business in `WEBHOOK_ENDPOINTS`. That
is the whole point of configuring endpoints by environment: both sides read one value from one
place, and there is no registration response to capture and forward.

A malformed value is fatal rather than silently ignored — a receiver that quietly came up unable
to verify anything would accept nothing and say nothing about why.

## Verification

Every delivery carries four headers:

```
webhook-id: 01a0471f-3a31-7a33-a066-89badf71de40
webhook-timestamp: 1787923200
webhook-attempt: 2
webhook-signature: v1=9f2c4a...
```

`POST /webhooks` runs these four steps, in this order, and `src/verify.rs` is the whole of it:

1. **Check the timestamp is recent.** Reject anything more than **300 seconds** from now, in
   either direction. This is what makes a captured delivery expire; without it a signature stays
   valid forever, and a recording of one request is a working forgery.
2. **Recompute `HMAC-SHA256(secret, "{webhook-id}.{webhook-timestamp}.{raw body}")`** and hex it.
   The **raw** body, byte for byte as it arrived — parsing and re-serialising first is the usual
   way to get a signature that will not match.
3. **Compare in constant time** against each `v1=` in `webhook-signature`. A plain `==` returns at
   the first differing byte, so how long the answer took leaks how much of the signature was
   right.
4. **Deduplicate on `webhook-id`.** Delivery is at-least-once: a receiver that answered `200` on
   a response the sender never saw gets the same event again, and the id is stable across every
   attempt so that recognising it is enough.

Steps 1 and 4 are both replay protection and neither is sufficient alone — the window bounds how
long a capture is worth anything, the id kills duplicates inside that window. Note that the id is
*inside* the signed string, so it cannot be swapped for a fresh one to slip past step 4.

## Endpoints

### `POST /webhooks`

The ordinary receiver. Verifies, records, and answers `200`.

An unverifiable delivery is recorded too, with `verified: false`, and still answered `200`. That
is a decision worth naming: this is a development harness, and swallowing the evidence would make
a signing bug look like a delivery that never arrived. **A real receiver must answer `401` and
process nothing** — a body it cannot authenticate is a body from anyone.

### `GET /received`

Everything that has arrived, oldest first.

```json
[ { "webhook_id": "01a0471f-3a31-7a33-a066-89badf71de40",
    "type": "invoice.paid", "attempt": 1,
    "verified": true, "duplicate": false, "rejected": null,
    "body": { "id": "…", "type": "invoice.paid", "created_at": "…", "data": { "…": "…" } } } ]
```

`rejected` names why verification failed — `no_secret_configured`, `timestamp_outside_tolerance`,
`signature_mismatch`, `malformed_signature` — or is `null` when it did not.

### `GET /health`

Liveness, for the Compose healthcheck. Touches nothing.

## State

Held in memory only, and lost on restart. There is no database. That is the right amount for a
harness whose job is to show what arrived during one demo — and it does mean the deduplication
table empties on restart, so a redelivery across one would be recorded as new.

Deduplication is keyed on `(path, webhook-id)`, not on the id alone. One process can serve several
configured endpoints, and an event fans out to all of them; they are separate receivers that happen
to share an address, so one endpoint's first delivery is not a duplicate of another's.
