# Design

Three services: **`business`** owns every row and serves the API, **`mock-payment-service`** is the PSP, and
**`webhook-receiver`** implements the verification recipe so the contract is executable rather than described.

## 1. Data model

| Table | Columns | Primary key | Other indexes |
| --- | --- | --- | --- |
| `customers` | name, email | uuidv7 | UNIQUE `email` |
| `invoices` | customer_id, total_cents, due_date, state, in-flight intent pointer | uuidv7 | `customer_id` |
| `payment_intents` | invoice_id, status | `text` — the PSP's `pi_…` | `invoice_id` |
| `idempotency_keys` | request_hash, status_code, response_body | the caller's key | — |
| `webhook_endpoints` | url, secret, disabled_at | uuidv7 | UNIQUE `url` |
| `webhook_events` | type, payload `jsonb` | uuidv7 | — |
| `webhook_deliveries` | event_id, endpoint_id, status, attempts, next_attempt_at, last failure | uuidv7 | UNIQUE `(event_id, endpoint_id)`; partial `(next_attempt_at) WHERE status='pending'`; both FKs |

### Why this shape

- **`invoices.state` is stored, not derived** from `payment_intents`. `draft`/`ready`/`void` are the business deciding a
  charge is allowed; they say nothing about the PSP. It is also the row a charge locks, so it has to be a column.
- **`currently_processed_by_pi_id`** — the **payment intent** (the PSP's handle for one charge attempt) currently being
  charged. It narrows "being charged" to "being charged by *this* attempt", so recovery can close out an abandoned
  attempt without touching a live one. At most one charge in flight per invoice becomes a property of the data, not a
  convention in the code.
- **`payment_intents` is a table**, not a status column on the invoice. Attempts are the domain: collapsing them to one
  status erases the declines that reconciliation and disputes are made of.
- **Events and deliveries are separate tables.** An event exists even when no endpoint is configured, and each delivery
  carries its own retry schedule — the backlog lives in rows rather than in a timer.

### At 100×

Three workloads that only look alike at this size: a **ledger** (`customers`, `invoices`, `payment_intents`), a
**queue** (`webhook_deliveries`), and an **append-only log** (`webhook_events`, `idempotency_keys`).

- **Webhook delivery becomes its own service — the first thing to move.** It is the only table with a multiplier on it
  (events × endpoints), and the outbox is already the boundary.
- **The charge path must not split.** `claim` decides on `invoices`; `settle` writes `invoices` and `payment_intents` in
  one transaction — what makes cases (a) and (c) correct. Separate stores make it a saga on money.
- **The ledger shards by `customer_id`.** Nothing in the domain joins two customers, while the time-ordered uuidv7
  invoice id would hot-spot one shard and scatter-gather a customer's list.
- **`idempotency_keys` leaves Postgres for a KV store with a TTL.** It references nothing and is meaningless after its
  window, yet sits on the hot path of every charge and grows forever.
- **Reads move to replicas, except `GET /payment_intents/{id}`** — a caller polls it after a `504`, and lag reporting
  `pending` for a settled charge is what it exists to prevent.
- **Still missing, at any scale:** no timestamps and no `currency` in the core schema, line items summed and discarded,
  and `webhook_deliveries` wants time partitioning for retention.

## 2. Invoice state machine

```
  POST /invoices
        │
        v
   ┌─────────┐   POST /ready   ┌─────────┐     claim      ┌──────────────┐
   │  draft  │ ──────────────> │  ready  │ ─────────────> │  processing  │
   └─────────┘ <────────────── └─────────┘ <───────────── └──────────────┘
        │        POST /draft        │      settle(failed)        │
        │ POST /void                │ POST /void                 │ settle(succeeded)
        └───────────┬───────────────┘                            v
                    v                                     ┌─────────────┐
               ┌─────────┐                                │ processed * │
               │ void *  │                                └─────────────┘
               └─────────┘
```

`POST /ready`, `/draft` and `/void` are operator endpoints. **`claim`** (take the invoice for one attempt) and
**`settle`** (record how that attempt ended) are internal steps of `POST /pay`; the daily reconciler settles through the
same function. `*` marks a terminal state.

| From | To | Trigger | Reversible? |
| --- | --- | --- | --- |
| — | `draft` | `POST /invoices` | — |
| `draft` | `ready` | `POST /invoices/{id}/ready` | yes, via `POST /draft` |
| `ready` | `draft` | `POST /invoices/{id}/draft` | yes, via `POST /ready` |
| `draft`, `ready` | `void` | `POST /invoices/{id}/void` | **no** — terminal |
| `ready` | `processing` | `claim`, inside `POST /pay` | released by `settle` |
| `processing` | `ready` | `settle(failed)` — card declined | — |
| `processing` | `processed` | `settle(succeeded)` | **no** — terminal |

- **Terminal:** `void` and `processed`. Undoing `processed` is a refund, which is cut — see *What I cut and why*.
- **Reversible:** only `ready -> draft`. `processing -> ready` on a decline is a *release*, not a reversal — the invoice
  simply becomes chargeable again.
- **Invalid transitions** are rejected under the same row lock the valid ones take: `handlers::transition` reads `SELECT
  state … FOR UPDATE`, tests membership in an explicit `from` list, and answers a miss with `409` naming the rule.
- **`processing` is in no operator endpoint's `from` list**, so the back office cannot touch an invoice mid-charge.

## 3. Payment correctness & failure modes

`POST /invoices/{id}/pay` runs four phases, in an order that does not survive rearranging:

```
  POST /invoices/{id}/pay        Idempotency-Key: required
        │
        ├─ 0. pre-check    state must be 'ready'       no writes, no PSP call
        ├─ 1. mint intent  PSP issues pi_…             row recorded as 'pending'
        ├─ 2. claim        ready -> processing         conditional UPDATE, one winner
        └─ 3. charge       PSP answers succeeded / failed / nothing at all
              └─ settle    processing -> processed | ready, webhook enqueued
```

**Concurrency mechanism: row-level locking**, under READ COMMITTED. `claim` acquires the lock implicitly, via a
status-conditional `UPDATE`; `settle` and the operator transitions acquire it explicitly, with `SELECT … FOR UPDATE`.

- **Phase 2 is what prevents a double charge.** Phase 0 only avoids minting an intent nobody will use — it can be stale
  by the time phase 2 runs, and nothing depends on it being right.
- **`Idempotency-Key` is required**, because a client whose request times out cannot otherwise say "retry this charge"
  rather than "make a second one".
- **No transaction spans a PSP call**, so no lock is ever held across the network.

| Case | Answer | Invoice | At the PSP | Resolved by |
| --- | --- | --- | --- | --- |
| (a) two concurrent `/pay` | one `200`, one `409` | `processed` | one intent charged; the other created but never charged | — |
| (b) PSP times out | `504 charge_unresolved` + intent id | `processing` | **charged** — `tok_timeout` succeeds at 30 s | daily reconciler |
| (c) crash after PSP success | `409` on retry | `processing` | **charged** | daily reconciler |
| (d) key reused, new body | `400 idempotency_key_reused` | unchanged | never called | caller |
| (e) already paid | `409 invoice_not_payable` | `processed` | never called | — |

### (a) Two clients pay the same invoice at the same instant

- Both mint an intent, then both run `claim`'s conditional `UPDATE` with a `LOCK TIMEOUT` so no-one gets blocked waiting.
- The second request waits on the row lock the first `UPDATE` took. When the first commits, Postgres re-runs the `WHERE`
  against the version it committed (EvalPlanQual): `state` is now `processing`, so the row no longer matches, the second
  updates **zero rows**, and `rows_affected() == 0` is the whole verdict -> `409 invoice_not_payable`.
- **Exactly one charge.** The check *is* the write, so there is no window between them to lose.
- If that wait exceeds the 100 ms `lock_timeout`, the request is refused the same way. To the caller it is the same fact
  either way: no charge was made.

### (b) The PSP times out (`tok_timeout`, 30 s)

- The client timeout is 5 s, so the call returns `Charge::Unknown` — a first-class third outcome, not a failure. The
  card is charged 25 s later; this service never hears it.
- The endpoint answers **`504` with the intent id and writes nothing**: invoice still `processing`, intent still
  `pending`. Assuming a decline would release the invoice and let the same money be taken twice.
- `GET /payment_intents/{id}` reports the recorded row with no PSP call, so it reads `pending` until the **daily
  reconciler** (first tick at startup) asks the PSP and settles — through the same `settle`, which is why the webhook
  still fires.

### (c) The PSP succeeds and the service crashes before persisting

- Nothing was written. Retrying the **same key** finds a reservation holding no answer -> `409
  idempotency_key_in_flight`. A **new key** hits phase 0, which sees `processing` rather than `ready` -> `409
  invoice_not_payable`.
- **The customer is not charged twice:** the PSP accepts one attempt per intent, and no second intent is minted for an
  invoice this service cannot re-claim.
- Cost: a crash there **spends that key permanently** — fail-closed. The reconciler recovers the answer out of band.

### (d) An idempotency key is reused with a different body

- `400 idempotency_key_reused`; nothing charged.
- The fingerprint is `sha256("METHOD path\n<body>")` — **the path is in the hash**, so one key sent against two
  different invoices is caught as the different request it is.

### (e) A `processed` invoice receives another `POST /pay`

- Phase 0 reads `processed`, not `ready` -> `409 invoice_not_payable`; nothing charged.
- However, if the *original* idemmpotency key is used, the first response is replayed.

**Why this over the alternatives:**

| Alternative | Why not |
| --- | --- |
| `SERIALIZABLE` | When two charges collide, Postgres cancels one with a serialization error instead of letting it simply find nothing to update. Every caller then needs retry logic, for a collision the row lock already prevents. |
| Advisory locks | Not attached to the row, so a code path that forgets one fails silently. |
| In-process mutex | Introduces state in the `business` app and prevents horizontal scaling. |

## 4. Webhook design

- **Signing:** `HMAC-SHA256`, hex, over `"{webhook-id}.{timestamp}.{raw body}"`, keyed on the endpoint's own secret — so
  one compromised receiver cannot forge to another. Sent as `webhook-signature: v1=…`.
- **Replay protection is the receiver's to enforce** — this service only puts the material on the wire;
  `webhook-receiver` runs the recipe. The **300 s tolerance window** on the signed timestamp expires a *captured*
  request; **dedupe on `webhook-id`** suppresses a *legitimate* retry, and so must outlive that window — every attempt
  is signed afresh, so one arrives valid and identical up to 20 h later. The id is *inside the signed string*, so a
  capture cannot be relabelled past it.
- **Retries: 8 attempts over ~20 h 31 m**, waiting `10s -> 1m -> 5m -> 25m -> 2h -> 6h -> 12h` after each failure, ±10%
  jitter. Roughly ×5 up to a ceiling, so a receiver down for a deploy is retried in seconds and one down for the night
  is still caught before morning. A claim only *leases* the row, so a dispatcher that dies mid-attempt costs a minute
  rather than one of the eight.
- **When the budget runs out** the delivery turns `exhausted` — terminal, and the dispatcher only selects `pending`, so
  nothing revives it. The row keeps `attempts` and the classified last failure (`last_status` or `last_error`, never
  both), so `GET /webhook_deliveries?status=exhausted` separates a handler returning `500`s from connections that never
  landed. **Exhaustion is announced, not merely recorded:** an event a business was owed and never got is news, so
  giving up logs the event, endpoint and reason — a line ending in `would send email`, there being no mailer behind it.
- **Reconciling misses** — exhausted or never attempted — is a pull: `GET /events?after=<id>` returns the byte-identical
  envelope that was or would have been delivered, covering even events raised before any endpoint existed. This is why
  exhaustion can be terminal: the event is never lost, only one endpoint's delivery of it.

**Why delivery is decoupled from the API response — a transactional outbox:**

- The event and one delivery row per enabled endpoint are written **in the same transaction as the state change**, so an
  event exists exactly when that change committed.
- A background task polls every second, claims with `FOR UPDATE SKIP LOCKED`, and leases for 60 s.
- In-path delivery would fail `POST /invoices` during a *receiver's* outage — and worse, `settle` holds row locks on
  `invoices` and `payment_intents`, so an HTTP call inside it would hold them **across the network** and blow the 100 ms
  `lock_timeout` on live charges.
- Consequence: delivery is at-least-once and unordered.

## 5. API key model

One shared static token, `BUSINESS_API_KEY`, supplied out of band and taken verbatim.

| | Today | A real model |
| --- | --- | --- |
| Generation | none — an operator picks it | 32 random bytes |
| Storage | **plaintext** in the environment; no prefix, no per-key row | `sk_live_` prefix, hashed at rest, one row per key |
| Transmission | `X-API-Token`, compared in **constant time** | unchanged |
| Rotation | edit the config and restart; no overlap window | two keys valid at once |
| Revocation | edit the config and restart; no key row to disable | one `UPDATE` setting `revoked_at` |

**Blast radius if leaked:** the whole back office — every customer, every invoice, the event log, and the ability to
void invoices. It does not reach the payer endpoints, and **it cannot move money**: `/pay` charges a card token, not an
API key.

## 6. What I cut and why

- **Multi-tenancy.** The whole system is assumed to be run by one business. Real isolation is a schema per tenant or a
  dedicated deployment — plumbing, not logic, so little here would change.
- **Stored line items.** Summed on the way in and discarded; the invoice keeps a total. A child table is the first thing
  the data model would add back.
- **Abandoned state for payment intent** - Some intents can be pending for the rest of their lives which is not ideal, I
  would like to introduce an `abandoned` state that can garbage collect records from db.
- **due_date** No special care has been taking care for handling what happens when an invoice is unpaid and the due_date
  has passed. I felt like it could go in many ways, wasn't sure what to do.

## 7. Production readiness gap
- **External PgPool** - Right now the pool is internal to the business app, so if I want to ever scale horizontally, it
  will problems in the future because we may run out of max connections to the database.
- **KYC** - I would invest in Know Your Customer before taking on legal responsibility to settle payments for clients.
  That would definitely warrant rebuilding the auth system.
- **Rate limiting.** `POST /invoices/{id}/pay` is unauthenticated by design, and nothing bounds attempts per invoice or
  per caller. That makes it a card-testing oracle and a way to mint unbounded intents at the PSP.
