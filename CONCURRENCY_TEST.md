# Concurrency & payment safety tests

Three tests in `business/tests/payment_safety.rs` cover the three ways
`POST /invoices/{id}/pay` can take money it should not: **twice at once**, **twice on retry**, and
**once with nobody able to tell**. Each gets its own freshly migrated database from
`#[sqlx::test]`, so they are order-independent and safe to run concurrently.

```sh
docker compose up -d postgres        # from the repo root
cd business
DATABASE_URL=postgres://postgres:postgres@localhost:5432/business \
    cargo test --test payment_safety
```

```
running 3 tests
test a_retried_unresolved_charge_replays_without_charging_again ... ok
test concurrent_charges_of_one_invoice_charge_the_card_once ... ok
test an_unanswered_charge_is_held_rather_than_stranded ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.54s
```

They are self-contained: the payment service is a stub at the bottom of the file speaking
mock-payment-service's contract under the mock's own card names, so none of this needs that
crate or its container. Postgres is the one real dependency, because these assert on what is
**committed**, not on what a handler returned. `Psp`'s timeout is a constructor argument, so the
timeout path runs in 200 ms rather than the five seconds production waits.

---

## 1. Concurrency — `concurrent_charges_of_one_invoice_charge_the_card_once`

Eight charges of one invoice, arriving together. At most one may be charged.

**The gate is what makes this a test rather than a hope.** Left to the scheduler, the attempts
trickle into `payments::claim` one at a time; each finds the invoice already taken and is refused
for a reason that never exercised the locking at all. So the stub holds every
`POST /payment_intents` on a `tokio::sync::Barrier` until all eight are waiting, then releases
them together:

```
  8 tasks ──> phase 0: pre-flight check      all eight see `ready`
                       │
                       v
              phase 1: POST /payment_intents ─── held on the Barrier ───┐
                       │                                                │
                       │  <──────────── released together ──────────────┘
                       v
              phase 2: payments::claim
                       │
              ┌────────┴────────┐
          1 winner          7 losers
       the UPDATE wins    409 invoice_not_payable
```

Creating the intent is the right place to hold them: it is the last thing a charge does before
it tries to claim, and it happens *after* the pre-flight check, so every released attempt has
already seen the invoice as `ready` and is committed to going for it. Nothing holds a database
connection while it waits, so parking more attempts than the pool has connections is safe, and
the wait is bounded at 5 s so a gate that will never fill fails the test instead of hanging it.

Every attempt carries its own idempotency key, so none of these is a retry and the idempotency
table cannot be what saves it.

### What it asserts

- `stub.calls().0 == 8` — the burst actually raced. Without this the rest can pass against a
  service that does no locking whatsoever.
- **`held.len() <= 1`** — at most one attempt got past the claim.
- **`stub.calls().1 == held.len()`** — the payment service ran a charge for that attempt and no
  other. This is the no-double-charge assertion proper, and the one nothing at the HTTP layer can
  make. All eight *minted* intents — the documented cost of the PSP owning the id — but a minted
  intent is not a charged card.
- Every loser answered `409 invoice_not_payable`, not a `500` or a second `200`.

The assertions are upper bounds rather than equalities on purpose. Which attempt wins, and
whether the winner gets far enough to record its own answer, are both genuinely racy; that no
card is charged twice is not. The test then checks the invoice and its intents agree, in
whichever of the three ways it could legally have landed:

| Winner's answer | invoice state | pointer | intents |
| --- | --- | --- | --- |
| `200` | `processed` | cleared | one `succeeded`, seven `pending` |
| `504` | `processing` | the winner's intent | all eight `pending` |
| nobody won | `ready` | cleared | all eight `pending` |

### It is verified to catch the bug

`payments::claim` is one conditional `UPDATE`, so the mutation that breaks it is deleting the
state test from its `WHERE` — leaving `WHERE id = $1::uuid`, which every attempt matches.
Re-running:

| `claim` | burst | result |
| --- | --- | --- |
| conditional `UPDATE` | gated | **passes** |
| predicate stripped | gated | **fails** — all 8 attempts past the claim, one `200` and seven `504`s |
| predicate stripped | **ungated** | **fails on the gate assertion** — `stub.calls().0` is 4, not 8 |

The middle row is the mutation test proper: with nothing narrowing the `WHERE`, every attempt
updates the row and every attempt believes it won.

The last row is the argument for the gate, and it fails for a reason that has nothing to do with
`claim` — an unmutated `claim` fails it identically. Ungated, only four of the eight attempts
ever reach the payment service; the other four are refused at the pre-flight check without ever
racing, and `held.len() <= 1` would then pass **vacuously**, against a correct service and a
broken one alike. That is why `assert_eq!(stub.calls().0, ATTEMPTS, "the burst did not race")`
runs first: the vacuity is caught by an assertion of its own rather than left to be noticed.

---

## 2. Idempotency — `a_retried_unresolved_charge_replays_without_charging_again`

The retry that actually happens: a client sees a `504`, cannot tell whether the card was charged,
and sends the identical request again.

That is the single moment where a second charge would be invisible to everybody involved — the
first attempt wrote no outcome, so a handler that ran again would find an invoice it is itself
holding and an intent it created. The key is what stops it.

The test charges `tok_network_error` (which settles the charge and *then* drops the connection),
gets a `504`, and retries the same key three times:

- **`again == first`** — the same status *and* the same body, byte for byte. The body carries the
  intent id, and a retry answering with a different id would have created one.
- **`stub.calls() == (1, 1)`** after every retry — the payment service was asked for nothing. No
  second intent, and no second charge.
- The invoice is left exactly as the replayed charge left it: `processing`, still pointing at one
  `pending` intent, for the reconciler to close out.

It is asked three times rather than once because the guarantee is *one key, one response,
forever* — a cache that expired, or a reservation quietly released by a failure, would show up on
a later pass and not on the first.

**The unresolved response is the one worth retrying, which is why this uses a `504` rather than a
`200`.** Caching only successes is a plausible-looking implementation that leaves exactly this
case open, and a replay test built on a `200` cannot see it.

---

## 3. PSP failure — `an_unanswered_charge_is_held_rather_than_stranded`

Neither way of not answering may strand an invoice. A charge that times out and a charge whose
connection drops are the same fact — the card may or may not have been charged — so neither is
guessed at. The test runs both, on **one invoice each** so they cannot mask each other:
`tok_timeout` outlives the client's timeout, and `tok_network_error` settles the charge and then
loses the response, so in that one the money really has moved.

For each, at the moment of failure:

- `504` carrying the intent id.
- The invoice is **held, not released**: `processing`, still pointing at its intent. Releasing it
  is exactly what would let the same money be taken twice.
- The intent is **`pending`, not guessed at** — in particular not `failed`, which is the guess
  that reads as harmless and is not.
- A second charge, **with a card that would work**, gets `409 invoice_not_payable`. This is the
  bad state the invoice is *not* in: chargeable while an earlier charge for it may already have
  gone through.
- `stub.calls() == (2, 2)` — those refusals never reached the payment service, so a caller
  hammering a held invoice cannot run a card again however many times it tries.

Then the test calls `business::jobs::reconcile_once(&state)` directly — the way out, and the only
one. The stub now reports `succeeded`, so both charges did go through, which is what neither
request could find out:

- Both invoices are `processed` with the pointer cleared; both intents `succeeded`.
- Both are **still** `409` to a new charge — now for the opposite reason. Not "we do not know
  yet" but "this is paid". An invoice that came out of the reconciler chargeable again would be
  the double charge this whole path exists to prevent, arriving a day late.

**Held is not stuck, and the difference is only visible over time.** At the moment of failure the
two look identical; what tells them apart is the refusal to everyone in between, and the
reconciler afterwards.

---

## What these three do not cover

They are the safety properties of one endpoint, not the suite. Webhook delivery, the outbox and
its retry budget are covered in `business/tests/webhooks.rs`; the full matrix of pay responses
lives in `business/tests/pay.rs`. Nothing here exercises more than one process — the claim is
safe across replicas because the lock is a row lock, but these tests run one service against one
database and cannot demonstrate that.
