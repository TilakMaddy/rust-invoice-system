//! The three safety properties of `POST /invoices/{id}/pay`, end to end.
//!
//! 1. **Concurrency** — N charges of one invoice arriving together charge the card at most
//!    once, and leave the invoice and its intents agreeing about what happened.
//! 2. **Idempotency** — a retry under the same key is answered with the first response and
//!    never reaches the payment service a second time.
//! 3. **PSP failure** — a charge the payment service never answers leaves the invoice held
//!    rather than stranded: refused to everyone while the outcome is unknown, and closed out
//!    by the reconciler once it can be established.
//!
//! Self-contained on purpose. The payment service is the stub at the bottom of this file, which
//! speaks mock-payment-service's contract under the mock's own card names, so none of this
//! needs that crate, its container, or the rest of the suite. Postgres is the one thing it does
//! need, because these are assertions about what is committed, not about what a handler
//! returned. `Psp`'s timeout is a constructor argument, so the timeout path runs in 200ms
//! rather than the five seconds production waits.
//!
//! ```sh
//! docker compose up -d postgres        # from the repo root
//! cd business
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/business \
//!     cargo test --test payment_safety
//! ```
//!
//! Each test gets its own freshly migrated database from `#[sqlx::test]`, so they are
//! order-independent and safe to run concurrently.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use business::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tower::ServiceExt;

/// `ready`, 125000 cents. The invoice every charge below goes for.
const READY: &str = "00000000-0000-4000-8000-000000000102";
/// The other `ready` one, so the two PSP failure modes get an invoice each.
const READY_ZERO_TOTAL: &str = "00000000-0000-4000-8000-000000000106";

/// The operator token. The charge endpoint does not require it — it is the payer's — and that
/// every request below succeeds in reaching the handler without one is part of the contract.
const TOKEN: &str = "test-token";

// ---------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------

/// Serves `router` on an ephemeral loopback port and returns its base URL.
///
/// The task is never joined: it lives until the test's runtime is dropped, which is exactly as
/// long as anything can still send it a request.
async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{address}")
}

/// An app wired to whatever is at `url`.
fn at(pool: PgPool, url: &str) -> (Router, AppState) {
    let state = AppState::new(
        pool,
        url,
        Duration::from_millis(200),
        Duration::from_millis(200),
        TOKEN,
    )
    .unwrap();
    (business::app(state.clone()), state)
}

/// An app whose payment service answers `reported` to a status lookup, and the stub itself so a
/// test can ask what the payment service was actually asked to do.
async fn app(pool: PgPool, reported: Option<&'static str>) -> (Router, AppState, Stub) {
    let stub = Stub::new(reported);
    let url = serve(stub_psp(stub.clone())).await;
    let (app, state) = at(pool, &url);
    (app, state, stub)
}

/// An app whose payment service holds every intent request until `attempts` are waiting, so a
/// burst of charges races into the claim instead of queueing politely behind each other. See
/// [`Stub::gated`], which is the reason the concurrency test below tests anything at all.
async fn app_gated(pool: PgPool, attempts: usize) -> (Router, Stub) {
    let stub = Stub::gated(attempts);
    let url = serve(stub_psp(stub.clone())).await;
    let (app, _) = at(pool, &url);
    (app, stub)
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// A charge under a key no other call will use.
///
/// The endpoint requires `Idempotency-Key`, and two of the three tests here are about something
/// else — so the key is generated and each call gets a fresh one, making every call a distinct
/// intended charge. The test that is *about* the key calls [`pay_with_key`].
async fn pay(app: &Router, invoice: &str, card_token: &str) -> (StatusCode, Value) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let key = format!("key_{:04}", NEXT.fetch_add(1, Ordering::SeqCst));
    pay_with_key(app, invoice, card_token, &key).await
}

async fn pay_with_key(
    app: &Router,
    invoice: &str,
    card_token: &str,
    key: &str,
) -> (StatusCode, Value) {
    let request = Request::post(format!("/invoices/{invoice}/pay"))
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(Body::from(json!({ "card_token": card_token }).to_string()))
        .unwrap();
    send(app, request).await
}

/// An invoice's state and the intent it is being charged by, if any.
async fn invoice(pool: &PgPool, id: &str) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT state::text, currently_processed_by_pi_id FROM invoices WHERE id = $1::uuid",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Every intent recorded against an invoice, as `(id, status)`, oldest label first.
async fn intents(pool: &PgPool, invoice: &str) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT id, status::text FROM payment_intents WHERE invoice_id = $1::uuid ORDER BY id",
    )
    .bind(invoice)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------------------
// 1. Concurrency
// ---------------------------------------------------------------------------------------

/// N charges of one invoice, arriving together. At most one may be charged — otherwise several
/// read `ready`, several proceed, and the customer is billed more than once.
///
/// The gate is what makes this a test rather than a hope. Left to the scheduler the attempts
/// trickle into `payments::claim` one at a time, each finds the invoice already taken, and each
/// is refused for a reason that never exercised the claim at all: ungated, only four of the
/// eight ever reach the payment service, so `held.len() <= 1` holds vacuously — against a
/// correct `claim` and a broken one alike. Gated, a `claim` whose `WHERE` has lost its state
/// test fails every time, with all eight attempts past the claim.
///
/// Every attempt carries its own key, so none of these is a retry and the idempotency table
/// cannot be what saves it.
///
/// The assertions are upper bounds rather than equalities on purpose. Which attempt wins, and
/// whether the winner gets far enough to record its own answer, are both genuinely racy; that
/// no card is charged twice is not. Pinning down the racy part is how this test would start
/// failing on a loaded machine for reasons that have nothing to do with money.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn concurrent_charges_of_one_invoice_charge_the_card_once(pool: PgPool) {
    const ATTEMPTS: usize = 8;
    let (app, stub) = app_gated(pool.clone(), ATTEMPTS).await;

    let mut burst = JoinSet::new();
    for n in 0..ATTEMPTS {
        let app = app.clone();
        burst.spawn(async move {
            pay_with_key(&app, READY, "tok_success", &format!("key_burst_{n}")).await
        });
    }
    let mut answers = Vec::with_capacity(ATTEMPTS);
    while let Some(joined) = burst.join_next().await {
        answers.push(joined.expect("a charge task panicked"));
    }
    assert_eq!(answers.len(), ATTEMPTS);

    // The gate only releases once all eight are through the pre-flight check, so all eight
    // minted an intent and all eight went for the claim. Without this the assertions below can
    // pass against a service that does no locking whatsoever.
    assert_eq!(stub.calls().0, ATTEMPTS, "the burst did not race");

    // Everything that is not a `409` is an attempt that got the invoice. There is at most one:
    // the winner holds it until it settles, and with a card that succeeds it only ever settles
    // into `processed`, which nobody can claim again.
    let (held, refused): (Vec<_>, Vec<_>) = answers
        .iter()
        .partition(|(status, _)| *status != StatusCode::CONFLICT);
    assert!(
        held.len() <= 1,
        "{} attempts got past the claim: {held:?}",
        held.len()
    );

    // Every loser said the same thing, and said it as a conflict rather than as a `500` or a
    // second `200`. A caller cannot tell which of them lost how, and should not have to.
    for (status, body) in &refused {
        assert_eq!(*status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "invoice_not_payable");
    }

    // The no-double-charge assertion proper, and the one nothing at the HTTP layer can make:
    // the payment service ran a charge for the attempt that held the invoice and for no other.
    // All eight minted intents — the documented cost of the PSP owning the id — but a minted
    // intent is not a charged card.
    assert_eq!(stub.calls().1, held.len());

    // Consistent means the invoice and the intents agree, in whichever of the three ways this
    // could have landed. Every intent that was never charged is still `pending` in all of them.
    let (state, pointer) = invoice(&pool, READY).await;
    let recorded = intents(&pool, READY).await;
    assert_eq!(recorded.len(), ATTEMPTS);
    let settled: Vec<_> = recorded.iter().filter(|(_, s)| s != "pending").collect();

    match held.first() {
        // Charged and recorded. The invoice is done and lets go of the intent that finished it.
        Some((StatusCode::OK, body)) => {
            assert_eq!(state, "processed");
            assert_eq!(pointer, None);
            assert_eq!(settled.len(), 1);
            assert_eq!(settled[0].1, "succeeded");
            assert_eq!(body["payment_intent_id"], settled[0].0.as_str());
        }

        // Charged, and the answer could not be written down — the winner's `settle` was itself
        // blocked out by a loser's rollback. Not a double charge and not a lost one: the invoice
        // is still held by that intent, which is precisely what the reconciler resolves.
        Some((StatusCode::GATEWAY_TIMEOUT, body)) => {
            assert_eq!(state, "processing");
            assert_eq!(pointer.as_deref(), body["payment_intent_id"].as_str());
            assert!(settled.is_empty());
        }

        // Nobody got through at all. Nothing was charged, so the invoice is exactly as it was
        // and is still on sale.
        None => {
            assert_eq!(state, "ready");
            assert_eq!(pointer, None);
            assert!(settled.is_empty());
        }

        Some(other) => panic!("unexpected answer from the winning attempt: {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------
// 2. Idempotency
// ---------------------------------------------------------------------------------------

/// The retry that actually happens. A client sees a `504`, cannot tell whether the card was
/// charged, and sends the identical request again.
///
/// That is the single moment where a second charge would be invisible to everybody involved:
/// the first attempt wrote no outcome, so a handler that ran again would find an invoice it is
/// itself holding and an intent it created. The key is what stops it, and what it hands back is
/// the first `504` — byte for byte, every time.
///
/// The unresolved response is the one worth retrying, which is why this uses it rather than a
/// `200`. Caching only successes is a plausible-looking implementation that leaves exactly this
/// case open, and a replay test built on a `200` cannot see it.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_retried_unresolved_charge_replays_without_charging_again(pool: PgPool) {
    let (app, _, stub) = app(pool.clone(), None).await;

    let first = pay_with_key(&app, READY, "tok_network_error", "key_unresolved").await;
    assert_eq!(first.0, StatusCode::GATEWAY_TIMEOUT);
    let intent = first.1["payment_intent_id"]
        .as_str()
        .expect("an unresolved charge still answers with the intent it created")
        .to_owned();
    assert_eq!(stub.calls(), (1, 1));

    // One key, one response, forever — so this is asked more than once. A cache that expired, or
    // a reservation quietly released by a failure, would show up on a later pass and not on the
    // first.
    for retry in 1..=3 {
        let again = pay_with_key(&app, READY, "tok_network_error", "key_unresolved").await;

        // The same status and the same body, not merely another `504`. The body is what carries
        // the intent id, and a retry answering with a *different* id would have created one.
        assert_eq!(again, first, "retry {retry}");

        // The point of the whole mechanism: the payment service was asked for nothing. No second
        // intent, and — the one that costs money — no second charge.
        assert_eq!(stub.calls(), (1, 1), "retry {retry}");
    }

    // And the retries left the charge they replayed exactly as it was: one intent, still
    // unresolved, still holding the invoice for the reconciler to close out.
    assert_eq!(
        intents(&pool, READY).await,
        [(intent.clone(), "pending".to_owned())]
    );
    assert_eq!(
        invoice(&pool, READY).await,
        ("processing".to_owned(), Some(intent))
    );
}

// ---------------------------------------------------------------------------------------
// 3. PSP failure
// ---------------------------------------------------------------------------------------

/// Neither way of not answering strands an invoice.
///
/// A charge that times out and a charge whose connection drops are the same fact — the card may
/// or may not have been charged — so neither is guessed at. `tok_timeout` outlives the client's
/// timeout; `tok_network_error` settles the charge at the payment service and then loses the
/// response, so the money really has moved in that one.
///
/// Held is not stuck, and the difference is only visible over time: at the moment of the failure
/// the two look identical. What tells them apart is that the invoice is refused to everyone
/// while the outcome is unknown, and that the reconciler closes it out afterwards.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unanswered_charge_is_held_rather_than_stranded(pool: PgPool) {
    let (app, state, stub) = app(pool.clone(), Some("succeeded")).await;

    // One invoice each, so the two failures cannot mask each other.
    let unanswered = [
        (READY, "tok_timeout"),
        (READY_ZERO_TOTAL, "tok_network_error"),
    ];

    for (id, card) in unanswered {
        let (status, body) = pay(&app, id, card).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{card}");
        let intent = body["payment_intent_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{card}: answered without an intent id"))
            .to_owned();

        // Held, not released. Releasing it is exactly what would let the same money be taken
        // twice, since the money may already have moved.
        assert_eq!(
            invoice(&pool, id).await,
            ("processing".to_owned(), Some(intent.clone())),
            "{card}"
        );

        // And not guessed at. Nothing was written about an outcome nobody knows — in particular
        // not `failed`, which is the guess that reads as harmless and is not.
        assert_eq!(
            intents(&pool, id).await,
            [(intent, "pending".to_owned())],
            "{card}"
        );

        // Held against everyone, including a caller holding a card that would work. This is the
        // bad state the invoice is *not* in: chargeable while an earlier charge for it may
        // already have gone through.
        let (status, body) = pay(&app, id, "tok_success").await;
        assert_eq!(status, StatusCode::CONFLICT, "{card}");
        assert_eq!(body["error"], "invoice_not_payable", "{card}");
    }

    // Two intents and two charges: one per invoice, from the attempts that were never answered.
    // The refusals above never reached the payment service, so a caller hammering a held invoice
    // cannot run a card again however many times it tries.
    assert_eq!(stub.calls(), (2, 2));

    // The way out, and the only one. Until this runs the invoices stay where they are — that is
    // the point of holding them — but they do not stay there.
    business::jobs::reconcile_once(&state).await;

    for (id, card) in unanswered {
        // Both charges did go through, which is what the payment service says and what neither
        // request could find out. The invoice is done and has let go of the intent.
        assert_eq!(
            invoice(&pool, id).await,
            ("processed".to_owned(), None),
            "{card}"
        );
        assert_eq!(intents(&pool, id).await[0].1, "succeeded", "{card}");

        // Still not payable, and now for the opposite reason: not "we do not know yet" but
        // "this is paid". An invoice that came out of the reconciler chargeable again would be
        // the double charge this whole path exists to prevent, arriving a day late.
        assert_eq!(
            pay(&app, id, "tok_success").await.0,
            StatusCode::CONFLICT,
            "{card}"
        );
    }
    assert_eq!(stub.calls(), (2, 2));
}

// ---------------------------------------------------------------------------------------
// The stub payment service
// ---------------------------------------------------------------------------------------

#[derive(Clone)]
struct Stub {
    next: Arc<AtomicUsize>,
    /// How many charges the stub has been asked for. What a test asserts on to show that a
    /// replayed response never reached the payment service at all.
    charges: Arc<AtomicUsize>,
    /// What `GET /payment_intents/{id}` answers; `None` is a 404, the shape of a payment service
    /// that has restarted and lost the intent.
    reported: Option<&'static str>,
    /// Holds every request for an intent until a set number of them have arrived. See
    /// [`Stub::gated`].
    gate: Option<Arc<Barrier>>,
}

impl Stub {
    fn new(reported: Option<&'static str>) -> Self {
        Self {
            next: Arc::new(AtomicUsize::new(0)),
            charges: Arc::new(AtomicUsize::new(0)),
            reported,
            gate: None,
        }
    }

    /// A stub that answers no intent request until `attempts` of them are waiting.
    ///
    /// This is what makes a concurrency test test anything. Charges left to their own devices
    /// trickle into `payments::claim` one at a time — each finds the invoice already taken and
    /// is refused, which is the right answer arrived at without ever running the race.
    ///
    /// Creating the intent is the right place to hold them: it is the last thing a charge does
    /// before it tries to claim, and it happens after the pre-flight check, so every attempt
    /// released here has already seen the invoice as `ready` and is committed to going for it.
    /// Nothing holds a database connection while it waits, so parking more attempts than the
    /// pool has connections is safe.
    ///
    /// `attempts` must be the number of charges that will actually reach the payment service,
    /// which is not always the number sent: a burst sharing one idempotency key never gets past
    /// the middleware. The wait is bounded rather than trusting the caller to get that right.
    fn gated(attempts: usize) -> Self {
        Self {
            gate: Some(Arc::new(Barrier::new(attempts))),
            ..Self::new(None)
        }
    }

    /// How many payment intents the stub has issued, and how many charges it has run. A replayed
    /// response should move neither.
    fn calls(&self) -> (usize, usize) {
        (
            self.next.load(Ordering::SeqCst),
            self.charges.load(Ordering::SeqCst),
        )
    }
}

/// Speaks mock-payment-service's contract under the mock's own card names, so a test naming
/// `tok_timeout` names the card an operator would reach for by hand. Only the three cards these
/// tests need are implemented; anything else gets the 400 the mock answers a typo with.
fn stub_psp(stub: Stub) -> Router {
    Router::new()
        .route("/payment_intents", post(create))
        .route("/payment_intents/{id}", get(status))
        .route("/payment_intents/{id}/pay", post(charge))
        .with_state(stub)
}

async fn create(State(stub): State<Stub>) -> Response {
    // Bounded, so a gate that is never going to fill fails the test instead of hanging it.
    if let Some(gate) = &stub.gate {
        let _ = tokio::time::timeout(Duration::from_secs(5), gate.wait()).await;
    }
    let id = format!("pi_stub_{:04}", stub.next.fetch_add(1, Ordering::SeqCst));
    (
        StatusCode::CREATED,
        Json(json!({ "payment_intent_id": id, "status": "pending" })),
    )
        .into_response()
}

async fn status(State(stub): State<Stub>, Path(id): Path<String>) -> Response {
    match stub.reported {
        Some(status) => Json(json!({ "payment_intent_id": id, "status": status })).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "payment_intent_not_found" })),
        )
            .into_response(),
    }
}

async fn charge(
    State(stub): State<Stub>,
    Path(id): Path<String>,
    Json(request): Json<Value>,
) -> Response {
    stub.charges.fetch_add(1, Ordering::SeqCst);

    match request["card_token"].as_str().unwrap_or_default() {
        "tok_success" => {
            Json(json!({ "payment_intent_id": id, "status": "succeeded" })).into_response()
        }

        // Outlives the client's timeout, the way the real mock's 30 second card does.
        "tok_timeout" => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Json(json!({ "payment_intent_id": id, "status": "succeeded" })).into_response()
        }

        // The charge lands and the response does not — the ambiguous failure the reconciler
        // exists for.
        "tok_network_error" => drop_connection(),

        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "unknown_card_token" })),
        )
            .into_response(),
    }
}

/// Copied from mock-payment-service: a body stream that errors on its first poll, so hyper
/// abandons the response and closes the connection before flushing a status line.
fn drop_connection() -> Response {
    let stream = futures_util::stream::once(async {
        Err::<Vec<u8>, io::Error>(io::Error::other("simulated network error"))
    });
    Response::new(Body::from_stream(stream))
}
