//! End-to-end tests for `POST /invoices/{id}/pay` and the daily reconciler.
//!
//! The payment service is a stub defined at the bottom of this file rather than the real
//! mock-payment-service crate: a path dev-dependency would tie the two services' build graphs
//! together for the sake of a router, and a stub can produce responses the mock has no card
//! for (a 5xx) as easily as the ones it does. It answers to the mock's own card names, so a
//! test naming `tok_timeout` is naming the card an operator would reach for by hand.
//!
//! `Psp`'s timeout is a constructor argument, so the timeout path here runs in 200ms rather
//! than the five seconds production waits.
//!
//! ```sh
//! docker compose up -d postgres    # from the repo root
//! cargo test                       # from business/
//! ```

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
use tower::ServiceExt;

const READY: &str = "00000000-0000-4000-8000-000000000102";
const READY_ZERO_TOTAL: &str = "00000000-0000-4000-8000-000000000106";
const DRAFT: &str = "00000000-0000-4000-8000-000000000101";
const PROCESSING: &str = "00000000-0000-4000-8000-000000000103";
const PROCESSED: &str = "00000000-0000-4000-8000-000000000104";
const VOID: &str = "00000000-0000-4000-8000-000000000105";
const IN_FLIGHT: &str = "pi_seed_103_inflight";

/// `GET /invoices/{id}` is an operator route and needs this. The charge endpoints below do not,
/// and that every test here still drives them without one is the assertion that they stayed
/// open to the person being billed.
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

/// An app wired to a stub PSP whose `GET /payment_intents/{id}` answers `reported`, or 404
/// when that is `None`.
async fn app_with(pool: PgPool, reported: Option<&'static str>) -> (Router, AppState) {
    let (app, state, _) = app_counting(pool, reported).await;
    (app, state)
}

/// As [`app_with`], and hands back the stub so a test can ask what the payment service was
/// actually asked to do.
async fn app_counting(pool: PgPool, reported: Option<&'static str>) -> (Router, AppState, Stub) {
    let stub = Stub::new(reported);
    let url = serve(stub_psp(stub.clone())).await;
    let (app, state) = at(pool, &url);
    (app, state, stub)
}

/// An app whose payment service holds every intent request until `attempts` are waiting, so a
/// burst of charges races into the claim instead of queueing politely behind each other.
async fn app_gated(pool: PgPool, attempts: usize) -> (Router, Stub) {
    let stub = Stub::gated(attempts);
    let url = serve(stub_psp(stub.clone())).await;
    let (app, _) = at(pool, &url);
    (app, stub)
}

/// An app wired to whatever is (or is not) at `url`.
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

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// A charge under a key no other call will use.
///
/// The endpoint requires `Idempotency-Key`, and almost every test here is about something else
/// — so the key is generated rather than passed in, and each call gets a fresh one. Two calls
/// in one test are two distinct charges, which is what these tests meant before the header
/// existed. Tests that are *about* the key call [`pay_with_key`].
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

async fn invoice_status(app: &Router, id: &str) -> (StatusCode, Value) {
    let request = Request::get(format!("/invoices/{id}"))
        .header("x-api-token", TOKEN)
        .body(Body::empty())
        .unwrap();
    send(app, request).await
}

async fn intent_status(app: &Router, id: &str) -> (StatusCode, Value) {
    let request = Request::get(format!("/payment_intents/{id}"))
        .body(Body::empty())
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

async fn intent_ids(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT id FROM payment_intents ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------------------
// The charge itself
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_ready_invoice_is_charged(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, READY, "tok_success").await;
    assert_eq!(status, StatusCode::OK);

    // The intent id and nothing else. It is the handle to the attempt, which is the one thing
    // a caller cannot read back off the invoice.
    let recorded = intents(&pool, READY).await;
    assert_eq!(body, json!({ "payment_intent_id": recorded[0].0 }));

    // The pointer is cleared in the same statement that leaves `processing`, so a settled
    // invoice never names the intent that settled it.
    assert_eq!(invoice(&pool, READY).await.1, None);
    assert_eq!(invoice(&pool, READY).await.0, "processed");
    assert_eq!(intents(&pool, READY).await[0].1, "succeeded");
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_declined_card_returns_the_invoice_to_ready(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, READY, "tok_card_declined").await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);

    // `charge_declined` says the charge was declined and stops there. The PSP's own `code` is
    // deliberately not carried through — why a card was declined is between the cardholder and
    // their bank — so `tok_insufficient_funds` answers with this same body, and the id is what
    // to ask about if the caller wants more than that.
    let recorded = intents(&pool, READY).await;
    assert_eq!(
        body,
        json!({ "error": "charge_declined", "payment_intent_id": recorded[0].0 })
    );

    // Chargeable again, with another card.
    assert_eq!(invoice(&pool, READY).await, ("ready".into(), None));
    assert_eq!(recorded[0].1, "failed");
}

/// The 30 second charge, in miniature. The money may already have moved, so nothing is written.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_slow_psp_leaves_the_charge_unresolved(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, READY, "tok_timeout").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);

    let (state, pointer) = invoice(&pool, READY).await;
    assert_eq!(state, "processing");
    let recorded = intents(&pool, READY).await;
    assert_eq!(pointer.as_deref(), Some(recorded[0].0.as_str()));
    assert_eq!(recorded[0].1, "pending");

    // The id is handed over precisely because the outcome was not. Without it there is no way
    // left to ask whether the card was charged. `charge_unresolved` names that width exactly:
    // not that the charge failed, but that this service cannot say either way.
    assert_eq!(
        body,
        json!({ "error": "charge_unresolved", "payment_intent_id": recorded[0].0 })
    );
}

/// `tok_network_error`'s shape: the PSP settles the charge and then loses the response.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_dropped_connection_leaves_the_charge_unresolved(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, READY, "tok_network_error").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(body["payment_intent_id"].is_string());

    assert_eq!(invoice(&pool, READY).await.0, "processing");
    assert_eq!(intents(&pool, READY).await[0].1, "pending");
}

/// The one that matters most. A PSP is allowed to fail after moving the money, so reading a
/// 5xx as a decline is how an invoice gets released and then charged a second time.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_psp_5xx_is_not_read_as_a_failure(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, READY, "tok_500").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(body["payment_intent_id"].is_string());

    assert_ne!(invoice(&pool, READY).await.0, "ready");
    assert_eq!(invoice(&pool, READY).await.0, "processing");
}

/// An unrecognized token is rejected before the charge stage, so nothing moved and the invoice
/// is safe to release. It is not special-cased: it is simply a charge that did not happen.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unrecognized_token_is_an_ordinary_failure(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    assert_eq!(
        pay(&app, READY, "tok_nonsense").await.0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert_eq!(invoice(&pool, READY).await, ("ready".into(), None));
}

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn only_a_ready_invoice_can_be_charged(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;
    let before = intent_ids(&pool).await;

    for id in [DRAFT, PROCESSING, PROCESSED, VOID] {
        let (status, body) = pay(&app, id, "tok_success").await;
        assert_eq!(status, StatusCode::CONFLICT, "{id}");
        assert_eq!(body["error"], "invoice_not_payable", "{id}");
    }

    // Rejected before an intent is created, not after: a caller hammering an unpayable invoice
    // must not leave a trail of intents behind at the PSP.
    assert_eq!(intent_ids(&pool).await, before);
}

/// Two charges of one invoice, arriving together. Exactly one may claim it — otherwise both
/// read `ready`, both proceed, and the customer is billed twice.
///
/// Gated, for the reason spelled out on [`Stub::gated`]: without it the loser is usually turned
/// away by the pre-flight check, having never reached the claim at all, and everything asserted
/// below about what it left behind is then up to the scheduler.
///
/// Two attempts is still too narrow to *detect* a broken claim with any reliability. Measured
/// against a `claim` with its `FOR UPDATE` deleted, this caught the resulting double charge 4
/// times in 20 ungated and 7 in 20 gated — the window between the claim's `SELECT` and its
/// `UPDATE` is small enough that two attempts mostly miss it whatever they are lined up with.
/// What guards that invariant is the eight-way burst below, which catches it every time. This
/// test is here to say plainly what the two parties are each told, which the burst does not.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_concurrent_charge_of_one_invoice_is_rejected(pool: PgPool) {
    let (app, stub) = app_gated(pool.clone(), 2).await;

    // Both use the slow card, so the winner is still mid-charge when the loser tries to claim.
    let (first, second) = tokio::join!(
        pay(&app, READY, "tok_timeout"),
        pay(&app, READY, "tok_timeout")
    );

    let (winner, loser) = if first.0 == StatusCode::GATEWAY_TIMEOUT {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(winner.0, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(loser.0, StatusCode::CONFLICT);
    assert_eq!(loser.1["error"], "invoice_not_payable");

    // Two intents and one charge. The second half is the whole point: both attempts got as far
    // as asking the payment service for an intent, and only one of them was allowed to put a
    // card through.
    assert_eq!(stub.calls(), (2, 1));

    let (state, pointer) = invoice(&pool, READY).await;
    assert_eq!(state, "processing");
    let pointer = pointer.expect("the winning attempt should hold the invoice");

    // Both were past the pre-flight check before either could claim, so the loser was turned
    // away by the claim itself — the only place that can be got wrong. It leaves its intent
    // behind, pointed at by nothing, which is the documented price of the PSP owning the id.
    let recorded = intents(&pool, READY).await;
    assert_eq!(recorded.len(), 2);
    assert!(recorded.iter().any(|(id, _)| *id == pointer));
    assert!(recorded.iter().all(|(_, status)| status == "pending"));
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unknown_invoice_is_not_found(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, "00000000-0000-4000-8000-0000000009ff", "tok_success").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "invoice_not_found");
    assert!(intent_ids(&pool).await.is_empty());
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_malformed_id_is_rejected(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = pay(&app, "not-a-uuid", "tok_success").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_invoice_id");
    assert!(intent_ids(&pool).await.is_empty());
}

/// Nothing was created and nothing moved, so this is a request that did not happen — not an
/// ambiguous charge.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unreachable_psp_is_a_bad_gateway(pool: PgPool) {
    // Port 1 is privileged and unbound: the connection is refused rather than timing out.
    let (app, _) = at(pool.clone(), "http://127.0.0.1:1");

    let (status, body) = pay(&app, READY_ZERO_TOTAL, "tok_success").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"], "payment_service_unavailable");

    assert_eq!(
        invoice(&pool, READY_ZERO_TOTAL).await,
        ("ready".into(), None)
    );
    assert!(intent_ids(&pool).await.is_empty());
}

/// A trailing slash in the configured URL would otherwise produce `//payment_intents`, which
/// axum does not route — a typo that only shows up at the first charge.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_trailing_slash_in_the_url_is_tolerated(pool: PgPool) {
    let url = serve(stub_psp(Stub::new(None))).await;
    let (app, _) = at(pool.clone(), &format!("{url}/"));

    assert_eq!(pay(&app, READY, "tok_success").await.0, StatusCode::OK);
}

// ---------------------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------------------

/// The reason the header exists. A client whose charge timed out retries the identical request,
/// and gets the identical answer — without the card being charged a second time.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_retry_with_the_same_key_replays_the_first_response(pool: PgPool) {
    let (app, _, stub) = app_counting(pool.clone(), None).await;

    let first = pay_with_key(&app, READY, "tok_success", "key_retry").await;
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(stub.calls(), (1, 1));

    let second = pay_with_key(&app, READY, "tok_success", "key_retry").await;

    // Not merely "also a 200": the same status and the same body, which is the promise the key
    // makes. The invoice is `processed` by now, so a second run of the handler could not have
    // produced this — it would have been turned away with `invoice_not_payable`.
    assert_eq!(second, first);

    // The payment service was not asked for anything the second time: no intent, no charge.
    assert_eq!(stub.calls(), (1, 1));
    assert_eq!(intents(&pool, READY).await.len(), 1);
    assert_eq!(invoice(&pool, READY).await, ("processed".into(), None));
}

/// A key stands for one request. Sent again with a different body it is a caller bug, and the
/// only answer that does not either charge twice or answer a question nobody asked is to say so.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_key_reused_with_a_different_body_is_rejected(pool: PgPool) {
    let (app, _, stub) = app_counting(pool.clone(), None).await;

    assert_eq!(
        pay_with_key(&app, READY, "tok_card_declined", "key_reused")
            .await
            .0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert_eq!(stub.calls(), (1, 1));

    let (status, body) = pay_with_key(&app, READY, "tok_success", "key_reused").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "idempotency_key_reused");

    // The declined charge left the invoice payable again, so this was refused on the key alone
    // rather than on anything about the invoice.
    assert_eq!(stub.calls(), (1, 1));
    assert_eq!(invoice(&pool, READY).await, ("ready".into(), None));
}

/// The header is required, and refused before anything happens. A charge with no key is one
/// nobody can safely retry, which on this endpoint is the whole problem.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_charge_without_an_idempotency_key_is_rejected(pool: PgPool) {
    let (app, _, stub) = app_counting(pool.clone(), None).await;

    let request = Request::post(format!("/invoices/{READY}/pay"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "card_token": "tok_success" }).to_string(),
        ))
        .unwrap();
    let (status, body) = send(&app, request).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "idempotency_key_missing");
    assert_eq!(stub.calls(), (0, 0));
    assert_eq!(invoice(&pool, READY).await, ("ready".into(), None));
    assert!(intent_ids(&pool).await.is_empty());
}

/// The one case where the same key cannot be given the same answer: the first request is still
/// charging and there is no answer yet. Waiting for one would hold a pool connection for the
/// length of a charge, so the caller is told what is true and can ask again.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_duplicate_arriving_mid_charge_is_told_it_is_in_flight(pool: PgPool) {
    let (app, _, stub) = app_counting(pool.clone(), None).await;

    // The slow card outlives the client's timeout, so the first request is still in the PSP
    // when the second arrives.
    let (first, second) = tokio::join!(
        pay_with_key(&app, READY, "tok_timeout", "key_inflight"),
        pay_with_key(&app, READY, "tok_timeout", "key_inflight"),
    );

    let (winner, loser) = if first.0 == StatusCode::GATEWAY_TIMEOUT {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(winner.0, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(loser.0, StatusCode::CONFLICT);
    assert_eq!(loser.1["error"], "idempotency_key_in_flight");

    // Turned away by the key, not by the invoice: the loser never reached the handler, so it
    // never created an intent of its own the way a losing claim would.
    assert_eq!(stub.calls(), (1, 1));
    assert_eq!(intents(&pool, READY).await.len(), 1);

    // Once the winner's answer is recorded, the same key produces it rather than a conflict.
    assert_eq!(
        pay_with_key(&app, READY, "tok_timeout", "key_inflight").await,
        winner
    );
    assert_eq!(stub.calls(), (1, 1));
}

// ---------------------------------------------------------------------------------------
// Payment intent status
// ---------------------------------------------------------------------------------------

/// The endpoint reports the row, and only the row. The payment service is not asked, so a stub
/// that would answer `succeeded` for anything changes nothing about what comes back — and
/// nothing is settled as a side effect of a read.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn a_status_is_read_from_the_recorded_row(pool: PgPool) {
    let (app, _, stub) = app_counting(pool.clone(), Some("succeeded")).await;

    // Settled as `failed`. A handler that asked the stub would answer `succeeded` instead.
    assert_eq!(
        intent_status(&app, "pi_seed_102_declined").await.1,
        json!({ "payment_intent_id": "pi_seed_102_declined", "status": "failed" })
    );

    // Unresolved, with its invoice still being charged by it — the case most tempting to
    // resolve on the caller's behalf. It reads `pending`, and the invoice is left where it was.
    assert_eq!(
        intent_status(&app, IN_FLIGHT).await.1,
        json!({ "payment_intent_id": IN_FLIGHT, "status": "pending" })
    );
    assert_eq!(stub.calls(), (0, 0));
    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
}

/// `pending` means *this service does not know yet*, not *the card was not charged*. The charge
/// answered with an id and nothing else, and that id reads `pending` until the reconciler asks
/// the payment service what really happened.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unresolved_charge_reads_as_pending_until_the_reconciler_settles_it(pool: PgPool) {
    let (app, state, _) = app_counting(pool.clone(), Some("succeeded")).await;

    let (status, body) = pay(&app, READY, "tok_network_error").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    let intent = body["payment_intent_id"].as_str().unwrap().to_owned();

    assert_eq!(intent_status(&app, &intent).await.1["status"], "pending");
    assert_eq!(invoice(&pool, READY).await.0, "processing");

    // The job that does resolve it. The same id then reads what actually happened.
    business::jobs::reconcile_once(&state).await;

    assert_eq!(intent_status(&app, &intent).await.1["status"], "succeeded");
    assert_eq!(invoice(&pool, READY).await, ("processed".into(), None));
}

/// An intent whose attempt lost the claim was never charged, and nothing will ever settle it.
/// It reads `pending` for good — which is what the row says, and therefore what is reported.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_intent_nothing_is_charging_reads_as_pending(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    sqlx::query(
        "INSERT INTO payment_intents (id, invoice_id, status) VALUES ($1, $2::uuid, 'pending')",
    )
    .bind("pi_never_claimed")
    .bind(READY)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        intent_status(&app, "pi_never_claimed").await.1["status"],
        "pending"
    );
    assert_eq!(invoice(&pool, READY).await, ("ready".into(), None));
}

/// Reading a status never touches the network, so a payment service that is down does not take
/// this endpoint down with it.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn a_status_is_readable_with_the_psp_unreachable(pool: PgPool) {
    let (app, _) = at(pool.clone(), "http://127.0.0.1:1");

    let (status, body) = intent_status(&app, IN_FLIGHT).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "pending");
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unknown_intent_is_not_found(pool: PgPool) {
    let (app, _) = app_with(pool, None).await;

    let (status, body) = intent_status(&app, "pi_no_such_thing").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "payment_intent_not_found");
}

// ---------------------------------------------------------------------------------------
// Invoice status
// ---------------------------------------------------------------------------------------

/// Where the outcome of a charge is read off the invoice. `pay` answers with an intent id,
/// which says what happened to the attempt; this says what happened to the invoice.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_invoice_reports_the_state_a_charge_left_it_in(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (status, body) = invoice_status(&app, READY).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "id": READY,
            "customer_id": "00000000-0000-4000-8000-000000000001",
            "total_cents": 125000,
            "due_date": body["due_date"],
            "state": "ready",
        })
    );

    assert_eq!(pay(&app, READY, "tok_success").await.0, StatusCode::OK);
    assert_eq!(invoice_status(&app, READY).await.1["state"], "processed");
}

/// A charge with no answer leaves the two endpoints disagreeing, and that is not a bug: the
/// attempt is unresolved and the invoice is held for it. Reading either must not hide the other.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unresolved_charge_is_visible_from_both_endpoints(pool: PgPool) {
    let (app, _) = app_with(pool.clone(), None).await;

    let (_, body) = pay(&app, READY, "tok_network_error").await;
    let intent = body["payment_intent_id"].as_str().unwrap();

    assert_eq!(invoice_status(&app, READY).await.1["state"], "processing");
    assert_eq!(intent_status(&app, intent).await.1["status"], "pending");
}

/// Reading an invoice never resolves anything. `GET /payment_intents/{id}` is what settles a
/// charge; making an ordinary read do it would put the payment service in the path of every
/// invoice lookup.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn reading_an_invoice_settles_nothing(pool: PgPool) {
    // The stub answers `succeeded` for every status lookup, so anything that asked would settle.
    let (app, _, stub) = app_counting(pool.clone(), Some("succeeded")).await;

    assert_eq!(
        invoice_status(&app, PROCESSING).await.1["state"],
        "processing"
    );

    assert_eq!(stub.calls(), (0, 0));
    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn reading_an_unknown_or_malformed_invoice_is_rejected(pool: PgPool) {
    let (app, _) = app_with(pool, None).await;

    let (status, body) = invoice_status(&app, "00000000-0000-4000-8000-000000000999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "invoice_not_found");

    let (status, body) = invoice_status(&app, "not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_invoice_id");
}

// ---------------------------------------------------------------------------------------
// The reconciler
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn the_reconciler_settles_a_succeeded_intent(pool: PgPool) {
    let (_, state) = app_with(pool.clone(), Some("succeeded")).await;

    business::jobs::reconcile_once(&state).await;

    assert_eq!(invoice(&pool, PROCESSING).await, ("processed".into(), None));
    assert_eq!(intents(&pool, PROCESSING).await[0].1, "succeeded");
}

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn the_reconciler_settles_a_failed_intent(pool: PgPool) {
    let (_, state) = app_with(pool.clone(), Some("failed")).await;

    business::jobs::reconcile_once(&state).await;

    assert_eq!(invoice(&pool, PROCESSING).await, ("ready".into(), None));
    assert_eq!(intents(&pool, PROCESSING).await[0].1, "failed");
}

/// Still being charged. The PSP promises a terminal state within 24 hours, so the next daily
/// pass finds it settled.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn the_reconciler_leaves_a_pending_intent_alone(pool: PgPool) {
    let (_, state) = app_with(pool.clone(), Some("pending")).await;

    business::jobs::reconcile_once(&state).await;

    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
    assert_eq!(intents(&pool, PROCESSING).await[0].1, "pending");
}

/// The PSP's state is in memory and does not survive a restart, so an intent it has forgotten
/// may still have been charged. Guessing either way is how an invoice gets billed twice or
/// written off unpaid, so it is left exactly as it was.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn the_reconciler_leaves_an_intent_the_psp_has_forgotten(pool: PgPool) {
    let (_, state) = app_with(pool.clone(), None).await;

    business::jobs::reconcile_once(&state).await;

    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
}

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn an_unreachable_psp_leaves_the_reconciler_harmless(pool: PgPool) {
    let (_, state) = at(pool.clone(), "http://127.0.0.1:1");

    business::jobs::reconcile_once(&state).await;

    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
}

/// The whole loop, from an unresolved charge to a settled invoice: a charge whose response was
/// lost, then the pass that finds out what really happened.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn the_reconciler_closes_a_charge_whose_answer_was_lost(pool: PgPool) {
    let (app, state) = app_with(pool.clone(), Some("succeeded")).await;

    assert_eq!(
        pay(&app, READY, "tok_network_error").await.0,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(invoice(&pool, READY).await.0, "processing");

    business::jobs::reconcile_once(&state).await;

    assert_eq!(invoice(&pool, READY).await, ("processed".into(), None));
    assert_eq!(intents(&pool, READY).await[0].1, "succeeded");
}

/// A settlement that cannot take every row it needs defers, it does not fail. Holding the
/// intent row is the only way to reach that from outside — the invoice is taken first and its
/// own timeout is already covered — and the pass has to leave the charge exactly as it found
/// it, then close it on the next run once the row is free.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn a_settlement_that_cannot_take_the_intent_row_waits_for_the_next_pass(pool: PgPool) {
    let (_, state) = app_with(pool.clone(), Some("succeeded")).await;

    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM payment_intents WHERE id = $1 FOR UPDATE")
        .bind(IN_FLIGHT)
        .execute(&mut *holder)
        .await
        .unwrap();

    business::jobs::reconcile_once(&state).await;

    assert_eq!(
        invoice(&pool, PROCESSING).await,
        ("processing".into(), Some(IN_FLIGHT.into()))
    );
    assert_eq!(intents(&pool, PROCESSING).await[0].1, "pending");

    holder.rollback().await.unwrap();
    business::jobs::reconcile_once(&state).await;

    assert_eq!(invoice(&pool, PROCESSING).await, ("processed".into(), None));
    assert_eq!(intents(&pool, PROCESSING).await[0].1, "succeeded");
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
    /// What `GET /payment_intents/{id}` answers; `None` is a 404, the shape of a PSP that has
    /// restarted and lost the intent.
    reported: Option<&'static str>,
    /// Holds every request for an intent until a set number of them have arrived, so a burst
    /// of charges reaches `payments::claim` together rather than in whatever order the
    /// scheduler happened to run them in. See [`Stub::gated`].
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
    /// is refused, which is the right answer arrived at without ever running the race. Measured
    /// against this file's own suite, a `claim` with its `FOR UPDATE` deleted passed an
    /// eight-way burst without the gate and fails it with one.
    ///
    /// Creating the intent is the right place to hold them: it is the last thing a charge does
    /// before it tries to claim, and it happens after the pre-flight check, so every attempt
    /// released here has already seen the invoice as `ready` and is committed to going for it.
    /// Nothing is holding a database connection while it waits, so parking more attempts than
    /// the pool has connections is safe.
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

    /// How many payment intents the stub has issued, and how many charges it has run. A
    /// replayed response should move neither.
    fn calls(&self) -> (usize, usize) {
        (
            self.next.load(Ordering::SeqCst),
            self.charges.load(Ordering::SeqCst),
        )
    }
}

/// Speaks mock-payment-service's contract, branching on the card token so one stub covers
/// every outcome the client has to tell apart.
///
/// The cards are the mock's, by the mock's names, with two the mock does not have: `tok_500`,
/// because a payment service is allowed to fail after moving money and nothing there simulates
/// it, and any unrecognized token, which is the 400 the mock answers a typo with.
fn stub_psp(stub: Stub) -> Router {
    Router::new()
        .route("/payment_intents", post(create))
        .route("/payment_intents/{id}", get(status))
        .route("/payment_intents/{id}/pay", post(charge))
        .with_state(stub)
}

async fn create(State(stub): State<Stub>) -> Response {
    // Bounded, so a gate that is never going to fill fails the test instead of hanging it.
    // Waiting for eight attempts when only one can reach this — a burst sharing one idempotency
    // key, where seven are turned away by the middleware before the handler runs — is an easy
    // mistake to make, and a test that hangs says nothing about which mistake it was.
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
    let settled = |status: &str| Json(json!({ "payment_intent_id": id, "status": status }));

    match request["card_token"].as_str().unwrap_or_default() {
        "tok_success" => settled("succeeded").into_response(),
        "tok_card_declined" => Json(json!({
            "payment_intent_id": id, "status": "failed", "code": "card_declined"
        }))
        .into_response(),

        // Outlives the client's timeout, the way the real mock's 30 second card does.
        "tok_timeout" => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            settled("succeeded").into_response()
        }

        // The charge lands and the response does not — the ambiguous failure that the whole
        // reconciler exists for.
        "tok_network_error" => drop_connection(),
        "tok_500" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "charge_failed" })),
        )
            .into_response(),

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
