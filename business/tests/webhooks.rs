//! End-to-end tests for the webhook outbox, its signing, and its retry schedule.
//!
//! Both stubs are defined at the bottom of this file rather than pulled in as crates, the same
//! reading `tests/pay.rs` takes: a stub can produce what a real service has no way to — a
//! receiver that fails exactly twice, one that answers slower than the timeout — as easily as
//! the ordinary case.
//!
//! The signature is recomputed here with `hmac` and `sha2` directly rather than by calling the
//! crate's own `sign`. A test that signed with the code under test would pass whatever that code
//! did; this one fails if the signed string ever stops being `"{id}.{timestamp}.{body}"`, which
//! is the part a receiver depends on.
//!
//! Delivery is driven by calling `jobs::deliver_once` rather than by waiting on the spawned
//! loop, exactly as the reconciler tests drive `reconcile_once`.
//!
//! ```sh
//! docker compose up -d postgres    # from the repo root
//! cargo test                       # from business/
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use business::AppState;
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::PgPool;
use std::sync::Mutex;
use tower::ServiceExt;

const TOKEN: &str = "test-token";
const ADA: &str = "00000000-0000-4000-8000-000000000001";
const READY: &str = "00000000-0000-4000-8000-000000000102";
const RETIRED: &str = "00000000-0000-4000-8000-000000000203";
const SECRET: &str = "whsec_test";

/// The dispatcher's first backoff, in seconds. Duplicated from `delivery::BACKOFF` on purpose:
/// the constant is private, and a test that imported it could not tell a schedule that changed
/// from one that was always this.
const FIRST_BACKOFF: f64 = 10.0;

// ---------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------

/// Serves `router` on an ephemeral loopback port and returns its base URL.
async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{address}")
}

/// An app wired to a stub PSP, with the webhook timeout given in milliseconds so the timeout
/// path runs in a test rather than in ten seconds.
async fn app_with(pool: PgPool, webhook_timeout: Duration) -> (Router, AppState) {
    let psp = serve(stub_psp()).await;
    let state = AppState::new(
        pool,
        &psp,
        Duration::from_millis(200),
        webhook_timeout,
        TOKEN,
    )
    .unwrap();
    (business::app(state.clone()), state)
}

async fn app(pool: PgPool) -> (Router, AppState) {
    app_with(pool, Duration::from_millis(500)).await
}

/// Replaces whatever the fixture configured with one endpoint pointing at `url`.
///
/// Called before the event is raised, because the fan-out is decided at enqueue: an endpoint
/// added afterwards is not owed an event that predates it, which is what `GET /events` is for.
async fn only_endpoint(pool: &PgPool, url: &str) -> String {
    sqlx::query("UPDATE webhook_endpoints SET disabled_at = now() WHERE disabled_at IS NULL")
        .execute(pool)
        .await
        .unwrap();

    let (id,): (String,) = sqlx::query_as(
        "INSERT INTO webhook_endpoints (url, secret) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(url)
    .bind(SECRET)
    .fetch_one(pool)
    .await
    .unwrap();
    id
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn read(app: &Router, path: &str) -> (StatusCode, Value) {
    let request = Request::get(path)
        .header("x-api-token", TOKEN)
        .body(Body::empty())
        .unwrap();
    send(app, request).await
}

/// Raises an invoice through the API, so the transaction under test is the handler's own.
async fn create_invoice(app: &Router) -> String {
    let request = Request::post("/invoices")
        .header("content-type", "application/json")
        .header("x-api-token", TOKEN)
        .body(Body::from(
            json!({
                "customer_id": ADA,
                "due_date": "2026-10-01",
                "line_items": [{"description": "Consulting", "quantity": 2, "unit_amount_cents": 500}]
            })
            .to_string(),
        ))
        .unwrap();
    let (status, body) = send(app, request).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

async fn pay(app: &Router, invoice: &str, card_token: &str) -> (StatusCode, Value) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let key = format!("key_{:04}", NEXT.fetch_add(1, Ordering::SeqCst));
    let request = Request::post(format!("/invoices/{invoice}/pay"))
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(Body::from(json!({ "card_token": card_token }).to_string()))
        .unwrap();
    send(app, request).await
}

/// Every event recorded, oldest first, as `(type, payload)`.
async fn events(pool: &PgPool) -> Vec<(String, Value)> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT type, payload::text FROM webhook_events ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
    rows.into_iter()
        .map(|(kind, payload)| (kind, serde_json::from_str(&payload).unwrap()))
        .collect()
}

/// One delivery's bookkeeping: status, attempts, last status, last error.
async fn delivery(pool: &PgPool) -> (String, i32, Option<i16>, Option<String>) {
    sqlx::query_as(
        "SELECT status::text, attempts, last_status, last_error
           FROM webhook_deliveries ORDER BY id LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn deliveries(pool: &PgPool) -> i64 {
    sqlx::query_as::<_, (i64,)>("SELECT count(*) FROM webhook_deliveries")
        .fetch_one(pool)
        .await
        .unwrap()
        .0
}

/// How many seconds from now the only delivery is next due.
async fn due_in(pool: &PgPool) -> f64 {
    sqlx::query_as::<_, (f64,)>(
        "SELECT EXTRACT(EPOCH FROM (next_attempt_at - now()))::double precision
           FROM webhook_deliveries ORDER BY id LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap()
    .0
}

/// Makes the only delivery due now, undoing the lease the last pass took.
async fn make_due(pool: &PgPool) {
    sqlx::query("UPDATE webhook_deliveries SET next_attempt_at = now()")
        .execute(pool)
        .await
        .unwrap();
}

/// The verification recipe a receiver runs, written out here rather than delegated to the crate's
/// own `sign`: a test that signed with the code under test would pass whatever that code did.
///
/// Recompute `HMAC-SHA256(secret, "{id}.{timestamp}.{body}")` over the *raw* body as it arrived,
/// hex it, and compare against the `v1=` in `webhook-signature`. A real receiver additionally
/// checks that the timestamp is inside its tolerance window and that the id is one it has not
/// already processed.
fn verifies(received: &Received) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("{}.{}.{}", received.id, received.timestamp, received.body).as_bytes());
    let expected: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    received.signature == format!("v1={expected}")
}

// ---------------------------------------------------------------------------------------
// Queueing
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn raising_an_invoice_queues_one_delivery_per_enabled_endpoint(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    create_invoice(&app).await;

    let events = events(&pool).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "invoice.created");
    assert_eq!(events[0].1["invoice"]["state"], "draft");
    assert_eq!(events[0].1["invoice"]["total_cents"], 1000);

    // Two enabled endpoints in the fixture, and a third that was dropped from the configuration
    // at some earlier boot. The retired one is owed nothing.
    assert_eq!(deliveries(&pool).await, 2);
    let owed: Vec<(String,)> =
        sqlx::query_as("SELECT endpoint_id::text FROM webhook_deliveries ORDER BY endpoint_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(owed.iter().all(|(id,)| id != RETIRED));
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_event_with_nobody_listening_is_still_recorded(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    create_invoice(&app).await;

    // No endpoints at all, so nothing is owed — but the event happened, and a receiver
    // configured tomorrow can still find it. An event that left no trace because nobody was
    // listening at the time is one that could never be caught up on.
    assert_eq!(events(&pool).await.len(), 1);
    assert_eq!(deliveries(&pool).await, 0);

    let (status, body) = read(&app, "/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[0]["type"], "invoice.created");
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_charge_queues_invoice_paid_and_a_decline_queues_payment_failed(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    assert_eq!(
        pay(&app, READY, "tok_declined").await.0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert_eq!(pay(&app, READY, "tok_success").await.0, StatusCode::OK);

    let events = events(&pool).await;
    let kinds: Vec<&str> = events.iter().map(|(kind, _)| kind.as_str()).collect();
    assert_eq!(kinds, ["invoice.payment_failed", "invoice.paid"]);

    // A decline puts the invoice back where it can be charged again; a success ends it. The
    // payload carries the state at the moment of the event, which is what a receiver acts on.
    assert_eq!(events[0].1["invoice"]["state"], "ready");
    assert_eq!(events[1].1["invoice"]["state"], "processed");
    assert!(
        events[1].1["payment_intent_id"]
            .as_str()
            .unwrap()
            .starts_with("pi_")
    );
}

/// The event is queued by `payments::settle`, which the reconciler runs too — so a charge whose
/// response was lost is announced when the reconciler closes it out, with no second code path.
#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn the_reconciler_emits_the_event_a_live_charge_would_have(pool: PgPool) {
    let (app, state) = app(pool.clone()).await;

    assert_eq!(
        pay(&app, READY, "tok_drop").await.0,
        StatusCode::GATEWAY_TIMEOUT
    );
    // Nothing is known, so nothing is announced.
    assert!(events(&pool).await.is_empty());

    business::jobs::reconcile_once(&state).await;

    let events = events(&pool).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "invoice.paid");
    assert_eq!(events[0].1["invoice"]["state"], "processed");
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_charge_that_settles_nothing_announces_nothing(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    // Not `ready`, so the claim never happens and no settlement follows it.
    let (status, _) = pay(&app, "00000000-0000-4000-8000-000000000101", "tok_success").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(events(&pool).await.is_empty());
}

// ---------------------------------------------------------------------------------------
// Signing
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_delivery_is_signed_so_a_receiver_can_verify_it(pool: PgPool) {
    let receiver = Receiver::new();
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;
    business::jobs::deliver_once(&state).await;

    let got = receiver.received();
    assert_eq!(got.len(), 1);
    let received = &got[0];

    assert!(verifies(received), "the signature does not check out");

    // The id in the header is the event's, so deduplicating on it deduplicates the event — and
    // it is inside the signed string, so it cannot be swapped for a fresh one.
    let body: Value = serde_json::from_str(&received.body).unwrap();
    assert_eq!(body["id"], received.id);
    assert_eq!(body["type"], "invoice.created");
    assert_eq!(received.attempt, "1");

    // A plausible timestamp, which is what a receiver's tolerance window is checked against.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let sent: i64 = received.timestamp.parse().unwrap();
    assert!((now - sent).abs() < 60, "timestamp {sent} against {now}");
}

/// What a receiver reads back while reconciling is what it would have been sent.
#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn the_event_log_shows_the_body_that_was_delivered(pool: PgPool) {
    let receiver = Receiver::new();
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;
    business::jobs::deliver_once(&state).await;

    let delivered: Value = serde_json::from_str(&receiver.received()[0].body).unwrap();
    let (_, logged) = read(&app, "/events").await;
    assert_eq!(logged[0], delivered);
}

// ---------------------------------------------------------------------------------------
// Retrying
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_rejected_delivery_is_rescheduled_rather_than_dropped(pool: PgPool) {
    let receiver = Receiver::new().failing(1);
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;
    business::jobs::deliver_once(&state).await;

    let (status, attempts, last_status, last_error) = delivery(&pool).await;
    assert_eq!((status.as_str(), attempts), ("pending", 1));
    assert_eq!(last_status, Some(500));
    assert_eq!(
        last_error, None,
        "a receiver that answered has no transport error"
    );

    // Due again after the first backoff, give or take the ±10% jitter.
    let due = due_in(&pool).await;
    assert!(
        due > FIRST_BACKOFF * 0.85 && due < FIRST_BACKOFF * 1.15,
        "next attempt due in {due}s, expected about {FIRST_BACKOFF}s"
    );

    // Not due yet, so a pass that runs in between leaves it alone rather than hammering the
    // receiver at the dispatcher's tick rate.
    business::jobs::deliver_once(&state).await;
    assert_eq!(receiver.received().len(), 1);

    // And when it is due, the same event goes out again — same id, so a receiver that did get
    // the first one recognises the duplicate, with the attempt number to say which try it is.
    make_due(&pool).await;
    business::jobs::deliver_once(&state).await;

    let got = receiver.received();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].id, got[1].id);
    assert_eq!(got[1].attempt, "2");
    // Each attempt is signed over its own timestamp. Two attempts inside the same second sign
    // the same string and so carry the same signature -- which is why a receiver's replay
    // protection is the tolerance window *and* the id, not the signature being unique.
    assert!(
        got.iter().all(verifies),
        "every attempt verifies on its own"
    );
    assert!(got[1].timestamp >= got[0].timestamp);
    assert_eq!(delivery(&pool).await.0, "succeeded");
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_receiver_that_never_answers_records_the_transport_error(pool: PgPool) {
    // Port 1, which nothing listens on: a refused connection rather than a status.
    only_endpoint(&pool, "http://127.0.0.1:1/hooks").await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;
    business::jobs::deliver_once(&state).await;

    let (status, attempts, last_status, last_error) = delivery(&pool).await;
    assert_eq!((status.as_str(), attempts), ("pending", 1));
    assert_eq!(last_status, None);
    assert!(
        last_error.is_some(),
        "nothing answered, so the reason is the transport's"
    );
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_receiver_slower_than_the_timeout_counts_as_a_failure(pool: PgPool) {
    let receiver = Receiver::new().slow(Duration::from_millis(400));
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app_with(pool.clone(), Duration::from_millis(100)).await;
    create_invoice(&app).await;
    business::jobs::deliver_once(&state).await;

    let (status, attempts, last_status, last_error) = delivery(&pool).await;
    assert_eq!((status.as_str(), attempts), ("pending", 1));
    assert_eq!(last_status, None);
    assert!(last_error.is_some());
}

/// The budget is finite, and running out of it is a terminal state rather than a slower loop.
#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_delivery_that_spends_its_budget_is_given_up_on(pool: PgPool) {
    let receiver = Receiver::new().failing(usize::MAX);
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;

    // Eight attempts, with the wait between them collapsed — the schedule is asserted above,
    // and what matters here is what the eighth failure leaves behind.
    for _ in 0..8 {
        make_due(&pool).await;
        business::jobs::deliver_once(&state).await;
    }

    let (status, attempts, last_status, _) = delivery(&pool).await;
    assert_eq!((status.as_str(), attempts), ("exhausted", 8));
    assert_eq!(
        last_status,
        Some(500),
        "the last failure is kept, not flattened"
    );
    assert_eq!(receiver.received().len(), 8);

    // Nothing picks it up again, however due it looks.
    make_due(&pool).await;
    business::jobs::deliver_once(&state).await;
    assert_eq!(receiver.received().len(), 8);
    assert_eq!(delivery(&pool).await.1, 8);

    // And it is findable, which is the point of leaving the row rather than deleting it.
    let (_, listed) = read(&app, "/webhook_deliveries?status=exhausted").await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["event_type"], "invoice.created");
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_delivered_webhook_is_not_sent_again(pool: PgPool) {
    let receiver = Receiver::new();
    let url = serve(stub_receiver(receiver.clone())).await;
    only_endpoint(&pool, &format!("{url}/hooks")).await;

    let (app, state) = app(pool.clone()).await;
    create_invoice(&app).await;

    business::jobs::deliver_once(&state).await;
    make_due(&pool).await;
    business::jobs::deliver_once(&state).await;

    assert_eq!(receiver.received().len(), 1);
    let (status, attempts, last_status, _) = delivery(&pool).await;
    assert_eq!(
        (status.as_str(), attempts, last_status),
        ("succeeded", 1, Some(200))
    );
}

// ---------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------

#[test]
fn configuration_that_would_sign_with_nothing_is_refused() {
    use business::webhooks::parse;

    assert!(parse("[]").unwrap().is_empty());
    assert_eq!(
        parse(r#"[{"url":"https://a.example/h","secret":"s"}]"#)
            .unwrap()
            .len(),
        1
    );

    // Each of these is a deployment that would come up looking configured and be anything but.
    assert!(parse("not json").is_err());
    assert!(parse(r#"[{"url":"ftp://a.example/h","secret":"s"}]"#).is_err());
    assert!(parse(r#"[{"url":"https://a.example/h","secret":""}]"#).is_err());
    assert!(parse(r#"[{"url":"https://a.example/h"}]"#).is_err());
    // A typo'd key would otherwise be an endpoint with no secret at all.
    assert!(parse(r#"[{"url":"https://a.example/h","secrets":"s"}]"#).is_err());
    // Two rows for one URL is a statement Postgres refuses; caught here, where it can be named.
    assert!(
        parse(
            r#"[{"url":"https://a.example/h","secret":"s"},
                  {"url":"https://a.example/h","secret":"t"}]"#
        )
        .is_err()
    );
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn the_configuration_decides_which_endpoints_receive(pool: PgPool) {
    use business::webhooks::{Endpoint, sync};

    let configured = |url: &str, secret: &str| Endpoint {
        url: url.into(),
        secret: secret.into(),
    };

    // A boot that keeps one fixture endpoint with a new secret, revives the retired one, and
    // adds a third. The other fixture endpoint is not listed, so it stops receiving.
    sync(
        &pool,
        &[
            configured("http://127.0.0.1:1/hooks/primary", "whsec_rotated"),
            configured("http://127.0.0.1:1/hooks/retired", "whsec_retired"),
            configured("http://127.0.0.1:1/hooks/new", "whsec_new"),
        ],
    )
    .await
    .unwrap();

    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT url, secret, disabled_at IS NULL FROM webhook_endpoints ORDER BY url",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        rows,
        vec![
            (
                "http://127.0.0.1:1/hooks/new".into(),
                "whsec_new".into(),
                true
            ),
            // Rotation lands as an update to the row, not a second one.
            (
                "http://127.0.0.1:1/hooks/primary".into(),
                "whsec_rotated".into(),
                true
            ),
            // Re-listed after being dropped: revived rather than duplicated.
            (
                "http://127.0.0.1:1/hooks/retired".into(),
                "whsec_retired".into(),
                true
            ),
            // Dropped from the configuration. The row stays; only the fan-out stops.
            (
                "http://127.0.0.1:1/hooks/secondary".into(),
                "whsec_secondary".into(),
                false
            ),
        ]
    );

    // An empty configuration disables everything and deletes nothing.
    sync(&pool, &[]).await.unwrap();
    let enabled: (i64,) =
        sqlx::query_as("SELECT count(*) FROM webhook_endpoints WHERE disabled_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(enabled.0, 0);
    assert_eq!(
        sqlx::query_as::<_, (i64,)>("SELECT count(*) FROM webhook_endpoints")
            .fetch_one(&pool)
            .await
            .unwrap()
            .0,
        4
    );
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn a_disabled_endpoint_is_owed_nothing(pool: PgPool) {
    business::webhooks::sync(&pool, &[]).await.unwrap();

    let (app, _) = app(pool.clone()).await;
    create_invoice(&app).await;

    assert_eq!(events(&pool).await.len(), 1);
    assert_eq!(deliveries(&pool).await, 0);
}

// ---------------------------------------------------------------------------------------
// Reconciling
// ---------------------------------------------------------------------------------------

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn the_event_log_is_a_cursor_a_business_can_catch_up_from(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    create_invoice(&app).await;
    create_invoice(&app).await;
    create_invoice(&app).await;

    let (status, all) = read(&app, "/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(all.as_array().unwrap().len(), 3);

    // Everything after the first: what a receiver that processed one event asks for next.
    let first = all[0]["id"].as_str().unwrap();
    let (_, rest) = read(&app, &format!("/events?after={first}")).await;
    assert_eq!(rest.as_array().unwrap().len(), 2);
    assert_eq!(rest[0], all[1]);

    // Caught up: an empty page, not a 404.
    let last = all[2]["id"].as_str().unwrap();
    let (status, none) = read(&app, &format!("/events?after={last}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(none.as_array().unwrap().len(), 0);

    let (_, page) = read(&app, "/events?limit=2").await;
    assert_eq!(page.as_array().unwrap().len(), 2);

    // A type nothing has emitted is an empty page: the vocabulary is open, so an unknown type is
    // not a bad request.
    let (status, filtered) = read(&app, "/events?type=invoice.paid").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered.as_array().unwrap().len(), 0);

    assert_eq!(
        read(&app, "/events?after=not-a-uuid").await.0,
        StatusCode::BAD_REQUEST
    );
    // Refused rather than silently capped: a short page read as "that is all of them" is how a
    // reconciliation skips everything past the cap.
    assert_eq!(
        read(&app, "/events?limit=0").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read(&app, "/events?limit=5000").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn the_endpoint_listing_never_carries_a_secret(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;

    let (status, listed) = read(&app, "/webhook_endpoints").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed.as_array().unwrap().len(), 3);

    for endpoint in listed.as_array().unwrap() {
        let keys: Vec<&String> = endpoint.as_object().unwrap().keys().collect();
        assert!(!keys.iter().any(|key| key.as_str() == "secret"), "{keys:?}");
    }

    // The retired fixture endpoint says so rather than vanishing.
    let retired = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|endpoint| endpoint["id"] == RETIRED)
        .unwrap();
    assert!(retired["disabled_at"].is_string());
}

#[sqlx::test(fixtures("customers", "invoices", "webhook_endpoints"))]
async fn deliveries_can_be_narrowed_to_one_event_or_endpoint(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;
    create_invoice(&app).await;

    let (_, all) = read(&app, "/webhook_deliveries").await;
    assert_eq!(all.as_array().unwrap().len(), 2);
    assert_eq!(all[0]["status"], "pending");
    assert_eq!(all[0]["attempts"], 0);

    let event = all[0]["event_id"].as_str().unwrap();
    let (_, by_event) = read(&app, &format!("/webhook_deliveries?event_id={event}")).await;
    assert_eq!(by_event.as_array().unwrap().len(), 2);

    let endpoint = all[0]["endpoint_id"].as_str().unwrap();
    let (_, by_endpoint) = read(&app, &format!("/webhook_deliveries?endpoint_id={endpoint}")).await;
    assert_eq!(by_endpoint.as_array().unwrap().len(), 1);

    assert_eq!(
        read(&app, "/webhook_deliveries?status=nonsense").await.0,
        StatusCode::BAD_REQUEST
    );
}

// ---------------------------------------------------------------------------------------
// The stub receiver
// ---------------------------------------------------------------------------------------

/// One delivery as it arrived, headers and raw body.
#[derive(Clone, Debug)]
struct Received {
    id: String,
    timestamp: String,
    attempt: String,
    signature: String,
    body: String,
}

#[derive(Clone)]
struct Receiver {
    got: Arc<Mutex<Vec<Received>>>,
    /// How many more requests to answer `500`. Set to `usize::MAX` for a receiver that is simply
    /// down, which is what exhausting the budget needs.
    failures: Arc<Mutex<usize>>,
    /// How long to hold the request open, for the timeout path.
    delay: Duration,
}

impl Receiver {
    fn new() -> Self {
        Self {
            got: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(0)),
            delay: Duration::ZERO,
        }
    }

    fn failing(self, times: usize) -> Self {
        *self.failures.lock().unwrap() = times;
        self
    }

    fn slow(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn received(&self) -> Vec<Received> {
        self.got.lock().unwrap().clone()
    }
}

fn stub_receiver(receiver: Receiver) -> Router {
    Router::new()
        .route("/hooks", post(hook))
        .with_state(receiver)
}

async fn hook(State(receiver): State<Receiver>, headers: HeaderMap, body: String) -> Response {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };

    receiver.got.lock().unwrap().push(Received {
        id: header("webhook-id"),
        timestamp: header("webhook-timestamp"),
        attempt: header("webhook-attempt"),
        signature: header("webhook-signature"),
        body,
    });

    if !receiver.delay.is_zero() {
        tokio::time::sleep(receiver.delay).await;
    }

    // Recorded either way: a receiver that answers `500` still saw the request, and a test about
    // retrying wants to count what actually arrived.
    let mut failures = receiver.failures.lock().unwrap();
    if *failures > 0 {
        *failures = failures.saturating_sub(1);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

// ---------------------------------------------------------------------------------------
// The stub payment service
// ---------------------------------------------------------------------------------------

/// Enough of mock-payment-service's contract to settle a charge, so the tests above can reach
/// `invoice.paid` and `invoice.payment_failed` through the real endpoint. `tests/pay.rs` is
/// where the charge flow itself is covered.
fn stub_psp() -> Router {
    Router::new()
        .route("/payment_intents", post(create_intent))
        .route("/payment_intents/{id}", get(intent_status))
        .route("/payment_intents/{id}/pay", post(charge))
}

async fn create_intent() -> Response {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let id = format!("pi_stub_{:04}", NEXT.fetch_add(1, Ordering::SeqCst));
    (
        StatusCode::CREATED,
        Json(json!({ "payment_intent_id": id, "status": "pending" })),
    )
        .into_response()
}

/// Always `succeeded`, which is what the reconciler test needs it to say about the charge whose
/// answer was lost.
async fn intent_status(Path(id): Path<String>) -> Response {
    Json(json!({ "payment_intent_id": id, "status": "succeeded" })).into_response()
}

async fn charge(Path(id): Path<String>, Json(request): Json<Value>) -> Response {
    match request["card_token"].as_str().unwrap_or_default() {
        "tok_success" => {
            Json(json!({ "payment_intent_id": id, "status": "succeeded" })).into_response()
        }
        "tok_declined" => {
            Json(json!({ "payment_intent_id": id, "status": "failed" })).into_response()
        }
        // The charge lands and the response does not: the ambiguous failure the reconciler is
        // for, and the one case where the event is emitted by a background pass.
        "tok_drop" => (
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
