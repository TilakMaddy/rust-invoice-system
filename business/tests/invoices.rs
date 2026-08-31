//! End-to-end tests for `GET /invoices`.
//!
//! Separate from `pay.rs`, which covers what a *charge* does to an invoice and needs a stub
//! payment service for it. Listing touches nothing but the database.
//!
//! ```sh
//! docker compose up -d postgres    # from the repo root
//! cargo test                       # from business/
//! ```

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use business::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

/// Every route in this file is an operator route, so every request carries the token. That they
/// are gated at all is `auth.rs`'s business; these tests are about what they answer.
const TOKEN: &str = "test-token";

/// The payment service is never reached by this endpoint, so the URL is a placeholder that
/// nothing connects to. `AppState` still needs one, because charging shares the same router.
fn app(pool: PgPool) -> Router {
    business::app(
        AppState::new(
            pool,
            "http://127.0.0.1:1",
            Duration::from_millis(200),
            Duration::from_millis(200),
            TOKEN,
        )
        .unwrap(),
    )
}

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::get(path)
                .header("x-api-token", TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// The last two id digits identify a fixture row, which keeps these assertions readable.
fn tails(body: &Value) -> Vec<String> {
    body.as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap()[34..].to_owned())
        .collect()
}

/// Unfiltered: everything, oldest first. Ids are uuidv7, so ordering by the primary key is
/// ordering by creation time — which the fixture's fixed ids preserve.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn every_invoice_is_listed_oldest_first(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/invoices").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(tails(&body), ["01", "02", "03", "04", "05", "06", "07"]);
    assert_eq!(body[0]["state"], "draft");
    assert_eq!(body[0]["total_cents"], 4999);
}

/// Every state the enum has, so a state added without a filter working for it fails here.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_state_filter_selects_only_that_state(pool: PgPool) {
    let app = app(pool);

    for (state, expected) in [
        ("draft", vec!["01"]),
        ("ready", vec!["02", "06"]),
        ("processing", vec!["03"]),
        ("processed", vec!["04", "07"]),
        ("void", vec!["05"]),
    ] {
        let (status, body) = get(&app, &format!("/invoices?state={state}")).await;

        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(tails(&body), expected, "{state}");
        assert!(
            body.as_array().unwrap().iter().all(|i| i["state"] == state),
            "{state}"
        );
    }
}

/// A state nothing is in is an empty array, not a 404: "none" is a successful answer.
#[sqlx::test(fixtures("customers"))]
async fn a_state_with_no_invoices_is_an_empty_array(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/invoices?state=ready").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([]));
}

/// Rejected by the enum cast in Postgres rather than by a `match` in Rust, so the labels live
/// in exactly one place — the migration.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unknown_state_is_rejected(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/invoices?state=nonsense").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_invoice_state");
}

/// An empty `?state=` is a present filter naming no state, not an absent one. Rejected rather
/// than quietly treated as "everything", which would hide a client building the URL wrong.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_empty_state_is_rejected(pool: PgPool) {
    let app = app(pool);

    assert_eq!(
        get(&app, "/invoices?state=").await.0,
        StatusCode::BAD_REQUEST
    );
}
