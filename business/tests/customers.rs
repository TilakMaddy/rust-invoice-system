//! End-to-end tests for the customer endpoints.
//!
//! Separate from `pay.rs`, which is about charging: nothing here needs a payment service, and
//! the app under test is built from a bare pool rather than that file's stub-wired harness.
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

const ADA: &str = "00000000-0000-4000-8000-000000000001";

/// Every route in this file is an operator route, so every request carries the token. That they
/// are gated at all is `auth.rs`'s business; these tests are about what they answer.
const TOKEN: &str = "test-token";

/// The payment service is never reached by these endpoints, so the URL is a placeholder that
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

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let request = Request::get(path)
        .header("x-api-token", TOKEN)
        .body(Body::empty())
        .unwrap();
    send(app, request).await
}

#[sqlx::test(fixtures("customers"))]
async fn a_customer_is_read_back_whole(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, &format!("/customers/{ADA}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "id": ADA, "name": "Ada Lovelace", "email": "ada@example.com" })
    );
}

/// Oldest first. Ids are uuidv7, whose leading bits are a timestamp, so ordering by the primary
/// key is ordering by creation time — which the fixture's fixed ids preserve.
#[sqlx::test(fixtures("customers"))]
async fn customers_are_listed_oldest_first(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/customers").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_array()
            .unwrap()
            .iter()
            .map(|c| &c["name"])
            .collect::<Vec<_>>(),
        ["Ada Lovelace", "Grace Hopper", "Alan Turing"]
    );
    assert_eq!(body[0]["email"], "ada@example.com");
}

/// A newly created customer appears in the list, at the end, without anything else changing.
#[sqlx::test(fixtures("customers"))]
async fn a_created_customer_joins_the_list(pool: PgPool) {
    let app = app(pool);

    let created = send(
        &app,
        Request::post("/customers")
            .header("content-type", "application/json")
            .header("x-api-token", TOKEN)
            .body(Body::from(
                json!({ "name": "Katherine Johnson", "email": "katherine@example.com" })
                    .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);

    let (_, body) = get(&app, "/customers").await;
    let listed = body.as_array().unwrap();

    assert_eq!(listed.len(), 4);
    assert_eq!(listed[3], created.1);

    // And readable on its own, by the id it was issued.
    let id = created.1["id"].as_str().unwrap();
    assert_eq!(get(&app, &format!("/customers/{id}")).await.1, created.1);
}

/// "No customers" is a successful answer to "which customers?", not a 404 and not an error.
#[sqlx::test]
async fn an_empty_list_is_an_empty_array(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/customers").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([]));
}

#[sqlx::test(fixtures("customers"))]
async fn reading_an_unknown_or_malformed_customer_is_rejected(pool: PgPool) {
    let app = app(pool);

    let (status, body) = get(&app, "/customers/00000000-0000-4000-8000-000000000999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "customer_not_found");

    let (status, body) = get(&app, "/customers/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_customer_id");
}
