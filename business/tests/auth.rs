//! Which endpoints `X-API-Token` gates, and which it must not.
//!
//! The split is by audience rather than by method: the business operates one set, the person
//! being billed reaches the other. A test per route rather than a test per handler, because
//! what is being checked is where the layer sits, not what any handler does with the request
//! once it is through.
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

const TOKEN: &str = "test-token";
const READY: &str = "00000000-0000-4000-8000-000000000102";

/// The payment service is never reached: every request here is either turned away before a
/// handler runs, or is a read that talks only to the database.
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

/// Every route the business itself operates, as `(method, path)`. A route added to that group
/// without the layer, or added to the router and forgotten here, fails the sweeps below.
fn operator_routes() -> Vec<(&'static str, String)> {
    vec![
        ("GET", "/customers".into()),
        ("POST", "/customers".into()),
        ("GET", format!("/customers/{READY}")),
        ("GET", "/invoices".into()),
        ("POST", "/invoices".into()),
        ("GET", format!("/invoices/{READY}")),
        ("POST", format!("/invoices/{READY}/draft")),
        ("POST", format!("/invoices/{READY}/ready")),
        ("POST", format!("/invoices/{READY}/void")),
        ("GET", "/webhook_endpoints".into()),
        ("GET", "/webhook_deliveries".into()),
        ("GET", "/events".into()),
    ]
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

/// A request with whatever token is given, or none. The body is valid JSON for the `POST`s so
/// that a rejection is the layer's doing and not a deserialization failure behind it.
async fn call(app: &Router, method: &str, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("x-api-token", token);
    }
    send(
        app,
        request.body(Body::from(json!({}).to_string())).unwrap(),
    )
    .await
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_operator_route_needs_a_token(pool: PgPool) {
    let app = app(pool);

    for (method, path) in operator_routes() {
        let (status, body) = call(&app, method, &path, None).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(body["error"], "unauthorized", "{method} {path}");
    }
}

/// A wrong token is answered exactly as a missing one. Telling this caller that their header
/// was well formed and merely incorrect hands them the one bit they were missing.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_wrong_token_is_rejected_the_same_way(pool: PgPool) {
    let app = app(pool);

    for (method, path) in operator_routes() {
        // Including one that is right up to the last byte, and one of the wrong length.
        for wrong in ["test-toke", "test-tokem", "", "test-token-longer"] {
            let (status, body) = call(&app, method, &path, Some(wrong)).await;

            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {path} {wrong:?}"
            );
            assert_eq!(body["error"], "unauthorized", "{method} {path} {wrong:?}");
        }
    }
}

/// The right token gets past the layer. What each route then answers is its own business — the
/// only thing asserted is that none of them is still `401`.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn the_right_token_is_let_through(pool: PgPool) {
    let app = app(pool);

    for (method, path) in operator_routes() {
        let (status, _) = call(&app, method, &path, Some(TOKEN)).await;

        assert_ne!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
    }
}

/// Header names are case-insensitive, so a caller may send any casing.
#[sqlx::test(fixtures("customers"))]
async fn the_header_name_is_not_case_sensitive(pool: PgPool) {
    let app = app(pool);

    for name in ["x-api-token", "X-API-Token", "X-Api-Token"] {
        let request = Request::get("/customers")
            .header(name, TOKEN)
            .body(Body::empty())
            .unwrap();

        assert_eq!(send(&app, request).await.0, StatusCode::OK, "{name}");
    }
}

/// The payer has no token and cannot be given one, so their two endpoints are open. Answering
/// what the route actually says — a `404` for an intent that does not exist — rather than
/// `401`, which is what a layer reaching too far would produce.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn the_payers_routes_are_open(pool: PgPool) {
    let app = app(pool);

    let (status, body) = call(&app, "GET", "/payment_intents/pi_no_such_thing", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "payment_intent_not_found");

    // Reached the handler and was turned away by *its* rule, the idempotency key, not by auth.
    let (status, body) = call(&app, "POST", &format!("/invoices/{READY}/pay"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "idempotency_key_missing");
}

/// `/health` has to stay open: the container's HEALTHCHECK runs the binary with `--health`,
/// which writes a bare `GET /health` over a socket and carries no headers at all. Gating it
/// makes every container permanently unhealthy.
#[sqlx::test]
async fn health_is_open(pool: PgPool) {
    let app = app(pool);

    let request = Request::get("/health").body(Body::empty()).unwrap();

    assert_eq!(send(&app, request).await.0, StatusCode::OK);
}

/// An unmatched path is a `404`, not a `401`. `route_layer` is what keeps it that way, and it
/// matters: answering `401` would tell an unauthenticated caller which paths exist.
#[sqlx::test]
async fn an_unknown_path_is_not_found_rather_than_unauthorized(pool: PgPool) {
    let app = app(pool);

    let request = Request::get("/nope").body(Body::empty()).unwrap();

    assert_eq!(send(&app, request).await.0, StatusCode::NOT_FOUND);
}
