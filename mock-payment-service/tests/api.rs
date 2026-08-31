use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use mock_payment_service::app;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn create(app: &Router) -> String {
    let request = Request::post("/payment_intents")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    body["payment_intent_id"].as_str().unwrap().to_owned()
}

async fn pay(app: &Router, id: &str, card_token: &str) -> (StatusCode, Value) {
    let request = Request::post(format!("/payment_intents/{id}/pay"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "card_token": card_token }).to_string()))
        .unwrap();
    send(app, request).await
}

async fn status(app: &Router, id: &str) -> (StatusCode, Value) {
    let request = Request::get(format!("/payment_intents/{id}"))
        .body(Body::empty())
        .unwrap();
    send(app, request).await
}

#[tokio::test]
async fn a_new_intent_is_pending() {
    let app = app();
    let id = create(&app).await;
    assert!(id.starts_with("pi_"), "unexpected id shape: {id}");

    let (code, body) = status(&app, &id).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["payment_intent_id"], id.as_str());
    assert_eq!(body["status"], "pending");
    assert!(body["code"].is_null());
}

#[tokio::test]
async fn tok_success_succeeds() {
    let app = app();
    let id = create(&app).await;

    let (code, body) = pay(&app, &id, "tok_success").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["status"], "succeeded");
    assert!(body["code"].is_null());

    assert_eq!(status(&app, &id).await.1["status"], "succeeded");
}

#[tokio::test]
async fn declines_report_their_code() {
    for (token, expected) in [
        ("tok_insufficient_funds", "insufficient_funds"),
        ("tok_card_declined", "card_declined"),
    ] {
        let app = app();
        let id = create(&app).await;

        let (code, body) = pay(&app, &id, token).await;
        assert_eq!(code, StatusCode::OK, "{token}");
        assert_eq!(body["status"], "failed", "{token}");
        assert_eq!(body["code"], expected, "{token}");

        let (_, body) = status(&app, &id).await;
        assert_eq!(body["status"], "failed", "{token}");
        assert_eq!(body["code"], expected, "{token}");
    }
}

#[tokio::test]
async fn a_succeeded_intent_cannot_be_paid_again() {
    let app = app();
    let id = create(&app).await;
    assert_eq!(pay(&app, &id, "tok_success").await.0, StatusCode::OK);

    // A different token on the retry: if the charge re-ran, the status would change.
    let (code, body) = pay(&app, &id, "tok_card_declined").await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["error"], "payment_intent_already_paid");
    assert_eq!(status(&app, &id).await.1["status"], "succeeded");
}

#[tokio::test]
async fn a_failed_intent_cannot_be_paid_again() {
    let app = app();
    let id = create(&app).await;
    assert_eq!(pay(&app, &id, "tok_card_declined").await.0, StatusCode::OK);

    let (code, body) = pay(&app, &id, "tok_success").await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["error"], "payment_intent_already_paid");

    let (_, body) = status(&app, &id).await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["code"], "card_declined");
}

#[tokio::test]
async fn unknown_intents_are_not_found() {
    let app = app();

    let (code, body) = status(&app, "pi_does_not_exist").await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "payment_intent_not_found");

    let (code, body) = pay(&app, "pi_does_not_exist", "tok_success").await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "payment_intent_not_found");
}

#[tokio::test]
async fn an_unknown_token_does_not_consume_the_attempt() {
    let app = app();
    let id = create(&app).await;

    let (code, body) = pay(&app, &id, "tok_nonsense").await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unknown_card_token");
    assert_eq!(status(&app, &id).await.1["status"], "pending");

    // Still payable afterwards.
    assert_eq!(pay(&app, &id, "tok_success").await.0, StatusCode::OK);
}

#[tokio::test(start_paused = true)]
async fn tok_timeout_settles_after_thirty_seconds() {
    let app = app();
    let id = create(&app).await;

    let (code, body) = pay(&app, &id, "tok_timeout").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["status"], "succeeded");
}

#[tokio::test(start_paused = true)]
async fn a_charge_in_flight_blocks_a_second_payment() {
    let app = app();
    let id = create(&app).await;

    // Both requests use the slow token, so whichever claims the attempt first is still
    // mid-charge when the other arrives. Exactly one may charge; the other must be told the
    // payment is in progress.
    let (first, second) =
        tokio::join!(pay(&app, &id, "tok_timeout"), pay(&app, &id, "tok_timeout"),);

    let (winner, loser) = if first.0 == StatusCode::OK {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(winner.0, StatusCode::OK);
    assert_eq!(winner.1["status"], "succeeded");
    assert_eq!(loser.0, StatusCode::CONFLICT);
    assert_eq!(loser.1["error"], "payment_in_progress");

    assert_eq!(status(&app, &id).await.1["status"], "succeeded");
}

#[tokio::test(start_paused = true)]
async fn settlement_survives_a_client_that_gives_up() {
    let app = app();
    let id = create(&app).await;

    // A client that waits 5s and then disconnects: its request future is dropped long before
    // the 30 second charge finishes.
    let request = Request::post(format!("/payment_intents/{id}/pay"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "card_token": "tok_timeout" }).to_string(),
        ))
        .unwrap();
    let abandoned =
        tokio::time::timeout(Duration::from_secs(5), app.clone().oneshot(request)).await;
    assert!(abandoned.is_err(), "the charge should outlast the client");

    // Nobody is waiting on it any more, but the charge still has to land — otherwise the
    // intent is stranded mid-charge and can never be paid again.
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(status(&app, &id).await.1["status"], "succeeded");
}

#[tokio::test(start_paused = true)]
async fn settlement_survives_a_client_that_gives_up_on_a_fast_charge() {
    let app = app();
    let id = create(&app).await;

    // Same hazard as the 30s charge, just a narrower window: the client disconnects 50ms in,
    // half way through a 100ms charge. A declined card, so this also shows the *outcome* is
    // carried across, not just the fact of settling.
    let request = Request::post(format!("/payment_intents/{id}/pay"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "card_token": "tok_card_declined" }).to_string(),
        ))
        .unwrap();
    let abandoned =
        tokio::time::timeout(Duration::from_millis(50), app.clone().oneshot(request)).await;
    assert!(abandoned.is_err(), "the charge should outlast the client");

    tokio::time::sleep(Duration::from_secs(1)).await;
    let (_, body) = status(&app, &id).await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["code"], "card_declined");
}
