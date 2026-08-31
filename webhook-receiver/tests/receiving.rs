//! Tests for the verification recipe, which is the only part of this service worth being sure
//! about: a receiver that accepts a forged webhook is worse than one that accepts none.
//!
//! The signatures here are built by hand rather than by calling `verify`'s own helpers, so a
//! change that broke what the sender produces would fail these rather than move with them.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::Sha256;
use tower::ServiceExt;
use webhook_receiver::{AppState, Secret, parse, verify};

const SECRET: &str = "whsec_test";
const PATH: &str = "/webhooks";

fn app() -> Router {
    webhook_receiver::app(AppState::new(
        parse(&json!([{ "path": PATH, "secret": SECRET }]).to_string()).unwrap(),
    ))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Exactly what the sender does: `HMAC-SHA256(secret, "{id}.{timestamp}.{body}")`, hex.
fn sign(secret: &str, id: &str, timestamp: i64, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{id}.{timestamp}.{body}").as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn body_for(kind: &str) -> String {
    json!({ "id": "evt", "type": kind, "created_at": "2026-08-28T00:00:00Z", "data": {} })
        .to_string()
}

/// A delivery as the business sends it, with each header overridable so a test can break one.
struct Delivery {
    id: String,
    timestamp: i64,
    signature: Option<String>,
    body: String,
}

impl Delivery {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            timestamp: now(),
            signature: None,
            body: body_for("invoice.paid"),
        }
    }

    fn request(&self) -> Request<Body> {
        let signature = self.signature.clone().unwrap_or_else(|| {
            format!("v1={}", sign(SECRET, &self.id, self.timestamp, &self.body))
        });
        Request::post(PATH)
            .header("content-type", "application/json")
            .header("webhook-id", &self.id)
            .header("webhook-timestamp", self.timestamp.to_string())
            .header("webhook-attempt", "1")
            .header("webhook-signature", signature)
            .body(Body::from(self.body.clone()))
            .unwrap()
    }
}

async fn send(app: &Router, request: Request<Body>) -> StatusCode {
    app.clone().oneshot(request).await.unwrap().status()
}

async fn received(app: &Router) -> Value {
    let response = app
        .clone()
        .oneshot(Request::get("/received").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn a_genuine_delivery_verifies() {
    let app = app();
    assert_eq!(
        send(&app, Delivery::new("evt_1").request()).await,
        StatusCode::OK
    );

    let log = received(&app).await;
    assert_eq!(log[0]["verified"], true);
    assert_eq!(log[0]["rejected"], Value::Null);
    assert_eq!(log[0]["webhook_id"], "evt_1");
    assert_eq!(log[0]["type"], "invoice.paid");
    assert_eq!(log[0]["attempt"], 1);
    assert_eq!(log[0]["duplicate"], false);
}

#[tokio::test]
async fn a_body_changed_in_flight_does_not_verify() {
    let app = app();
    let mut delivery = Delivery::new("evt_1");
    // Signed over the real body, delivered with another: the tampering the signature exists to
    // catch, and the reason the raw bytes are what gets hashed.
    delivery.signature = Some(format!(
        "v1={}",
        sign(SECRET, &delivery.id, delivery.timestamp, &delivery.body)
    ));
    delivery.body = body_for("invoice.payment_failed");

    send(&app, delivery.request()).await;
    assert_eq!(received(&app).await[0]["rejected"], "signature_mismatch");
}

#[tokio::test]
async fn a_delivery_signed_with_another_key_does_not_verify() {
    let app = app();
    let mut delivery = Delivery::new("evt_1");
    delivery.signature = Some(format!(
        "v1={}",
        sign(
            "whsec_someone_else",
            &delivery.id,
            delivery.timestamp,
            &delivery.body
        )
    ));

    send(&app, delivery.request()).await;
    assert_eq!(received(&app).await[0]["rejected"], "signature_mismatch");
}

/// A capture stays valid forever without this, so a recording of one request is a forgery.
#[tokio::test]
async fn a_delivery_outside_the_tolerance_window_does_not_verify() {
    let app = app();

    let mut stale = Delivery::new("evt_old");
    stale.timestamp = now() - 3600;
    send(&app, stale.request()).await;

    // The future too: a sender whose clock runs fast would otherwise be trusted indefinitely.
    let mut ahead = Delivery::new("evt_new");
    ahead.timestamp = now() + 3600;
    send(&app, ahead.request()).await;

    let log = received(&app).await;
    assert_eq!(log[0]["rejected"], "timestamp_outside_tolerance");
    assert_eq!(log[1]["rejected"], "timestamp_outside_tolerance");
}

/// The id is inside the signed string, so relabelling a captured delivery to slip past the
/// receiver's deduplication breaks the signature instead.
#[tokio::test]
async fn a_relabelled_delivery_does_not_verify() {
    let app = app();
    let genuine = Delivery::new("evt_1");
    let signature = format!(
        "v1={}",
        sign(SECRET, &genuine.id, genuine.timestamp, &genuine.body)
    );

    let mut replayed = Delivery::new("evt_2");
    replayed.timestamp = genuine.timestamp;
    replayed.body = genuine.body.clone();
    replayed.signature = Some(signature);

    send(&app, replayed.request()).await;
    assert_eq!(received(&app).await[0]["rejected"], "signature_mismatch");
}

#[tokio::test]
async fn a_malformed_or_missing_signature_does_not_verify() {
    let app = app();

    let mut garbled = Delivery::new("evt_1");
    garbled.signature = Some("nonsense".into());
    send(&app, garbled.request()).await;

    // A version the receiver does not know, alongside none that it does.
    let mut future_scheme = Delivery::new("evt_2");
    future_scheme.signature = Some("v9=abcdef".into());
    send(&app, future_scheme.request()).await;

    let log = received(&app).await;
    assert_eq!(log[0]["rejected"], "malformed_signature");
    assert_eq!(log[1]["rejected"], "malformed_signature");
}

/// A version the receiver knows, among ones it does not: taken rather than refused, so the
/// scheme has room to add a second algorithm without breaking every receiver at once.
#[tokio::test]
async fn an_unknown_signature_version_alongside_a_known_one_verifies() {
    let app = app();
    let mut delivery = Delivery::new("evt_1");
    delivery.signature = Some(format!(
        "v9=notthisone,v1={}",
        sign(SECRET, &delivery.id, delivery.timestamp, &delivery.body)
    ));

    send(&app, delivery.request()).await;
    assert_eq!(received(&app).await[0]["verified"], true);
}

#[tokio::test]
async fn a_path_with_no_configured_secret_verifies_nothing() {
    let app = webhook_receiver::app(AppState::new(Vec::<Secret>::new()));
    send(&app, Delivery::new("evt_1").request()).await;
    assert_eq!(received(&app).await[0]["rejected"], "no_secret_configured");
}

/// Delivery is at-least-once, so recognising the second copy is the receiver's half of the deal.
#[tokio::test]
async fn a_redelivered_event_is_flagged_as_a_duplicate() {
    let app = app();
    send(&app, Delivery::new("evt_1").request()).await;
    send(&app, Delivery::new("evt_1").request()).await;

    let log = received(&app).await;
    assert_eq!(log[0]["duplicate"], false);
    assert_eq!(log[1]["duplicate"], true);
    assert_eq!(log[1]["verified"], true);
}

#[test]
fn configuration_that_could_not_verify_anything_is_refused() {
    assert!(parse("[]").unwrap().is_empty());
    assert!(parse("not json").is_err());
    assert!(parse(r#"[{"path":"webhooks","secret":"s"}]"#).is_err());
    assert!(parse(r#"[{"path":"/webhooks","secret":""}]"#).is_err());
    assert!(parse(r#"[{"path":"/webhooks","secrets":"s"}]"#).is_err());
}

#[test]
fn every_rejection_has_a_name() {
    use verify::Rejected;
    for rejected in [
        Rejected::NoSecretConfigured,
        Rejected::MalformedSignature,
        Rejected::TimestampOutsideTolerance,
        Rejected::SignatureMismatch,
    ] {
        assert!(!rejected.as_str().is_empty());
    }
}
