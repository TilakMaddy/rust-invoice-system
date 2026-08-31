use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use crate::state::{AppState, Received};
use crate::verify;

/// `POST /webhooks`
///
/// Verifies the signature, records what arrived, and answers `200`.
///
/// **An unverifiable delivery is recorded too, and still answered `200`.** That is a harness
/// decision and not a pattern to copy: this service exists to show what happened on the wire, and
/// swallowing a rejected delivery would make a signing bug look identical to one that never
/// arrived. A real receiver answers `401` and processes nothing — a body it cannot authenticate
/// is a body from anyone, and the `200` would tell the sender its webhook was handled.
pub async fn receive(
    State(state): State<AppState>,
    uri: OriginalUri,
    headers: HeaderMap,
    body: String,
) -> Response {
    record(&state, &uri, &headers, body);
    StatusCode::OK.into_response()
}

/// Runs the verification recipe and files the result.
fn record(state: &AppState, uri: &OriginalUri, headers: &HeaderMap, body: String) {
    let webhook_id = header(headers, "webhook-id").unwrap_or_default();

    // The secret is looked up by the path the delivery arrived on, because the sender signs each
    // configured endpoint with its own key — checking against the wrong one would fail every
    // delivery for a reason that looks exactly like a forgery.
    let rejected = verify::check(
        state.secret(uri.path()),
        webhook_id,
        header(headers, "webhook-timestamp"),
        header(headers, "webhook-signature"),
        &body,
    );

    // Parsed only after verification, and only for the log. A body that could not be
    // authenticated has no business being interpreted, and a real receiver would stop here.
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);

    let event_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let attempt: u32 = header(headers, "webhook-attempt")
        .and_then(|attempt| attempt.parse().ok())
        .unwrap_or_default();

    // The two ids the rest of the stack is traced by: the invoice the event is about, and the
    // charge behind it. An id in the log is what lets a delivery here be lined up against
    // `GET /invoices/{id}` and `GET /payment_intents/{id}` on the business — without them the
    // line says an invoice event arrived and leaves finding out which one to `GET /received`.
    let invoice_id = parsed
        .pointer("/data/invoice/id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    // Only `invoice.paid` and `invoice.payment_failed` carry one, so it is left off the line
    // rather than dashed out: absent here means "this kind of event has no charge behind it",
    // which is not the same fault a missing header is.
    let payment_intent_id = parsed
        .pointer("/data/payment_intent_id")
        .and_then(Value::as_str)
        .map(|intent| format!(" payment_intent={intent}"))
        .unwrap_or_default();

    let duplicate = state.record(
        uri.path(),
        Received {
            webhook_id: webhook_id.to_owned(),
            r#type: event_type.clone(),
            attempt,
            verified: rejected.is_none(),
            // Filled in by `record`, which owns the set that decides it.
            duplicate: false,
            rejected: rejected.map(verify::Rejected::as_str),
            body: parsed,
        },
    );

    // One line per delivery, on stdout, whatever the verdict.
    //
    // Without it a receiver that is refusing every delivery looks exactly like one nothing is
    // being sent to, and both look like a sender that is not sending — three very different
    // faults with one symptom. `GET /received` answers the same question, but only for someone
    // who already suspects there is a question to ask.
    println!(
        "received {} id={} type={} attempt={} invoice={}{} {}{}",
        uri.path(),
        blank_as_dash(webhook_id),
        blank_as_dash(&event_type),
        attempt,
        blank_as_dash(&invoice_id),
        payment_intent_id,
        match rejected {
            None => "verified".to_owned(),
            Some(reason) => format!("REJECTED {}", reason.as_str()),
        },
        if duplicate { " duplicate" } else { "" },
    );
}

/// `-` rather than an empty field, so every line has the same shape and a missing header is
/// visibly missing instead of running into the next value.
fn blank_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn header<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}
