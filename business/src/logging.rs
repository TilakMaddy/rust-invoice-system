//! Structured logs: the subscriber, and the layer that records every request and response.
//!
//! This is one half of the crate's telemetry. It logs the HTTP *envelope* — method, matched route,
//! status, latency, headers, bodies — and knows nothing about invoices. What changed as a result of
//! a request is logged by the code that changed it, next to the change, because that is the only
//! place that knows: `payments` for the money state machine, `psp` for what the payment service
//! answered, `handlers::transition` for the lifecycle moves, `webhooks` for the outbox. Those events
//! nest inside the span this layer opens, so one request reads top to bottom.
//!
//! # Capturing a body without breaking one
//!
//! Bodies are streams, and logging one means holding it in memory. The rule is the same for the
//! request and the response: **buffer only a body that is already in memory, small, and JSON.**
//! Everything else is passed through untouched and logged as its size and content type. That is
//! what keeps this layer from being a change in behaviour — it introduces no new failure mode, adds
//! no size limit a route did not already have, and never buffers the Swagger UI asset bundles that
//! `crate::app` serves.
//!
//! # Redaction
//!
//! Two rules, each one rule rather than a list of places to remember:
//!
//! * **A header's value is logged only if its name is on [`SAFE_HEADERS`].** Everything else is
//!   `<redacted>`. Names are always logged, so `x-api-token` still shows up as having been sent —
//!   which is the part worth knowing — without its value. A header added to this service later is
//!   redacted by default rather than leaked by default.
//! * **A JSON body has every value whose key is in [`REDACTED_KEYS`] replaced, at any depth.** The
//!   card token is why: `handlers::pay_invoice::PayInvoice` has no `Debug` impl today, which is
//!   protection by accident rather than by decision, and this is the decision.
//!
//! What is *not* redacted, and is a deliberate choice rather than an oversight: customer names and
//! email addresses, invoice totals, and the invoice objects inside webhook payloads. Logging bodies
//! means logging those, and for a stack whose data is fictional that is the right trade. A
//! deployment holding real customer data adds their keys to [`REDACTED_KEYS`], which is why that is
//! one array and not a condition spread across the handlers.
//!
//! The same rules bind the instrumentation on the other side of the crate, where they take the form
//! of `skip_all` on every `#[tracing::instrument]`: the attribute records every argument through
//! `Debug` unless told not to, which on `psp::pay` is the card token and on a claimed delivery row
//! is the webhook signing key. `skip_all` makes naming a field the deliberate act.

use std::io::IsTerminal;
use std::time::Instant;

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{MatchedPath, Request};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::Value;
use tracing::{Instrument, info, info_span};
use tracing_subscriber::EnvFilter;

/// What replaces anything the two rules above cover.
const REDACTED: &str = "<redacted>";

/// The largest body this will hold in memory to log. Nothing this API answers with comes close
/// except a wide page of `GET /events`, which is logged as its size instead.
const MAX_LOGGED_BODY: u64 = 8 * 1024;

/// The header names whose values are safe to log. Everything else is [`REDACTED`] — including
/// `x-api-token`, which `auth` compares, and `idempotency-key`, which is a caller-chosen string
/// standing for one charge.
const SAFE_HEADERS: [&str; 5] = [
    "accept",
    "content-length",
    "content-type",
    "host",
    "user-agent",
];

/// The JSON keys whose values are [`REDACTED`] wherever they appear in a body.
///
/// `card_token` is the one that matters: it is the payer's instrument, it arrives in the body of
/// `POST /invoices/{id}/pay`, and it goes on to the payment service. `secret` is defensive — no
/// endpoint returns a webhook signing key, and `handlers::list_webhook_endpoints` explains why the
/// field is absent rather than blanked — but `webhooks::Endpoint` is `Serialize` with a public
/// `secret`, so the rule should already hold if one ever reaches a body.
const REDACTED_KEYS: [&str; 2] = ["card_token", "secret"];

/// Installs the subscriber. Called once, from `main`.
///
/// The default filter is `warn,business=debug`: everything verbose here and quiet everywhere else.
/// Quiet everywhere else is not tidiness. `tracing-subscriber` bridges the `log` crate, so `sqlx`
/// and `reqwest` are on this subscriber whether or not they are wanted, and `sqlx=debug` prints
/// every statement this service runs. `warn` still lets sqlx's slow-query warnings through, which
/// are worth having and carry `$1` placeholders rather than the values bound to them.
///
/// Not called from `crate::app`, so the integration tests — which build the router directly and
/// drive it with `oneshot` — get this layer's behaviour with none of its output. A `tracing` event
/// with no subscriber installed is a no-op, so there is nothing there to initialise twice.
pub fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,business=debug")),
        )
        // Colour only for a human at a terminal. Left on, the escape codes end up in
        // `docker compose logs` and in whatever collects them.
        .with_ansi(std::io::stdout().is_terminal())
        .init();
}

/// Opens a span for the request and logs both ends of it.
///
/// Applied with `layer` rather than `route_layer`, which is the opposite of what `auth` needs and
/// for the opposite reason: `auth` must not run on a path that matched nothing, and this must. A
/// `404` is a thing worth knowing about.
///
/// The route is read from `MatchedPath` — the pattern, `/invoices/{id}/pay`, not the expanded uuid
/// — so a span name is one of a fixed set rather than one per invoice. It is absent for a request
/// that matched no route, which is exactly the `404` case, and there the raw path is what there is.
pub(crate) async fn trace_requests(request: Request, next: Next) -> Response {
    let path = match request.extensions().get::<MatchedPath>() {
        Some(matched) => matched.as_str().to_owned(),
        None => request.uri().path().to_owned(),
    };
    let span = info_span!("request", method = %request.method(), path = %path);

    async move {
        let started = Instant::now();

        let (parts, body) = request.into_parts();
        let headers = describe(&parts.headers);
        let (body, captured) = capture(body, &parts.headers).await;
        info!(headers, body = %captured, "started");

        let response = next.run(Request::from_parts(parts, body)).await;

        let (parts, body) = response.into_parts();
        let headers = describe(&parts.headers);
        let (body, captured) = capture(body, &parts.headers).await;
        info!(
            status = parts.status.as_u16(),
            latency = ?started.elapsed(),
            headers,
            body = %captured,
            "finished"
        );

        Response::from_parts(parts, body)
    }
    .instrument(span)
    .await
}

/// Every header name, and the values the safelist allows.
fn describe(headers: &HeaderMap) -> String {
    let mut rendered = String::from("{");

    for (name, value) in headers {
        if rendered.len() > 1 {
            rendered.push_str(", ");
        }
        rendered.push_str(name.as_str());
        rendered.push_str(": ");

        // A header name off the wire is already lowercase, so this compares like with like.
        match SAFE_HEADERS.contains(&name.as_str()) {
            true => rendered.push_str(value.to_str().unwrap_or("<not utf-8>")),
            false => rendered.push_str(REDACTED),
        }
    }

    rendered.push('}');
    rendered
}

/// The body to hand onwards, and what to log for it.
///
/// The three conditions are checked before anything is read, so a body that fails any of them is
/// returned exactly as it arrived. `size_hint().exact()` is what makes that possible: it is `Some`
/// only when the length is already known — a `content-length` on the way in, a buffer on the way
/// out — and `None` for anything still streaming, which is the case that must not be touched.
async fn capture(body: Body, headers: &HeaderMap) -> (Body, String) {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    let Some(size) = body.size_hint().exact() else {
        return (body, String::from("<streaming>"));
    };
    if size == 0 {
        return (body, String::from("<empty>"));
    }
    // `starts_with` rather than `==`, so `application/json; charset=utf-8` counts.
    if size > MAX_LOGGED_BODY || !content_type.starts_with("application/json") {
        return (body, format!("<{size} bytes, {}>", label(content_type)));
    }

    // Cannot exceed the limit: `exact()` just said the whole body is smaller than it. What is left
    // is a read that fails partway, which means the far end went away mid-body — the request or
    // response is already lost at that point, and an empty body is the honest thing to pass on.
    let Ok(bytes) = axum::body::to_bytes(body, MAX_LOGGED_BODY as usize).await else {
        return (Body::empty(), String::from("<unreadable>"));
    };

    let rendered = redact(&bytes);
    (Body::from(bytes), rendered)
}

/// A JSON body with the sensitive keys blanked, or a description of one that is not JSON.
///
/// Re-serialising rather than logging the bytes is what makes the redaction total: a token can only
/// survive this if it is a value under some key, and every value under a redacted key is replaced
/// before anything is printed. A body that will not parse is not logged at all — an unparseable
/// body declaring itself JSON is the one case where what is in there cannot be known.
fn redact(body: &Bytes) -> String {
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return format!("<{} bytes, malformed json>", body.len());
    };

    scrub(&mut value);
    value.to_string()
}

/// Replaces every value under a redacted key, at any depth.
fn scrub(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (key, field) in fields.iter_mut() {
                match REDACTED_KEYS.contains(&key.as_str()) {
                    true => *field = Value::from(REDACTED),
                    false => scrub(field),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(scrub),
        _ => {}
    }
}

/// What to call a body that was not logged. A missing content type is worth saying out loud rather
/// than rendering as an empty pair of brackets.
fn label(content_type: &str) -> &str {
    match content_type.is_empty() {
        true => "no content-type",
        false => content_type,
    }
}
