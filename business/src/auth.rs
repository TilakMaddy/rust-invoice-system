//! `X-API-Token` on the endpoints the business itself operates.
//!
//! This service answers two different audiences over one port. Creating a customer, raising an
//! invoice, moving it through draft/ready/void, listing them — those are the business's own
//! back office, and nothing about them should be reachable by whoever happens to have the URL.
//! Paying an invoice and asking what became of that payment belong to the person being billed,
//! who has no token and cannot be given one.
//!
//! So the split is by audience, not by method: `GET /invoices` is gated and `POST
//! /invoices/{id}/pay` is not, because the question is who is asking rather than whether it
//! writes. `src/lib.rs` groups the routes accordingly, which is where the split is visible.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::state::ApiToken;

/// Rejects anything that does not carry the operator token.
///
/// A missing token and a wrong one get the same answer. Telling the second caller that their
/// header was well formed and merely incorrect hands an attacker the one bit they were missing.
pub(crate) async fn require_api_token(
    State(expected): State<ApiToken>,
    request: Request,
    next: Next,
) -> Response {
    // Header names are matched case-insensitively, so a caller may send any casing.
    let given = request
        .headers()
        .get("x-api-token")
        .map(|value| value.as_bytes());

    match given {
        Some(given) if token_matches(given, expected.as_bytes()) => next.run(request).await,
        _ => (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({ "error": "unauthorized" })),
        )
            .into_response(),
    }
}

/// Compares in time that does not depend on how much of the token was right.
///
/// `==` on bytes returns at the first difference, so how long the answer took reveals the
/// length of the matching prefix — enough, over enough requests, to recover a token one byte at
/// a time. Folding an XOR over every byte reads all of both regardless. Written out rather than
/// pulling in a crate for four lines.
///
/// The length check does short-circuit, which leaks the token's length. That is not a secret,
/// and comparing different lengths byte-for-byte would have to invent a rule for the overhang.
fn token_matches(given: &[u8], expected: &[u8]) -> bool {
    given.len() == expected.len()
        && given
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}
