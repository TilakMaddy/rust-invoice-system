//! One module per endpoint. Anything used by a single handler lives in that handler's file;
//! only what is genuinely shared sits here.

mod create_intent;
mod get_status;
mod health;
mod pay;

pub use create_intent::create_intent;
pub use get_status::get_status;
pub use health::health;
pub use pay::pay;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// The error body every endpoint returns.
fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}
