use axum::Json;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Liveness, not readiness: this deliberately does not touch the database. The container's
/// HEALTHCHECK gates on it, and a Postgres blip should not get the process killed and
/// restarted when restarting fixes nothing.
pub async fn health() -> Response {
    Json(json!({ "status": "ok" })).into_response()
}
