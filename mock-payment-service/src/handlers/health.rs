use axum::Json;
use axum::response::{IntoResponse, Response};
use serde_json::json;

pub async fn health() -> Response {
    Json(json!({ "status": "ok" })).into_response()
}
