use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// `GET /received`
///
/// Everything that has arrived, oldest first — what the demo prints to show the receiving end of
/// a delivery. A bare array, and an empty one when nothing has arrived rather than a `404`.
pub async fn received(State(state): State<AppState>) -> Response {
    Json(state.received()).into_response()
}
