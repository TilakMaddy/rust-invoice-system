use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use super::error;
use crate::state::{AppState, PaymentView};

pub async fn get_status(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.get(&id) {
        Some(intent) => Json(PaymentView::new(id, intent)).into_response(),
        None => error(StatusCode::NOT_FOUND, "payment_intent_not_found"),
    }
}
