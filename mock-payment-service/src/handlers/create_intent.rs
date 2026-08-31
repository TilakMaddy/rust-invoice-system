use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::state::{AppState, IntentState, PaymentView};

pub async fn create_intent(State(state): State<AppState>) -> Response {
    let id = state.create_intent();
    (
        StatusCode::CREATED,
        Json(PaymentView::new(id, IntentState::Created)),
    )
        .into_response()
}
