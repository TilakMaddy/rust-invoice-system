use axum::http::StatusCode;

/// `GET /health`
///
/// Liveness, for the Compose healthcheck. Touches nothing — there is nothing here whose failure
/// restarting would fix.
pub async fn health() -> StatusCode {
    StatusCode::OK
}
