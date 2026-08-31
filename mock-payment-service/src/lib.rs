mod handlers;
mod state;

use axum::Router;
use axum::routing::{get, post};

pub use state::AppState;

pub fn app() -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/payment_intents", post(handlers::create_intent))
        .route("/payment_intents/{id}", get(handlers::get_status))
        .route("/payment_intents/{id}/pay", post(handlers::pay))
        .with_state(AppState::default())
}
