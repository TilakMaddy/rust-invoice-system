mod handlers;
mod state;
pub mod verify;

use axum::Router;
use axum::routing::{get, post};

pub use state::{AppState, Secret, parse};

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        // The one delivery path the demo registers. A second configured endpoint is a second
        // route with its own secret; nothing else about the service changes.
        .route("/webhooks", post(handlers::receive))
        .route("/received", get(handlers::received))
        .with_state(state)
}
