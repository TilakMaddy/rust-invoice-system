use std::io;
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use super::error;
use crate::state::{AppState, BeginChargeError, FailureCode, IntentState, PaymentView};

#[derive(Deserialize)]
pub struct PayRequest {
    card_token: String,
}

pub async fn pay(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<PayRequest>,
) -> Response {
    if state.get(&id).is_none() {
        return error(StatusCode::NOT_FOUND, "payment_intent_not_found");
    }

    // Validated before the attempt is claimed, so a typo'd token does not burn the intent's
    // one chance at being charged.
    let Some(token) = CardToken::parse(&request.card_token) else {
        return error(StatusCode::BAD_REQUEST, "unknown_card_token");
    };

    // Phase 1: claim the single charge attempt, then release the lock.
    if let Err(err) = state.begin_charge(&id) {
        return match err {
            BeginChargeError::NotFound => error(StatusCode::NOT_FOUND, "payment_intent_not_found"),
            BeginChargeError::InProgress => error(StatusCode::CONFLICT, "payment_in_progress"),
            BeginChargeError::AlreadyPaid => {
                error(StatusCode::CONFLICT, "payment_intent_already_paid")
            }
        };
    }

    // Phase 2: the charge itself, with no lock held — status lookups stay responsive even
    // through a 30 second `tok_timeout`.
    let (delay, outcome) = match token {
        CardToken::Success => (Duration::from_millis(100), IntentState::Succeeded),
        CardToken::InsufficientFunds => (
            Duration::from_millis(100),
            IntentState::Failed(FailureCode::InsufficientFunds),
        ),
        CardToken::CardDeclined => (
            Duration::from_millis(100),
            IntentState::Failed(FailureCode::CardDeclined),
        ),
        CardToken::Timeout => (Duration::from_secs(30), IntentState::Succeeded),

        // Special case: nothing to wait for and no response to send. The charge is still
        // recorded as succeeded — the money moves, only the response is lost — which is the
        // ambiguous failure a caller has to recover from by reading status.
        CardToken::NetworkError => {
            state.settle(&id, IntentState::Succeeded);
            return drop_connection();
        }
    };

    // Phase 3: settle on a detached task, so a client that gives up mid-charge cannot cancel
    // it. Futures are dropped on disconnect, and settling inline would strand the intent in
    // `Processing` — permanently pending, and unpayable, since the claim is never released.
    // Dropping a `JoinHandle` does not abort the task, so the settlement runs either way.
    // The handler waits on it, so a client that does stay gets its result as usual.
    let settle = tokio::spawn({
        let (state, id) = (state.clone(), id.clone());
        async move {
            tokio::time::sleep(delay).await;
            state.settle(&id, outcome);
        }
    });

    match settle.await {
        Ok(()) => Json(PaymentView::new(id, outcome)).into_response(),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "charge_failed"),
    }
}

/// The test cards this mock accepts. Anything else is rejected before a charge is claimed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CardToken {
    Success,
    InsufficientFunds,
    CardDeclined,
    Timeout,
    NetworkError,
}

impl CardToken {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "tok_success" => Some(Self::Success),
            "tok_insufficient_funds" => Some(Self::InsufficientFunds),
            "tok_card_declined" => Some(Self::CardDeclined),
            "tok_timeout" => Some(Self::Timeout),
            "tok_network_error" => Some(Self::NetworkError),
            _ => None,
        }
    }
}

/// Simulates a network failure. The body stream errors on its first poll, so hyper abandons
/// the response and closes the connection before flushing anything — the client gets no
/// status line at all, just a closed socket (`curl` reports "Empty reply from server",
/// exit 52).
fn drop_connection() -> Response {
    let stream = futures_util::stream::once(async {
        Err::<Vec<u8>, io::Error>(io::Error::other("simulated network error"))
    });
    Response::new(Body::from_stream(stream))
}
