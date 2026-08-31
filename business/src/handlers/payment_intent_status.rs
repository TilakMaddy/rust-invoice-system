use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use sqlx::PgPool;

use super::{error, internal};
use crate::psp::PaymentStatus;

/// `GET /payment_intents/{id}`
///
/// What this service has recorded about one charge attempt — the other half of `POST
/// /invoices/{id}/pay`, which answers with an intent id and nothing else. A caller that got
/// `504` has no idea whether the card was charged, and this is where it looks.
///
/// **The row is the answer.** `payment_intents.status` and nothing else: no call to the payment
/// service, no settlement, no side effect of any kind. That row is what the rest of this system
/// acted on — the invoice was moved on the strength of it — so anything else reported here
/// would contradict `GET /invoices/{id}` with no way to tell which was right. It also keeps the
/// payment service out of the path of an ordinary read, which would otherwise fail or hang
/// whenever the PSP did.
///
/// The cost is that `pending` means *this service does not know yet*, not *the card was not
/// charged*. A charge whose response was lost reads `pending` until the daily reconciler asks
/// the PSP what really happened and settles it. Resolving is that job's business, and doing it
/// here as well would put two things in charge of the same transition.
pub async fn payment_intent_status(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    // No `::uuid` cast and so no `22P02` case, unlike every invoice endpoint: the PSP issues
    // these ids and the column stores them as the text they are, so an id that does not exist
    // is simply not found.
    let recorded: Result<Option<(PaymentStatus,)>, _> =
        sqlx::query_as("SELECT status FROM payment_intents WHERE id = $1")
            .bind(&id)
            .fetch_optional(&pool)
            .await;

    match recorded {
        // The same two fields the PSP's own status endpoint answers with, in the same
        // vocabulary — `PaymentStatus` is one type for the column, the wire and this response.
        Ok(Some((status,))) => {
            Json(json!({ "payment_intent_id": id, "status": status })).into_response()
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "payment_intent_not_found"),
        Err(err) => internal(err, "reading the payment intent"),
    }
}
