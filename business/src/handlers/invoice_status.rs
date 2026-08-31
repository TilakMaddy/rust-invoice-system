use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;

use super::{Invoice, error, internal};
use crate::payments::InvoiceRow;
use crate::sqlstate;

/// `GET /invoices/{id}`
///
/// The invoice as it stands. The same shape every endpoint that creates or moves one answers
/// with, so a caller has one invoice representation to parse rather than two.
///
/// This is where a caller reads the outcome of a charge on the invoice itself: `POST
/// /invoices/{id}/pay` answers with an intent id, which says what happened to the *attempt*,
/// and `state` here says what happened to the *invoice*. They can differ, and the gap is not a
/// bug — a charge whose response was lost leaves the intent `pending` and the invoice
/// `processing` until the daily reconciler resolves it, which is the only thing that does.
///
/// Reads, and only reads. Nothing is locked, no transition is attempted and the payment service
/// is not called: `GET /payment_intents/{id}` is the endpoint that resolves things, and folding
/// that into this one would make an ordinary read of an invoice depend on the PSP being up.
pub async fn invoice_status(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    // `22P02` is what turns a path segment that is not a uuid into a 400 without a uuid parser
    // in this crate, exactly as the other invoice endpoints do it.
    let found: Result<Option<InvoiceRow>, _> = sqlx::query_as(
        "SELECT id::text, customer_id::text, total_cents, due_date::text, state
           FROM invoices
          WHERE id = $1::uuid",
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await;

    match found {
        Ok(Some(row)) => Json(Invoice::from(row)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "invoice_not_found"),
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            error(StatusCode::BAD_REQUEST, "invalid_invoice_id")
        }
        Err(err) => internal(err, "reading the invoice"),
    }
}
