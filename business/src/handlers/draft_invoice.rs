use axum::extract::{Path, State};
use axum::response::Response;
use sqlx::PgPool;

use super::{InvoiceState, transition};

/// `POST /invoices/{id}/draft`
///
/// Takes a finalized invoice back to `draft`, so its details can be corrected before it is
/// issued again. Only `ready` qualifies: a `draft` invoice is already there, and `processing`,
/// `processed` and `void` have all been acted on — reopening one for editing would let the
/// figures drift away from what was charged or written off.
pub async fn draft_invoice(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    transition(
        &pool,
        &id,
        &[InvoiceState::Ready],
        InvoiceState::Draft,
        "invoice_not_ready",
    )
    .await
}
