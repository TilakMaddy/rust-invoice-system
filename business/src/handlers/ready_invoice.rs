use axum::extract::{Path, State};
use axum::response::Response;
use sqlx::PgPool;

use super::{InvoiceState, transition};

/// `POST /invoices/{id}/ready`
///
/// Finalizes a draft, marking it settled enough to be charged. Only `draft` qualifies: `ready`
/// is already there, and the remaining states have all been acted on, so re-readying one would
/// either race a charge in flight or reopen an invoice that has already been paid or written
/// off.
pub async fn ready_invoice(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    transition(
        &pool,
        &id,
        &[InvoiceState::Draft],
        InvoiceState::Ready,
        "invoice_not_draft",
    )
    .await
}
