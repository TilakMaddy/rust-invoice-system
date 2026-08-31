use axum::extract::{Path, State};
use axum::response::Response;
use sqlx::PgPool;

use super::{InvoiceState, transition};

/// `POST /invoices/{id}/void`
///
/// Writes off an invoice that will never be collected. Allowed from `draft` and `ready` — the
/// two states where nothing has been charged yet. `processing` is excluded because a charge is
/// in flight and voiding would race the PSP's answer; `processed` because the money already
/// moved, and cancelling that is a refund, not a void.
pub async fn void_invoice(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    transition(
        &pool,
        &id,
        &[InvoiceState::Draft, InvoiceState::Ready],
        InvoiceState::Void,
        "invoice_not_voidable",
    )
    .await
}
