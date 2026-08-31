//! One module per endpoint. Anything used by a single handler lives in that handler's file;
//! only what is genuinely shared sits here.

mod create_customer;
mod create_invoice;
mod docs;
mod draft_invoice;
mod get_customer;
mod health;
mod invoice_status;
mod list_customers;
mod list_events;
mod list_invoices;
mod list_webhook_deliveries;
mod list_webhook_endpoints;
mod pay_invoice;
mod payment_intent_status;
mod ready_invoice;
mod void_invoice;

pub use create_customer::create_customer;
pub use create_invoice::create_invoice;
pub use docs::openapi_spec;
pub use draft_invoice::draft_invoice;
pub use get_customer::get_customer;
pub use health::health;
pub use invoice_status::invoice_status;
pub use list_customers::list_customers;
pub use list_events::list_events;
pub use list_invoices::list_invoices;
pub use list_webhook_deliveries::list_webhook_deliveries;
pub use list_webhook_endpoints::list_webhook_endpoints;
pub use pay_invoice::pay_invoice;
pub use payment_intent_status::payment_intent_status;
pub use ready_invoice::ready_invoice;
pub use void_invoice::void_invoice;

use std::fmt::Display;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, error as log_error, info, instrument};

use crate::payments::{InvoiceRow, InvoiceState};
use crate::sqlstate;

/// The error body every endpoint returns. Same shape as mock-payment-service's, so a caller
/// speaking to both services parses failures one way.
fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}

/// A `500`, with the cause written down on its way out.
///
/// The response is byte for byte what it was before: the caller learns `internal_error` and nothing
/// else, which is deliberate — a database error is not a fact about their request and half of them
/// describe this service's internals. What changes is that the error stops being thrown away.
/// Nothing upstream is ever handed it, and the layer in `logging` sees only a status code and a
/// body that has already been stripped of it, so this is the one place it can be recorded at all.
///
/// One helper rather than a `tracing::error!` written out at each of the twenty-odd sites, so every
/// `500` this service answers is findable by one message and reads the same way.
pub(crate) fn internal(err: impl Display, failed: &'static str) -> Response {
    log_error!(%err, failed, "internal error");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
}

/// Every column a customer exposes, in the order the queries list them. The id is cast to text
/// in SQL for the same reason the invoice queries do it: nothing decodes a uuid in this crate.
pub(crate) type CustomerRow = (String, String, String);

/// The public shape of a customer, returned by every endpoint that creates or reads one.
#[derive(Serialize)]
struct Customer {
    id: String,
    name: String,
    email: String,
}

impl From<CustomerRow> for Customer {
    fn from((id, name, email): CustomerRow) -> Self {
        Self { id, name, email }
    }
}

/// The public shape of an invoice, returned by every endpoint that creates or moves one.
///
/// `pub(crate)` so that `webhooks` can put exactly this in an event payload. One shape rather
/// than two means what a receiver is sent, and what it reads back from `GET /events` while
/// reconciling, cannot drift from what `GET /invoices/{id}` would have told it.
#[derive(Serialize)]
pub(crate) struct Invoice {
    id: String,
    customer_id: String,
    total_cents: i64,
    due_date: String,
    state: InvoiceState,
}

impl From<InvoiceRow> for Invoice {
    fn from((id, customer_id, total_cents, due_date, state): InvoiceRow) -> Self {
        Self {
            id,
            customer_id,
            total_cents,
            due_date,
            state,
        }
    }
}

/// Moves an invoice out of one of the `from` states into `to`, under a row lock.
///
/// The row is read `FOR UPDATE`, so a concurrent transition cannot see the same state and act
/// on it too: the second caller blocks until the first commits, then reads the state that
/// actually resulted. Without the lock, two simultaneous voids of one `ready` invoice would
/// both read `ready` and both succeed — which is the check-then-act race, not a transition.
///
/// `SET LOCAL lock_timeout` bounds that wait at 100ms. The pool is five connections, so a
/// request queueing indefinitely behind someone else's open transaction would take one of
/// them out of circulation for as long as that lasts; failing fast keeps a stuck writer from
/// becoming a stuck service. LOCAL, so the setting dies with the transaction rather than
/// clinging to the pooled connection for whatever request picks it up next.
///
/// Every early return drops the transaction, which rolls it back and releases the lock.
///
/// Four endpoints reach this, so it is the one place the whole lifecycle passes through and the
/// only place worth instrumenting for it: `from` is not known until the row is read, which is to
/// say the handler that called this cannot log the transition it asked for.
#[instrument(skip_all, fields(invoice_id = %id, to = to.as_str()))]
async fn transition(
    pool: &PgPool,
    id: &str,
    from: &[InvoiceState],
    to: InvoiceState,
    wrong_state: &str,
) -> Response {
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return internal(err, "opening the transition transaction"),
    };

    if let Err(err) = sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *tx)
        .await
    {
        return internal(err, "setting the transition lock timeout");
    }

    let locked: Result<Option<(InvoiceState,)>, _> =
        sqlx::query_as("SELECT state FROM invoices WHERE id = $1::uuid FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await;

    let current = match locked {
        Ok(Some((state,))) => state,
        Ok(None) => return error(StatusCode::NOT_FOUND, "invoice_not_found"),
        Err(err) => {
            return match sqlstate(&err).as_deref() {
                // lock_not_available: someone else held the row for longer than lock_timeout.
                // A conflict with an in-flight change rather than a fault, and retryable — the
                // same reading mock-payment-service gives its `payment_in_progress`.
                Some("55P03") => error(StatusCode::CONFLICT, "invoice_locked"),
                // invalid_text_representation: the path segment is not a uuid at all.
                Some("22P02") => error(StatusCode::BAD_REQUEST, "invalid_invoice_id"),
                _ => internal(err, "locking the invoice to transition it"),
            };
        }
    };

    // Rejected rather than replayed when the invoice is already in `to`: a caller that voided
    // twice has a bug, and answering 200 both times hides it.
    if !from.contains(&current) {
        debug!(
            state = current.as_str(),
            "not transitioned: the invoice is in the wrong state"
        );
        return error(StatusCode::CONFLICT, wrong_state);
    }

    // Nothing clears `currently_processed_by_pi_id`, because no state reachable here carries
    // one: only a `processing` invoice points at an intent, and `processing` is not in either
    // endpoint's `from`.
    let updated: Result<InvoiceRow, _> = sqlx::query_as(
        "UPDATE invoices SET state = $2 WHERE id = $1::uuid
         RETURNING id::text, customer_id::text, total_cents, due_date::text, state",
    )
    .bind(id)
    .bind(to)
    .fetch_one(&mut *tx)
    .await;

    let row = match updated {
        Ok(row) => row,
        Err(err) => return internal(err, "writing the invoice's new state"),
    };

    match tx.commit().await {
        Ok(()) => {
            info!(from = current.as_str(), "invoice transitioned");
            Json(Invoice::from(row)).into_response()
        }
        Err(err) => internal(err, "committing the transition"),
    }
}
