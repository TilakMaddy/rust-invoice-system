use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use sqlx::PgPool;

use super::{Invoice, error, internal};
use crate::payments::InvoiceRow;
use crate::sqlstate;

#[derive(Deserialize)]
pub struct Filter {
    state: Option<String>,
}

/// `GET /invoices`, optionally `?state=ready`
///
/// Every invoice, oldest first, or only those in one state. `id` is a uuidv7 and its leading
/// bits are a timestamp, so ordering by the primary key *is* ordering by creation time.
///
/// The state is not parsed in Rust. It is bound as text and cast to `invoice_state` by
/// Postgres, which rejects a label the enum does not have with `22P02` — the same mechanism,
/// and the same `400`, that turns a path segment that is not a uuid into `invalid_invoice_id`.
/// A `match` here would restate the enum's labels a second time and drift from the migration
/// the first time one is added.
///
/// **Unpaginated, and only for now.** Nothing caps the result, so the response grows with the
/// table. A `limit`/`offset` pair is the obvious next step; capping silently would be worse,
/// turning "every invoice" into "some invoices" with nothing in the response to say which.
pub async fn list_invoices(State(pool): State<PgPool>, Query(filter): Query<Filter>) -> Response {
    // NULL casts to NULL rather than erroring, so an absent filter matches every row and only
    // a *present* and unrecognised one reaches the enum cast that rejects it.
    let found: Result<Vec<InvoiceRow>, _> = sqlx::query_as(
        "SELECT id::text, customer_id::text, total_cents, due_date::text, state
           FROM invoices
          WHERE $1::invoice_state IS NULL OR state = $1::invoice_state
          ORDER BY id",
    )
    .bind(&filter.state)
    .fetch_all(&pool)
    .await;

    match found {
        // A bare array, and an empty one when nothing matches — not a 404. "No invoices in that
        // state" is a successful answer to "which invoices are in that state?".
        Ok(rows) => Json(
            rows.into_iter()
                .map(Invoice::from)
                .collect::<Vec<Invoice>>(),
        )
        .into_response(),
        // invalid_text_representation: `?state=` named something `invoice_state` has no label
        // for. A client error, not a fault.
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            error(StatusCode::BAD_REQUEST, "invalid_invoice_state")
        }
        Err(err) => internal(err, "listing the invoices"),
    }
}
