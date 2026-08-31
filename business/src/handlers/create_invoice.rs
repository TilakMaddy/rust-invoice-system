use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

use super::{Invoice, InvoiceRow, error, internal, sqlstate};
use crate::webhooks::{self, EventType};

#[derive(Deserialize)]
pub struct CreateInvoice {
    customer_id: String,
    due_date: String,
    line_items: Vec<LineItem>,
}

#[derive(Deserialize)]
struct LineItem {
    description: String,
    quantity: i64,
    unit_amount_cents: i64,
}

/// `POST /invoices`
///
/// The caller sends line items and the server totals them; there is no way to supply a total
/// directly, so the number stored can never disagree with the lines it came from.
///
/// The line items themselves are not persisted — the schema holds a total and nothing else —
/// so they are input to the sum and then discarded.
pub async fn create_invoice(
    State(pool): State<PgPool>,
    Json(request): Json<CreateInvoice>,
) -> Response {
    let total_cents = match total(&request.line_items) {
        Ok(total) => total,
        Err(code) => return error(StatusCode::BAD_REQUEST, code),
    };

    // Checked here rather than left to the `::date` cast below, which would inherit Postgres's
    // parser: with DateStyle 'ISO, MDY' it reads '01/02/2026' as 2 January — a date a European
    // caller meant as 1 February — and accepts 'today' and 'infinity' outright. Pinning the
    // shape to YYYY-MM-DD makes the cast unambiguous, leaving only the calendar to Postgres.
    if !is_iso_date(&request.due_date) {
        return error(StatusCode::BAD_REQUEST, "invalid_due_date");
    }

    // A transaction rather than the bare INSERT it used to be, because the `invoice.created`
    // webhook is queued in it: the event and the invoice commit together, so there is no moment
    // at which one exists without the other. Every early return below drops the transaction,
    // which rolls it back — an invoice that could not be announced was never raised.
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return internal(err, "opening the create-invoice transaction"),
    };

    // Bound as text and cast, so a malformed uuid comes back as a SQLSTATE to map rather than
    // needing a uuid parser here. `state` is left to its 'draft' default: an invoice is only
    // ever raised as a draft, and the column is the one place that should say so.
    let inserted: Result<InvoiceRow, _> = sqlx::query_as(
        "INSERT INTO invoices (customer_id, total_cents, due_date)
         VALUES ($1::uuid, $2, $3::date)
         RETURNING id::text, customer_id::text, total_cents, due_date::text, state",
    )
    .bind(&request.customer_id)
    .bind(total_cents)
    .bind(&request.due_date)
    .fetch_one(&mut *tx)
    .await;

    match inserted {
        Ok(row) => {
            let invoice = Invoice::from(row);
            if let Err(err) = webhooks::enqueue(
                &mut tx,
                EventType::InvoiceCreated,
                &json!({ "invoice": &invoice }),
            )
            .await
            {
                return internal(err, "queueing the invoice.created event");
            }

            match tx.commit().await {
                Ok(()) => (StatusCode::CREATED, Json(invoice)).into_response(),
                Err(err) => internal(err, "committing the new invoice"),
            }
        }
        // foreign_key_violation: the customer does not exist. Reported as a 404 about the
        // customer rather than a 400 about the body, since the body is well-formed.
        Err(err) if sqlstate(&err).as_deref() == Some("23503") => {
            error(StatusCode::NOT_FOUND, "customer_not_found")
        }
        // invalid_text_representation: customer_id is not a uuid at all.
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            error(StatusCode::BAD_REQUEST, "invalid_customer_id")
        }
        // datetime_field_overflow: the right shape but not a real day, like 2026-02-30.
        Err(err) if sqlstate(&err).as_deref() == Some("22008") => {
            error(StatusCode::BAD_REQUEST, "invalid_due_date")
        }
        Err(err) => internal(err, "inserting the invoice"),
    }
}

/// Sums the line items, or names the rule that was broken.
///
/// Every step is checked arithmetic. `total_cents` is a bigint, and a caller able to overflow
/// it into a negative would otherwise be caught by `CHECK (total_cents >= 0)` and reported as
/// an internal error — a wrong answer to what is really a bad request.
fn total(line_items: &[LineItem]) -> Result<i64, &'static str> {
    if line_items.is_empty() {
        return Err("no_line_items");
    }

    let mut total: i64 = 0;
    for item in line_items {
        // A zero unit amount is allowed — a comped line still belongs on the invoice, and the
        // column's CHECK admits a zero total. A zero quantity does not: it is a line that
        // says nothing, and is far likelier to be a bug than an intent.
        if item.description.trim().is_empty() || item.quantity <= 0 || item.unit_amount_cents < 0 {
            return Err("invalid_line_item");
        }
        total = item
            .quantity
            .checked_mul(item.unit_amount_cents)
            .and_then(|line| total.checked_add(line))
            .ok_or("total_too_large")?;
    }
    Ok(total)
}

/// Whether `raw` is exactly `YYYY-MM-DD`: ten characters, digits everywhere but the two
/// hyphens. Deliberately does not check the calendar — that is Postgres's job on the `::date`
/// cast, and duplicating leap-year rules here would only create a second answer to disagree
/// with. All this has to rule out is the *other* formats that cast would otherwise accept.
fn is_iso_date(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, byte)| i == 4 || i == 7 || byte.is_ascii_digit())
}
