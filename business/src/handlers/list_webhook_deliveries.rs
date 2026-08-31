use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::{error, internal};
use crate::sqlstate;

#[derive(Deserialize)]
pub struct Filter {
    status: Option<String>,
    event_id: Option<String>,
    endpoint_id: Option<String>,
}

/// Every column a delivery exposes, in the order the query lists them.
type DeliveryRow = (
    String,
    String,
    String,
    String,
    String,
    i32,
    String,
    Option<i16>,
    Option<String>,
    Option<String>,
);

/// The public shape of one delivery attempt record.
#[derive(Serialize)]
struct Delivery {
    id: String,
    event_id: String,
    endpoint_id: String,
    /// Denormalised from the event so that scanning this list does not require joining it back
    /// against `GET /events` to find out what was being delivered.
    event_type: String,
    /// `pending`, `succeeded` or `exhausted`. Nothing leaves `exhausted`.
    status: String,
    attempts: i32,
    /// When the next attempt is due. Still set on a finished delivery, where it is simply the
    /// last lease that was taken — the status is what says whether anything will act on it.
    next_attempt_at: String,
    /// The last response's status, or `null` when nothing answered. Exactly one of this and
    /// `last_error` is ever set, which is the difference between a receiver that returned 500
    /// and a receiver that could not be reached.
    last_status: Option<i16>,
    last_error: Option<String>,
    delivered_at: Option<String>,
}

impl From<DeliveryRow> for Delivery {
    fn from(row: DeliveryRow) -> Self {
        let (
            id,
            event_id,
            endpoint_id,
            event_type,
            status,
            attempts,
            next_attempt_at,
            last_status,
            last_error,
            delivered_at,
        ) = row;
        Self {
            id,
            event_id,
            endpoint_id,
            event_type,
            status,
            attempts,
            next_attempt_at,
            last_status,
            last_error,
            delivered_at,
        }
    }
}

/// `GET /webhook_deliveries`, optionally `?status=`, `?event_id=`, `?endpoint_id=`
///
/// What has been sent, what is still owed, and what was given up on — oldest first.
///
/// `?status=exhausted` is the one worth watching. A delivery reaches it only after the whole
/// retry budget is spent, which means an event a receiver was owed and never got; the dispatcher
/// also says so on stderr at the moment it happens, but this is where the backlog of them lives.
///
/// Filters are bound as text and cast by Postgres, the same mechanism `GET /invoices?state=`
/// uses. Since three of them can fail that cast, one code covers all three: the alternative is
/// pre-parsing each in Rust to tell them apart, which restates the enum's labels and teaches
/// this crate to parse uuids, for a marginally better error on a malformed query string.
pub async fn list_webhook_deliveries(
    State(pool): State<PgPool>,
    Query(filter): Query<Filter>,
) -> Response {
    // NULL casts to NULL rather than erroring, so an absent filter matches every row and only a
    // *present* and unusable one reaches the cast that rejects it.
    let found: Result<Vec<DeliveryRow>, _> = sqlx::query_as(
        "SELECT delivery.id::text, delivery.event_id::text, delivery.endpoint_id::text,
                event.type, delivery.status::text, delivery.attempts,
                to_char(delivery.next_attempt_at AT TIME ZONE 'utc',
                        'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                delivery.last_status, delivery.last_error,
                to_char(delivery.delivered_at AT TIME ZONE 'utc',
                        'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
           FROM webhook_deliveries delivery
           JOIN webhook_events event ON event.id = delivery.event_id
          WHERE ($1::webhook_delivery_status IS NULL
                   OR delivery.status = $1::webhook_delivery_status)
            AND ($2::uuid IS NULL OR delivery.event_id = $2::uuid)
            AND ($3::uuid IS NULL OR delivery.endpoint_id = $3::uuid)
          ORDER BY delivery.id",
    )
    .bind(&filter.status)
    .bind(&filter.event_id)
    .bind(&filter.endpoint_id)
    .fetch_all(&pool)
    .await;

    match found {
        Ok(rows) => Json(
            rows.into_iter()
                .map(Delivery::from)
                .collect::<Vec<Delivery>>(),
        )
        .into_response(),
        // invalid_text_representation: a filter named something its column has no value for.
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            error(StatusCode::BAD_REQUEST, "invalid_filter")
        }
        Err(err) => internal(err, "listing the webhook deliveries"),
    }
}
