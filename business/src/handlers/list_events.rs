use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use sqlx::PgPool;

use super::{error, internal};
use crate::sqlstate;
use crate::webhooks;

#[derive(Deserialize)]
pub struct Filter {
    after: Option<String>,
    r#type: Option<String>,
    limit: Option<i64>,
}

/// How many events one page holds when the caller does not say.
const DEFAULT_LIMIT: i64 = 100;

/// The ceiling on `?limit=`. Asking for more is refused rather than quietly capped: a caller who
/// asked for 5000, got 1000, and read the short page as "that is all of them" would skip every
/// event in between — which is precisely the failure this endpoint exists to prevent.
const MAX_LIMIT: i64 = 1000;

/// Every column an event exposes, in the order the query lists them.
type EventRow = (String, String, String, String);

/// `GET /events`, optionally `?after=<event_id>`, `?type=`, `?limit=`
///
/// **The event log, and the answer to "how does a business reconcile what it missed?"** Each
/// entry is byte for byte the body that was, or would have been, delivered — same envelope, same
/// `data` — so catching up here and receiving a webhook are the same information arriving two
/// ways.
///
/// Poll it with the id of the last event already processed:
///
/// ```text
/// GET /events?after=01a0471f-3a31-7a33-a066-89badf71de40&limit=100
/// ```
///
/// `id` is a uuidv7 and its leading bits are a timestamp, so ordering by the primary key *is*
/// ordering by creation time and `after` is a cursor rather than an offset — a page cannot shift
/// under a caller who is midway through reading it.
///
/// **A pull, deliberately, rather than a redelivery endpoint.** Asking to be pushed something a
/// second time is asking to miss it a second time, and it would need a mechanism — a queue that
/// can be re-armed, a rule for which deliveries qualify — where this needs none. It also covers
/// the case a redelivery could not: events raised before an endpoint was ever configured have no
/// delivery rows to replay, and they are in here all the same.
pub async fn list_events(State(pool): State<PgPool>, Query(filter): Query<Filter>) -> Response {
    let limit = filter.limit.unwrap_or(DEFAULT_LIMIT);
    if limit <= 0 || limit > MAX_LIMIT {
        return error(StatusCode::BAD_REQUEST, "invalid_limit");
    }

    // NULL casts to NULL rather than erroring, so an absent filter matches every row and only a
    // *present* and malformed one reaches the cast that rejects it.
    let found: Result<Vec<EventRow>, _> = sqlx::query_as(
        "SELECT id::text, type,
                to_char(created_at AT TIME ZONE 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                payload::text
           FROM webhook_events
          WHERE ($1::uuid IS NULL OR id > $1::uuid)
            AND ($2::text IS NULL OR type = $2)
          ORDER BY id
          LIMIT $3",
    )
    .bind(&filter.after)
    .bind(&filter.r#type)
    .bind(limit)
    .fetch_all(&pool)
    .await;

    let rows = match found {
        Ok(rows) => rows,
        // invalid_text_representation: `?after=` is not a uuid. An unknown `?type=` is not an
        // error — the vocabulary is text and only ever grows, so a type nothing has emitted yet
        // is an empty page rather than a bad request.
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            return error(StatusCode::BAD_REQUEST, "invalid_after");
        }
        Err(err) => return internal(err, "listing the events"),
    };

    // Assembled by the same function the dispatcher signs, which is what makes "what you read
    // here is what you would have been sent" true rather than merely intended.
    let mut events = Vec::with_capacity(rows.len());
    for (id, event_type, created_at, payload) in rows {
        match webhooks::envelope(&id, &event_type, &created_at, &payload) {
            Ok(envelope) => events.push(envelope),
            // A stored payload that will not parse is this service's bug, not the caller's, and
            // silently omitting the row would leave a hole in a log whose whole value is that it
            // has none.
            Err(err) => return internal(err, "rebuilding a stored event envelope"),
        }
    }

    // A bare array, and an empty one when nothing is newer than `after` — not a 404. "Nothing
    // has happened since" is a successful answer to "what has happened since?", and it is the
    // answer a caller polling this endpoint gets most of the time.
    Json(events).into_response()
}
