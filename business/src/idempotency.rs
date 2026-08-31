//! `Idempotency-Key` for the one endpoint where a retry can cost real money.
//!
//! `payments::claim` already stops two concurrent charges of one invoice. What it cannot do is
//! recognise that two requests are *the same request*: a client whose charge timed out has no
//! way to say "this is the retry, not a second attempt". That identity has to come from the
//! caller, and this is what carries it.
//!
//! The guarantee is exact and has no expiry: **one key, one response, forever.** The first
//! request under a key is charged and its answer recorded; every later request under that key
//! is handed that same answer back, whatever it was. A caller who wants a genuinely new attempt
//! uses a new key — which is what a key is for, one per intended charge.
//!
//! Caching failures matters as much as caching successes. A `504` from a charge whose outcome
//! is unknown is precisely the response a client retries, and re-running the handler would put
//! a second charge against a card that may already have been billed.

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, error as log_error, instrument};

use crate::state::AppState;

/// What a request body may weigh before it is refused, in bytes.
///
/// The only body this endpoint takes is `{"card_token": "..."}`. The cap exists because the
/// body has to be held in memory to be hashed, and an unbounded read on a payment endpoint is
/// an invitation; nothing legitimate comes close to it.
const MAX_BODY: usize = 64 * 1024;

/// The error body every endpoint returns, duplicated from `handlers` rather than shared.
///
/// Middleware sits outside the handlers, and widening a private helper's visibility to reach
/// across that line would say the two are one thing. They are not: this answers before any
/// handler is chosen.
fn error(status: StatusCode, code: &str) -> Response {
    (status, axum::Json(json!({ "error": code }))).into_response()
}

/// A `500` with its cause logged, duplicated from `handlers` for the reason directly above.
fn internal(err: impl std::fmt::Display, failed: &'static str) -> Response {
    log_error!(%err, failed, "internal error");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
}

/// Wraps a route so that a key seen before answers from the record instead of running it.
///
/// The order is what makes this safe, and it does not survive rearranging:
///
/// 1. Reserve the key, and commit that. A row exists before the handler is entered, so a
///    duplicate arriving mid-charge collides with something.
/// 2. Run the handler.
/// 3. Record the response against the reservation.
///
/// Reserving *after* the charge would leave the window this exists to close: two retries of a
/// timed-out request would both find no key, both charge, and race to record the answer.
///
/// `skip_all` and no `key` field. The key is a caller-chosen string standing for one charge, and
/// the layer in `logging` redacts the header it arrives in; naming it here would undo that on every
/// request rather than only on the one place below that has a reason to.
#[instrument(skip_all)]
pub(crate) async fn idempotent(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Required, not optional. A charge with no key is a charge nobody can safely retry, and
    // letting one through would make the protection depend on the caller remembering to ask
    // for it — on the one endpoint where forgetting bills someone twice.
    let key = match request.headers().get("idempotency-key") {
        Some(value) => match value.to_str() {
            Ok(key) if !key.is_empty() => key.to_owned(),
            _ => return error(StatusCode::BAD_REQUEST, "idempotency_key_missing"),
        },
        None => return error(StatusCode::BAD_REQUEST, "idempotency_key_missing"),
    };

    // The body has to be read to be hashed, so it is buffered here and put back below. The
    // handler's `Json` extractor sees an ordinary body and cannot tell the difference.
    let (parts, body) = request.into_parts();
    let Ok(body) = axum::body::to_bytes(body, MAX_BODY).await else {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "request_body_too_large");
    };

    // Method and path, not just the body. Without the path, one key sent against two invoices
    // would replay the first invoice's answer for the second — the wrong invoice reported paid,
    // which is worse than either charging or refusing.
    let mut fingerprint = format!("{} {}\n", parts.method, parts.uri.path()).into_bytes();
    fingerprint.extend_from_slice(&body);

    match reserve(&state.pool, &key, &fingerprint).await {
        Err(err) => return internal(err, "reserving the idempotency key"),
        Ok(Reservation::Held(response)) => return response,
        Ok(Reservation::Taken) => {
            debug!("idempotency key reserved: this request is the one that runs");
        }
    }

    let response = next.run(Request::from_parts(parts, Body::from(body))).await;

    let (parts, body) = response.into_parts();
    let Ok(body) = axum::body::to_bytes(body, MAX_BODY).await else {
        return internal(
            "the response body exceeded MAX_BODY",
            "reading the response to record it",
        );
    };

    // Recorded before the response is handed back, so a caller fast enough to retry the instant
    // it sees an answer finds that answer already there.
    //
    // A failure here loses the *replay*, not the answer. The charge happened and this is its
    // result, so it goes back to the caller either way: a `500` would report a failure that did
    // not occur, and would withhold the one thing — the intent id — that lets the caller find
    // out what became of the money.
    //
    // The reservation is left exactly as it is. It cannot be filled in, since writing is what
    // just failed, and it must not be deleted: deleting it is what would let a retry charge the
    // card a second time. The key is spent, and the caller is holding the answer it stood for.
    //
    // The key is named here and only here. It is the sole handle on which reservation is now spent
    // with nothing recorded against it, and this module owns the key — the generic layer, which
    // cannot know that, redacts it.
    if let Err(err) = record(&state.pool, &key, parts.status, &body).await {
        log_error!(%err, key, "could not record the response against its idempotency key");
    }

    Response::from_parts(parts, Body::from(body))
}

/// What claiming a key established.
enum Reservation {
    /// This request owns the key and is the one that runs.
    Taken,
    /// Somebody else owns it. The response is what this request gets instead.
    Held(Response),
}

/// Claims `key` for this request, or works out what to answer because someone else has it.
///
/// `ON CONFLICT DO NOTHING` is the whole concurrency story: the primary key decides which of
/// several simultaneous requests owns this one, in one statement, with no read to race against.
async fn reserve(pool: &PgPool, key: &str, fingerprint: &[u8]) -> Result<Reservation, sqlx::Error> {
    let claimed = sqlx::query(
        "INSERT INTO idempotency_keys (key, request_hash)
         VALUES ($1, encode(sha256($2::bytea), 'hex'))
             ON CONFLICT (key) DO NOTHING",
    )
    .bind(key)
    .bind(fingerprint)
    .execute(pool)
    .await?;

    if claimed.rows_affected() == 1 {
        return Ok(Reservation::Taken);
    }

    let existing: Option<(String, Option<i16>, Option<String>)> = sqlx::query_as(
        "SELECT request_hash, status_code, response_body FROM idempotency_keys WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;

    // Nothing there after all: the row was deleted between the insert and this read, which
    // nothing in this service does. Treated as a failure rather than guessed at.
    let Some((recorded, status, body)) = existing else {
        return Ok(Reservation::Held(internal(
            "the reservation disappeared between the insert and the read",
            "reading back an idempotency key this request did not claim",
        )));
    };

    // Postgres computed the stored hash and computes this one the same way, so comparing here
    // rather than in SQL costs a round trip nothing and keeps the mismatch legible.
    let matches: Option<(bool,)> = sqlx::query_as("SELECT encode(sha256($1::bytea), 'hex') = $2")
        .bind(fingerprint)
        .bind(&recorded)
        .fetch_optional(pool)
        .await?;

    if matches != Some((true,)) {
        debug!("idempotency key reused for a different request");
        // A key stands for one request. Reused for a different one, the honest answer is that
        // the caller has a bug — replaying would answer a question they did not ask, and
        // charging would defeat the key entirely.
        return Ok(Reservation::Held(error(
            StatusCode::BAD_REQUEST,
            "idempotency_key_reused",
        )));
    }

    match (status, body) {
        (Some(status), Some(body)) => {
            debug!(
                status,
                "replaying the recorded response for this idempotency key"
            );
            Ok(Reservation::Held(replay(status, body)))
        }

        // The first request holds the key and has not finished. There is no recorded answer to
        // give, and waiting for one would pin a connection — five in the pool — for as long as
        // the PSP takes, which is up to the five second charge timeout. So the caller is told
        // what is true: this exact request is already running. Retrying the same key once it
        // finishes gets the answer.
        _ => {
            debug!("the first request under this idempotency key has not finished");
            Ok(Reservation::Held(error(
                StatusCode::CONFLICT,
                "idempotency_key_in_flight",
            )))
        }
    }
}

/// Rebuilds a recorded answer.
///
/// The body goes back verbatim rather than being parsed and re-serialised, so a replay is byte
/// for byte what the first caller saw. Nothing marks it as a replay: that a caller cannot tell
/// is the point, and a header saying otherwise would be a detail of this table leaking into the
/// API.
fn replay(status: i16, body: String) -> Response {
    let Ok(status) = StatusCode::from_u16(status as u16) else {
        return internal(
            format!("{status} is not a status code"),
            "replaying a recorded response",
        );
    };

    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Fills in the answer against a reservation this request already holds.
///
/// Both columns in one statement, which is what the table's CHECK constraint insists on: there
/// is no moment at which a status exists without its body for a replay to find.
async fn record(
    pool: &PgPool,
    key: &str,
    status: StatusCode,
    body: &Bytes,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE idempotency_keys
            SET status_code = $2, response_body = convert_from($3::bytea, 'UTF8')
          WHERE key = $1",
    )
    .bind(key)
    .bind(status.as_u16() as i16)
    .bind(body.as_ref())
    .execute(pool)
    .await
    .map(|_| ())
}
