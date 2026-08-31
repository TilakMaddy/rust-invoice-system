//! The dispatcher: the half of webhooks that talks to the network.
//!
//! It runs on a background task and nothing waits on it, which is the point — see the module
//! docs in `webhooks` for why a receiver must never be in the request path.
//!
//! Delivery is **at-least-once and unordered**. A retry can land after a later event, and a
//! receiver that answered `200` on a response this service never saw will be sent the same event
//! again. Both are why every delivery carries a stable `webhook-id`: deduplicating on it is the
//! receiver's half of the contract, and the payload's invoice `state` is authoritative over the
//! order things arrived in.

use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, error, info_span, instrument, warn};

use crate::state::AppState;
use crate::webhooks;

/// How many deliveries one pass takes on. They run concurrently, so this is the ceiling on
/// in-flight requests to third-party servers, not a batch that is worked through in order.
const BATCH: i64 = 20;

/// How long a claimed delivery is hidden from other passes.
///
/// The claim only *leases* a row: it pushes `next_attempt_at` out and leaves everything else
/// alone, and the outcome is recorded afterwards. So a process that dies mid-attempt loses
/// nothing — the lease expires and the delivery is retried without having burned an attempt.
///
/// Comfortably longer than the per-attempt timeout, which is what stops a slow receiver from
/// having its delivery claimed a second time while the first attempt is still open.
const LEASE: Duration = Duration::from_secs(60);

/// What a receiver gets before this service gives up, and how long it waits between tries.
///
/// `BACKOFF[n - 1]` is the wait after attempt `n` failed, so the array's length is the number of
/// retries and `MAX_ATTEMPTS` is one more than that. Roughly ×5 up to a ceiling, which puts the
/// first retry inside a deploy's window and the last one the far side of a night:
///
/// | after attempt | wait | cumulative |
/// | --- | --- | --- |
/// | 1 | 10s | 10s |
/// | 2 | 1m | 1m 10s |
/// | 3 | 5m | 6m 10s |
/// | 4 | 25m | 31m 10s |
/// | 5 | 2h | 2h 31m |
/// | 6 | 6h | 8h 31m |
/// | 7 | 12h | 20h 31m |
///
/// **The budget is eight attempts over about 20½ hours.** Long enough that an endpoint fixed the
/// next morning still receives what it missed overnight; short enough to close inside a day, so
/// a delivery cannot outlive the daily window a business reconciles in.
const BACKOFF: [i64; 7] = [10, 60, 300, 1500, 7200, 21600, 43200];

const MAX_ATTEMPTS: i32 = BACKOFF.len() as i32 + 1;

/// One claimed delivery, in the order the query below returns it: the delivery, how many
/// attempts it had already used, then the event it carries and the endpoint it is owed to.
type Due = (String, i32, String, String, String, String, String, String);

/// Delivers everything that is due, once.
///
/// Errors are logged and skipped rather than propagated, exactly as the reconciler treats them:
/// one endpoint that cannot be reached must not stop the others, and nothing here is worth
/// killing the task over — the next pass tries again.
#[instrument(skip_all, name = "dispatcher")]
pub async fn deliver_once(state: &AppState) {
    let due = match claim(&state.pool).await {
        Ok(due) => due,
        Err(err) => return error!(%err, "could not claim deliveries"),
    };

    // Only when there is something. This ticks every second and an empty outbox is the normal
    // case, so an unconditional line here is the entire log at the default level — once a second,
    // forever, saying nothing happened.
    if !due.is_empty() {
        debug!(claimed = due.len(), "claimed the deliveries that are due");
    }

    // Concurrently, so one receiver sitting on the full timeout does not hold up the nineteen
    // behind it. Each task writes only its own row, so they cannot conflict.
    let mut attempts = JoinSet::new();
    for row in due {
        let state = state.clone();
        // The span is opened here rather than with `#[instrument]` on `attempt`, because that
        // attribute would have to name `row` to skip it and the tasks are spawned rather than
        // awaited in place — a spawned task inherits no span, so it has to be given one. The
        // fields are read off the row by position: the delivery, the event type, the url. Not the
        // secret, which is the last element and is never named anywhere in this module.
        let span = info_span!(
            "delivery",
            delivery_id = %row.0,
            event = %row.3,
            url = %row.6,
            attempt = row.1 + 1
        );
        attempts.spawn(async move { attempt(&state, row).instrument(span).await });
    }
    attempts.join_all().await;
}

/// Takes up to [`BATCH`] due deliveries and leases them.
///
/// `FOR UPDATE SKIP LOCKED` is what makes this safe to run in more than one process: two
/// dispatchers polling at the same instant step over each other's rows instead of blocking on
/// them, and neither can claim what the other holds.
///
/// The lease is the only thing written. Attempts are counted when the outcome is known, so a
/// crash between here and there costs a minute rather than one of the eight tries.
async fn claim(pool: &PgPool) -> Result<Vec<Due>, sqlx::Error> {
    sqlx::query_as(
        "WITH due AS (
             SELECT id FROM webhook_deliveries
              WHERE status = 'pending' AND next_attempt_at <= now()
              ORDER BY next_attempt_at
                FOR UPDATE SKIP LOCKED
              LIMIT $1
         ),
         leased AS (
             UPDATE webhook_deliveries
                SET next_attempt_at = now() + ($2 || ' seconds')::interval
              WHERE id IN (SELECT id FROM due)
          RETURNING id, event_id, endpoint_id, attempts
         )
         SELECT leased.id::text,
                leased.attempts,
                event.id::text,
                event.type,
                to_char(event.created_at AT TIME ZONE 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                event.payload::text,
                endpoint.url,
                endpoint.secret
           FROM leased
           JOIN webhook_events event ON event.id = leased.event_id
           JOIN webhook_endpoints endpoint ON endpoint.id = leased.endpoint_id",
    )
    .bind(BATCH)
    .bind(LEASE.as_secs() as i64)
    .fetch_all(pool)
    .await
}

/// Signs one delivery, sends it, and records how it went.
async fn attempt(state: &AppState, row: Due) {
    let (delivery_id, used, event_id, event_type, created_at, payload, url, secret) = row;
    let attempts = used + 1;

    let envelope = match webhooks::envelope(&event_id, &event_type, &created_at, &payload) {
        Ok(envelope) => envelope,
        // The stored payload will not parse, so there is no body to send and no later attempt
        // that would do better. Given up on immediately rather than retried eight times.
        Err(err) => {
            let unreadable =
                Failure::unreachable(format!("event {event_id} has an unreadable payload: {err}"));
            return give_up(&state.pool, &delivery_id, attempts, &unreadable).await;
        }
    };

    let body = envelope.to_string();
    let timestamp = webhooks::now();
    let signature = webhooks::sign(&secret, &event_id, timestamp, &body);

    let sent = state
        .webhooks
        .client
        .post(&url)
        .header("content-type", "application/json")
        // Stable across retries: this is the key a receiver deduplicates on, and it is inside
        // the signed string so that it cannot be changed without breaking the signature.
        .header("webhook-id", &event_id)
        .header("webhook-timestamp", timestamp.to_string())
        // Not signed and not part of the contract — an operator aid, so a receiver's logs show
        // which try it was looking at.
        .header("webhook-attempt", attempts.to_string())
        .header("webhook-signature", format!("v1={signature}"))
        .body(body)
        .send()
        .await;

    // Any non-2xx counts as a failure, and so does no answer at all. Deliberately one rule with
    // no exceptions: a 404 or a 410 from a receiver whose routes are mid-deploy is exactly as
    // transient as a 502, and a policy that gave up on 4xx would drop events during a bad config
    // push -- silently, since the receiver never got them to notice.
    let failure = match sent {
        Ok(response) if response.status().is_success() => {
            return succeed(&state.pool, &delivery_id, attempts).await;
        }
        Ok(response) => Failure::answered(response.status().as_u16()),
        Err(err) => Failure::unreachable(because(&err)),
    };

    // One line per failed attempt, which is the half of the story the receiver's log cannot
    // tell: an attempt that never reached a receiver leaves no trace there, so a delivery that
    // is failing at the transport looks exactly like an event that was never sent. The record in
    // `webhook_deliveries` says the same thing, but only to someone who thought to query it.
    warn!(reason = failure.reason(), "delivery attempt failed");

    if attempts >= MAX_ATTEMPTS {
        return give_up(&state.pool, &delivery_id, attempts, &failure).await;
    }

    let backoff = BACKOFF[(attempts - 1) as usize];
    if let Err(err) = reschedule(&state.pool, &delivery_id, attempts, &failure, backoff).await {
        error!(%err, "could not reschedule the delivery");
    }
}

/// A transport failure and every cause underneath it, joined with `: `.
///
/// reqwest's own `Display` is always the same sentence — "error sending request for url (...)" —
/// and the url is the one part an operator already knows. Which fault it was lives entirely in
/// the source chain, so recording only the top line leaves a timed-out receiver, a refused
/// connection and a keep-alive socket closed under the client all looking identical in
/// `last_error`, which is the column that exists to tell them apart.
fn because(err: &reqwest::Error) -> String {
    use std::error::Error;
    use std::fmt::Write;

    let mut message = err.to_string();
    let mut cause = err.source();
    while let Some(current) = cause {
        let _ = write!(message, ": {current}");
        cause = current.source();
    }
    message
}

/// How one attempt failed.
///
/// The receiver's status, or the transport error when nothing answered — exactly one, which is
/// what the table's CHECK insists on. "Your endpoint returned 500" and "your endpoint did not
/// answer" need different fixes, and a single text column would blur the two into one string an
/// operator has to read rather than filter on.
struct Failure {
    status: Option<i16>,
    error: Option<String>,
}

impl Failure {
    fn answered(status: u16) -> Self {
        Self {
            status: Some(status as i16),
            error: None,
        }
    }

    /// A timeout, a refused connection, a socket dropped mid-response. The request left and
    /// nothing came back, so there is no status to record.
    fn unreachable(error: String) -> Self {
        Self {
            status: None,
            error: Some(error),
        }
    }

    /// The same fact as a sentence, for the log line that announces exhaustion.
    fn reason(&self) -> String {
        match (self.status, &self.error) {
            (Some(status), _) => format!("answered {status}"),
            (_, Some(error)) => error.clone(),
            // Both constructors set exactly one, and the struct is private to this module.
            _ => String::from("failed"),
        }
    }
}

async fn succeed(pool: &PgPool, delivery_id: &str, attempts: i32) {
    let recorded = sqlx::query(
        "UPDATE webhook_deliveries
            SET status = 'succeeded', attempts = $2, delivered_at = now(),
                last_status = 200, last_error = NULL
          WHERE id = $1::uuid",
    )
    .bind(delivery_id)
    .bind(attempts)
    .execute(pool)
    .await;

    // The receiver has it either way. A failure here loses the *record*, not the delivery, and
    // the cost of that is one duplicate once the lease expires — which is precisely what the
    // `webhook-id` header exists for.
    match recorded {
        Ok(_) => debug!("delivered"),
        Err(err) => error!(%err, "delivered, but the delivery could not be recorded"),
    }
}

/// Puts the delivery back in the queue, due after `backoff` seconds.
///
/// The jitter is ±10%, applied by Postgres rather than in Rust so that the schedule stays one
/// expression. It matters when an endpoint comes back after an outage: without it, every
/// delivery queued while it was down would be retried in the same instant, and the receiver's
/// first act on recovering would be to be knocked over again.
async fn reschedule(
    pool: &PgPool,
    delivery_id: &str,
    attempts: i32,
    failure: &Failure,
    backoff: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE webhook_deliveries
            SET attempts = $2, last_status = $3, last_error = $4,
                next_attempt_at = now()
                    + ($5::double precision * (0.9 + random() * 0.2)) * interval '1 second'
          WHERE id = $1::uuid",
    )
    .bind(delivery_id)
    .bind(attempts)
    .bind(failure.status)
    .bind(failure.error.as_deref())
    .bind(backoff)
    .execute(pool)
    .await
    .map(|_| ())
}

/// Ends the delivery: the budget is spent, and nothing will try again.
///
/// **Exhaustion is announced, not merely recorded.** A row that quietly turns `exhausted` is an
/// event a business was owed and never got, with nobody told — and the whole point of a retry
/// budget is that running out of it is news. The line ends in `would send email` because this
/// service has no mailer to send one with; wiring it to a real one is a single function, and
/// inventing an SMTP dependency to prove that is not.
async fn give_up(pool: &PgPool, delivery_id: &str, attempts: i32, failure: &Failure) {
    // The last failure is kept as it was classified, not flattened into prose: an operator
    // filtering `GET /webhook_deliveries?status=exhausted` wants to see at a glance whether these
    // are 500s from a broken handler or connections that never landed.
    let recorded = sqlx::query(
        "UPDATE webhook_deliveries
            SET status = 'exhausted', attempts = $2, last_status = $3, last_error = $4
          WHERE id = $1::uuid",
    )
    .bind(delivery_id)
    .bind(attempts)
    .bind(failure.status)
    .bind(failure.error.as_deref())
    .execute(pool)
    .await;

    if let Err(err) = recorded {
        error!(%err, "could not record the delivery as exhausted");
    }

    // The span already carries the delivery, the event type and the url, so what is left to say is
    // the count and the last reason. `would send email` stays word for word: it is the marker that
    // this is where a real alert goes, and it should still be greppable.
    error!(
        attempts,
        reason = failure.reason(),
        "giving up on this delivery -- would send email"
    );
}
