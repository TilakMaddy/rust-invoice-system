//! Outbound webhooks: what gets sent, who it goes to, and how it is signed.
//!
//! The mechanism is a **transactional outbox**. Nothing here makes an HTTP request — the
//! functions in this module write rows, and they are called from inside transactions that are
//! already open for another reason. [`delivery`] is the half that talks to the network, and it
//! runs on a background task with no request waiting on it.
//!
//! That split is the whole point. A receiver is a third party, and putting one in the request
//! path would mean `POST /invoices` failing because somebody else's server was down. Worse:
//! `payments::settle` does its work holding row locks on `invoices` and `payment_intents`, so an
//! HTTP call from in there would hold those locks across the network — and the 100ms
//! `lock_timeout` every transition in this service relies on would start failing live charges.
//!
//! Writing the event in the same transaction as the change that caused it is what makes the
//! handoff safe: an event exists exactly when that change committed. A rolled-back charge emits
//! nothing, and a process that dies before delivering has still recorded what it owes.

pub(crate) mod delivery;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use sqlx::{PgConnection, PgPool};
use tracing::{debug, info, instrument};

/// What this service emits.
///
/// The labels live here and nowhere else, the way `InvoiceState` owns the invoice vocabulary.
/// Stored as `text` rather than a Postgres enum: the core schema's enums describe state machines
/// this service must handle exhaustively, whereas an event vocabulary only ever grows, and a
/// receiver already has to ignore types it does not recognise. Adding one should be a line of
/// Rust, not a migration.
//
// Every variant shares the `Invoice` prefix because every event this service has *so far* is
// about an invoice; clippy reads that as a naming smell, and it would be right the moment a
// `customer.created` joined them. Allowed rather than renamed, because dropping the prefix
// would leave `Created`, `Paid` and `PaymentFailed` — names that stop saying what they are
// about exactly when a second subject makes the question live.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EventType {
    InvoiceCreated,
    InvoicePaid,
    InvoicePaymentFailed,
}

impl EventType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InvoiceCreated => "invoice.created",
            Self::InvoicePaid => "invoice.paid",
            Self::InvoicePaymentFailed => "invoice.payment_failed",
        }
    }
}

/// A registered receiver, as the environment declares it.
///
/// `deny_unknown_fields` so a mistyped key is a startup failure rather than an endpoint that
/// silently signs with no secret — the same reading `main` gives every other malformed value.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub url: String,
    pub secret: String,
}

/// Reads `WEBHOOK_ENDPOINTS`.
///
/// JSON rather than a separator-delimited list because a URL may legitimately contain `,`, `;`
/// and `=` in its query string, and `serde_json` is already a dependency — a hand-rolled parser
/// would be a new way to be wrong about somebody's URL.
///
/// The error is a string for `main` to print and die on. Every rejection here is a deployment
/// that would otherwise come up signing with an empty key or posting to a scheme reqwest cannot
/// speak, which is worth refusing to start over.
pub fn parse(raw: &str) -> Result<Vec<Endpoint>, String> {
    let endpoints: Vec<Endpoint> = serde_json::from_str(raw)
        .map_err(|err| format!("not a JSON array of {{url, secret}}: {err}"))?;

    for endpoint in &endpoints {
        // reqwest is built without a TLS feature, so an https:// URL fails at the request rather
        // than here. It is still accepted: the check that belongs in configuration is the shape
        // of the URL, and a deployment that adds the feature should not also have to edit this.
        if !endpoint.url.starts_with("http://") && !endpoint.url.starts_with("https://") {
            return Err(format!(
                "{}: expected an http:// or https:// URL",
                endpoint.url
            ));
        }
        if endpoint.secret.is_empty() {
            return Err(format!("{}: the signing secret is empty", endpoint.url));
        }
    }

    // A duplicate URL would make the startup upsert write the same row twice in one statement,
    // which Postgres rejects outright ("cannot affect row a second time"). Caught here so the
    // message names the mistake instead of the SQLSTATE it produces.
    for (i, endpoint) in endpoints.iter().enumerate() {
        if endpoints[..i].iter().any(|other| other.url == endpoint.url) {
            return Err(format!("{}: listed twice", endpoint.url));
        }
    }

    Ok(endpoints)
}

/// Makes `webhook_endpoints` say what the environment says.
///
/// Run at startup next to the migrations, before the listener binds, so no request is ever
/// served against a stale endpoint set.
///
/// Configuration decides *which* endpoints exist; the table is the durable record deliveries
/// point at. Keeping both is what lets the fan-out be one `INSERT ... SELECT` needing nothing
/// but a connection — read the list from memory instead and `settle` would grow an endpoints
/// argument threaded from both its call sites, while `create_invoice` would have to take the
/// whole `AppState` in place of the `PgPool` that `state`'s `FromRef` impl exists to give it.
///
/// It also means a delivery already queued survives a configuration change: its endpoint row
/// still carries a URL and a secret after the environment drops it.
///
/// Logs both counts rather than only the enabled one. An endpoint dropping out of the
/// configuration silently stops receiving, and the deployment that did it by accident — a typo in
/// `WEBHOOK_ENDPOINTS`, a key removed from the wrong environment — looks exactly like the
/// deployment that meant it. The number that changed is the only thing that says which.
///
/// `skip_all`, because the argument is a list of signing secrets.
#[instrument(skip_all)]
pub async fn sync(pool: &PgPool, endpoints: &[Endpoint]) -> Result<(), sqlx::Error> {
    // Serialising back to JSON so Postgres can expand it with `json_to_recordset` keeps the whole
    // sync to two statements with no loop — the same "let Postgres do it" reading `idempotency`
    // takes for hashing.
    let configured = serde_json::to_string(endpoints).expect("Endpoint always serialises");

    let mut tx = pool.begin().await?;

    // A URL that comes back after being dropped is revived rather than duplicated, and its
    // secret is whatever the configuration now says — that is how rotation lands.
    sqlx::query(
        "INSERT INTO webhook_endpoints (url, secret)
         SELECT url, secret FROM json_to_recordset($1::json) AS configured(url text, secret text)
             ON CONFLICT (url) DO UPDATE
                SET secret = EXCLUDED.secret, disabled_at = NULL",
    )
    .bind(&configured)
    .execute(&mut *tx)
    .await?;

    // NOT EXISTS rather than NOT IN: the two differ on NULLs, and a correlated subquery cannot
    // be quietly turned into "match nothing" by one.
    let retired = sqlx::query(
        "UPDATE webhook_endpoints SET disabled_at = now()
          WHERE disabled_at IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM json_to_recordset($1::json) AS configured(url text, secret text)
                 WHERE configured.url = webhook_endpoints.url
            )",
    )
    .bind(&configured)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    info!(
        enabled = endpoints.len(),
        disabled = retired.rows_affected(),
        "webhook endpoints synced"
    );
    Ok(())
}

/// Records an event and queues it for every enabled endpoint.
///
/// Takes a connection rather than a pool, so it composes into a transaction that is already
/// open. That is the entire contract: callers must be inside the transaction that performs the
/// state change, or the guarantee this module rests on is gone.
///
/// The event is written even when no endpoint is enabled, and the fan-out is then simply zero
/// rows. That is deliberate — `GET /events` is how a receiver added later catches up, and an
/// event that left no trace because nobody was listening at the time could not be caught up on.
///
/// `skip_all`, because `data` is the event payload — an invoice, its customer and its totals — and
/// the connection is a connection. The type is named explicitly; the payload is not, and reading it
/// back is what `GET /events` is for.
#[instrument(skip_all, fields(event = event.as_str()))]
pub(crate) async fn enqueue(
    conn: &mut PgConnection,
    event: EventType,
    data: &Value,
) -> Result<(), sqlx::Error> {
    let (id,): (String,) = sqlx::query_as(
        "INSERT INTO webhook_events (type, payload) VALUES ($1, $2::jsonb) RETURNING id::text",
    )
    .bind(event.as_str())
    .bind(data.to_string())
    .fetch_one(&mut *conn)
    .await?;

    // One statement, so which endpoints were enabled is decided at the same instant the event is
    // recorded rather than by a read that a concurrent sync could invalidate.
    let queued = sqlx::query(
        "INSERT INTO webhook_deliveries (event_id, endpoint_id)
         SELECT $1::uuid, id FROM webhook_endpoints WHERE disabled_at IS NULL",
    )
    .bind(&id)
    .execute(&mut *conn)
    .await?;

    // Not committed yet — the caller's transaction owns that, and the event exists only if it
    // commits. Logged here anyway: this is where the fan-out width is known, and a settlement that
    // rolls back leaves this line above a request that ends in a `500`, which is the shape of the
    // problem rather than a lie about it.
    debug!(
        event_id = id,
        deliveries = queued.rows_affected(),
        "webhook event enqueued"
    );
    Ok(())
}

/// The body a receiver gets, and the body `GET /events` shows.
///
/// One function for both, so what a business reads back while reconciling is byte for byte what
/// it would have been sent. `data` arrives as the stored JSON text and is reparsed here rather
/// than being interpolated, so a malformed payload fails loudly instead of producing a body that
/// is signed but not JSON.
pub(crate) fn envelope(
    id: &str,
    event_type: &str,
    created_at: &str,
    data: &str,
) -> Result<Value, serde_json::Error> {
    Ok(serde_json::json!({
        "id": id,
        "type": event_type,
        "created_at": created_at,
        "data": serde_json::from_str::<Value>(data)?,
    }))
}

/// `HMAC-SHA256(secret, "{id}.{timestamp}.{body}")`, hex.
///
/// **The id is inside the signed string, not merely a header.** That is what authenticates the
/// receiver's deduplication key: an attacker replaying a captured body under a fresh id cannot
/// slip past a dedupe table, because changing the id invalidates the signature.
///
/// **The timestamp is what makes a replay expire.** It is fresh on every attempt, so the same
/// event redelivered an hour later carries a signature a receiver can tell apart from the
/// original — and a receiver that enforces a tolerance window (300 seconds is the documented
/// one) is refusing a captured request rather than trusting that nobody kept it.
///
/// Note what this service cannot do: enforce either. It can only put the material on the wire.
/// The verification recipe is the receiver's to run, which is why webhook-receiver implements it
/// in full rather than the README merely describing it.
pub(crate) fn sign(secret: &str, id: &str, timestamp: i64, body: &str) -> String {
    // HMAC is defined for a key of any length — short keys are zero-padded and long ones are
    // hashed — so the only error this can return is unreachable.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts a key of any length");

    // Fed in pieces rather than concatenated into one String, so the body is never copied.
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());

    hex(&mac.finalize().into_bytes())
}

/// Seconds since the epoch, for the signature's timestamp.
///
/// A clock behind the epoch is not a case worth a `Result` that every caller would have to
/// invent an answer for: it reads as 0, the receiver's tolerance window rejects the delivery,
/// and the retry after the clock is fixed succeeds.
pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or_default()
}

/// Lowercase hex. Four lines rather than a crate, as `auth`'s comparison is.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// The HTTP client the dispatcher delivers with.
///
/// Separate from `Psp`'s despite both being reqwest clients: they have different timeouts for
/// different reasons, and a receiver being slow has nothing to do with the payment service being
/// slow. Cloning is an `Arc` bump, as it is there.
#[derive(Clone)]
pub struct Webhooks {
    pub(crate) client: reqwest::Client,
}

impl Webhooks {
    /// `timeout` bounds one delivery attempt, connection through body. An argument rather than a
    /// constant for the same reason `Psp`'s is: tests drive the timeout path in milliseconds
    /// instead of waiting out the ten seconds production allows.
    pub fn new(timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
        })
    }
}
