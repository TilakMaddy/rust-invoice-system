//! The client for mock-payment-service.
//!
//! Everything here exists to answer one question honestly: did the card get charged? The PSP
//! has two failure modes that look like errors but are not answers — a 30 second charge that
//! outlives any sane timeout, and a charge that succeeds and then loses its response — so the
//! client reports "I do not know" as a first-class outcome rather than folding it into
//! failure. Reading either as a decline would release an invoice whose money already moved.

use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, instrument, warn};

#[derive(Clone)]
pub struct Psp {
    // reqwest::Client is a handle around a shared, pooled inner client, so cloning it per
    // request is the intended usage and costs an Arc bump rather than a new connection pool.
    client: reqwest::Client,
    base_url: Arc<str>,
}

impl Psp {
    /// `timeout` bounds every call, connection through body. It is an argument rather than a
    /// constant so tests can drive the timeout path in milliseconds instead of waiting out the
    /// five seconds production uses.
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            // A trailing slash would produce `//payment_intents`, which axum's router does not
            // match — a configuration typo that would otherwise only surface as a 404 at the
            // first charge.
            base_url: base_url.trim_end_matches('/').into(),
        })
    }

    /// `POST /payment_intents`, returning the id the PSP issued.
    ///
    /// The PSP owns this id and `payment_intents.id` stores it verbatim, so no intent can be
    /// recorded before the PSP agrees one exists — which is why this call has to come first.
    #[instrument(skip_all)]
    pub async fn create_intent(&self) -> Result<String, PspError> {
        let url = format!("{}/payment_intents", self.base_url);
        let response = self.client.post(url).send().await.map_err(PspError::from)?;

        if !response.status().is_success() {
            warn!(status = response.status().as_u16(), "create was refused");
            return Err(PspError(format!("create returned {}", response.status())));
        }
        let view: PaymentView = response.json().await.map_err(PspError::from)?;

        debug!(intent_id = view.payment_intent_id, "intent issued");
        Ok(view.payment_intent_id)
    }

    /// `POST /payment_intents/{id}/pay` — the charge itself.
    ///
    /// Never returns an error: every way this can go wrong is already one of the three
    /// outcomes, and a caller that had to handle a fourth would only have to decide again what
    /// an unreachable PSP means for a charge that may have gone through.
    ///
    /// **`skip_all` is load-bearing here and nowhere more so.** The second argument is the payer's
    /// card token; a bare `#[instrument]` would record every argument through `Debug` and put it in
    /// the log on every charge. Only the intent id is named, and only because it is the handle this
    /// service hands the caller anyway.
    #[instrument(skip_all, fields(intent_id = %id))]
    pub async fn pay(&self, id: &str, card_token: &str) -> Charge {
        let url = format!("{}/payment_intents/{id}/pay", self.base_url);
        let sent = self
            .client
            .post(url)
            .json(&json!({ "card_token": card_token }))
            .send()
            .await;

        // A timeout, a dropped socket, a refused connection: the request left, and what the
        // PSP did with it is unknowable from here. `tok_network_error` is exactly this — it
        // records the charge as succeeded and *then* drops the connection.
        let Ok(response) = sent else {
            return unresolved("the request did not come back");
        };

        match response.status().as_u16() {
            // The only bodies worth parsing. A body that will not decode is not an answer.
            200 => match response.json::<PaymentView>().await {
                Ok(view) => match view.status {
                    PaymentStatus::Succeeded => resolved(Charge::Succeeded),
                    PaymentStatus::Failed => resolved(Charge::Failed),
                    // Not reachable from this endpoint, which only answers terminal states.
                    // Still not an answer if it ever were.
                    PaymentStatus::Pending => unresolved("the charge answered `pending`"),
                },
                Err(_) => unresolved("the body would not decode"),
            },

            // Both are definitive *on this endpoint*: an unrecognized card token is rejected
            // before the charge stage, and an intent the PSP has never heard of cannot have
            // been charged. Neither burns the attempt, so releasing the invoice is safe.
            //
            // Note this is the opposite reading to a 404 from `status` below, where the intent
            // is one we know exists and the PSP has simply forgotten it — possibly after
            // charging it.
            400 | 404 => resolved(Charge::Failed),

            // Anything else, 5xx included. A 5xx especially: the PSP is allowed to fail *after*
            // moving the money, so treating one as a decline is how an invoice gets released
            // and charged twice.
            status => unresolved_with(status),
        }
    }

    /// `GET /payment_intents/{id}` — what the PSP now says about an earlier attempt.
    ///
    /// `Ok(None)` means the PSP has no record of the intent. That is not the same as "it was
    /// never charged": the mock holds its state in memory and loses it on restart, so an
    /// intent it has forgotten may well have been paid.
    #[instrument(skip_all, fields(intent_id = %id))]
    pub async fn status(&self, id: &str) -> Result<Option<PaymentStatus>, PspError> {
        let url = format!("{}/payment_intents/{id}", self.base_url);
        let response = self.client.get(url).send().await.map_err(PspError::from)?;

        if response.status() == 404 {
            // The doc comment above is the reason this is a warning: an intent this service knows
            // it was issued, that the PSP no longer has, may well have been charged before it
            // restarted. Nothing can be inferred and the invoice stays in flight.
            warn!("the payment service has forgotten an intent it issued");
            return Ok(None);
        }
        if !response.status().is_success() {
            warn!(status = response.status().as_u16(), "status was refused");
            return Err(PspError(format!("status returned {}", response.status())));
        }
        let view: PaymentView = response.json().await.map_err(PspError::from)?;

        debug!(status = ?view.status, "intent read");
        Ok(Some(view.status))
    }
}

/// Logs a charge that came back with an answer, and returns it unchanged.
fn resolved(charge: Charge) -> Charge {
    debug!(outcome = ?charge, "charge returned");
    charge
}

/// Logs a charge that did not, and returns [`Charge::Unknown`].
///
/// **`warn`, and it is the most important line this service writes.** It means the card may have
/// been charged and this service cannot say — so the invoice is deliberately left in `processing`,
/// the caller gets a `504` carrying the intent id, and the money is where a human or the daily
/// reconciler has to go and look. `reason` is what distinguishes the four ways to arrive here,
/// none of which the caller is told apart because to them they are one fact.
fn unresolved(reason: &'static str) -> Charge {
    warn!(reason, "charge unresolved");
    Charge::Unknown
}

/// The same, for the status codes that carry their own reason.
fn unresolved_with(status: u16) -> Charge {
    warn!(
        status,
        reason = "the charge answered a status with no verdict in it",
        "charge unresolved"
    );
    Charge::Unknown
}

/// What a charge attempt established. Deliberately three-valued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Charge {
    Succeeded,
    Failed,
    /// The request did not come back with an answer. The money may or may not have moved, so
    /// nothing may be concluded and nothing may be written.
    Unknown,
}

/// The PSP's public status vocabulary, byte for byte the same labels as the
/// `payment_intent_status` enum the migration declares.
///
/// One type for the wire, the column and the API, because all three speak the same three
/// words. `sqlx::Type` decodes the column as itself rather than as text something re-parses —
/// so a label that drifts from the migration fails at the query — and `Serialize` is what
/// `GET /payment_intents/{id}` answers with, keeping this service's vocabulary and the PSP's
/// from diverging by accident.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "payment_intent_status", rename_all = "snake_case")]
pub enum PaymentStatus {
    Pending,
    Succeeded,
    Failed,
}

/// The PSP could not be reached, or answered something unusable.
///
/// Carries the reason as text rather than wrapping `reqwest::Error`: the only consumer is a
/// background job's log line, and an opaque string keeps reqwest's types out of the crate's
/// public surface.
#[derive(Debug)]
pub struct PspError(String);

impl From<reqwest::Error> for PspError {
    fn from(err: reqwest::Error) -> Self {
        Self(err.to_string())
    }
}

impl Display for PspError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The shape all three PSP endpoints answer with. `code` is ignored: the reason a card was
/// declined is the cardholder's business, and surfacing it would put the PSP's failure
/// vocabulary into this service's API.
#[derive(Deserialize)]
struct PaymentView {
    payment_intent_id: String,
    status: PaymentStatus,
}
