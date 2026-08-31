//! The database side of charging an invoice: the two transactions that move an invoice into a
//! charge and back out of it.
//!
//! These live here rather than in the handler because the daily reconciler runs the *same*
//! settlement. One definition means a change to how a charge is recorded cannot land in the
//! request path and miss the recovery path, which is exactly the drift that leaves invoices
//! stuck in `processing` forever.

use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, info, instrument};

use crate::handlers::Invoice;
use crate::sqlstate;
use crate::webhooks::{self, EventType};

/// The invoice lifecycle, mirroring the `invoice_state` enum the migration declares.
///
/// `sqlx::Type` binds and decodes it as that Postgres type directly, so a state crosses the
/// wire as itself rather than as text some cast has to re-parse — and a variant that drifts
/// from the migration's labels fails loudly at the query instead of quietly comparing unequal
/// to every state there is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "invoice_state", rename_all = "snake_case")]
pub(crate) enum InvoiceState {
    Draft,
    Ready,
    Processing,
    Processed,
    Void,
}

impl InvoiceState {
    /// The label the column, the API and the webhook payloads all use, for a log line that agrees
    /// with them. `Debug` would render `Processed` where every other view of this invoice says
    /// `processed`, which is one vocabulary too many for a state machine that has exactly one.
    /// Spelled out the way `webhooks::EventType::as_str` is, and for the same reason.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Ready => "ready",
            Self::Processing => "processing",
            Self::Processed => "processed",
            Self::Void => "void",
        }
    }
}

/// Every column the API exposes, in the order the `RETURNING` clauses list them. uuids and
/// dates are cast to text in SQL, which is what keeps a uuid decode feature and a date crate
/// out of the dependency list; `state` needs no cast, decoding as itself.
pub(crate) type InvoiceRow = (String, String, i64, String, InvoiceState);

/// How a charge attempt ended, for the transaction that records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Succeeded,
    Failed,
}

/// Records the intent the PSP has just issued, against the invoice it is meant to settle.
///
/// `'pending'` is written out rather than left to the column default: this row exists for
/// exactly as long as the outcome is unknown, and the one place that says so should say it out
/// loud.
///
/// The row is committed before any attempt to claim the invoice, so an attempt that goes on to
/// lose the claim leaves it behind, pointed at by nothing. That is the price of the PSP owning
/// the id, and the row stays: nothing deletes intents, because what it records — that this
/// service asked for an intent and never charged it — is true.
///
/// Logged at `info`, not `debug`. This row is the only handle on a charge whose response was lost:
/// `GET /payment_intents/{id}` reads it, the reconciler works from it, and the `504` the caller
/// gets carries its id for exactly that reason. A log that has the id has the thread to pull.
#[instrument(skip_all, fields(invoice_id = %invoice_id, intent_id = %intent_id))]
pub(crate) async fn record_intent(
    pool: &PgPool,
    invoice_id: &str,
    intent_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO payment_intents (id, invoice_id, status) VALUES ($1, $2::uuid, 'pending')",
    )
    .bind(intent_id)
    .bind(invoice_id)
    .execute(pool)
    .await?;

    info!("payment intent recorded");
    Ok(())
}

/// Takes the invoice for this attempt, returning whether this attempt is the one that got it.
///
/// The state test lives in the `WHERE` of the write itself, so there is no verdict that can go
/// stale between reading and acting: under READ COMMITTED a rival that blocks here re-evaluates
/// that `WHERE` against whatever version the transaction it waited on committed (EvalPlanQual),
/// so a winner that set `processing` makes the predicate stop matching and the rival updates
/// nothing. `rows_affected` is the whole answer — one row means this attempt got it, zero means
/// the invoice was not `ready`, was already held, or is not there at all, which to a caller are
/// the same fact.
///
/// No `SELECT … FOR UPDATE` ahead of it. The `UPDATE` takes the row lock itself, and an explicit
/// one would only guard the window between a check and a write that this shape does not have.
///
/// **The transaction exists for `SET LOCAL lock_timeout`, which is worth its three round trips.**
/// An `UPDATE` waits on a row lock for as long as its holder holds it — never on the predicate,
/// which it re-tests once and answers immediately. Every holder here is local work (`settle`,
/// `handlers::transition`; no transaction in this service spans a PSP call), so the wait is
/// normally sub-millisecond. The cap is for the case that is not normal: a backend wedged with an
/// open transaction would otherwise block claims indefinitely, each holding one of the pool's five
/// connections until the pool is gone and every request fails. Bounded, that becomes a `409` and a
/// returned connection. `LOCAL`, so the setting dies with the transaction rather than clinging to
/// the pooled connection for whatever request picks it up next.
///
/// Every early return drops the transaction, which rolls it back and releases the lock.
///
/// The three ways to lose are one fact to a caller and three different facts to whoever is reading
/// the log during an incident, so they are logged apart even though they answer alike. Winning is
/// `info`: it is the moment an invoice stops being chargeable by anyone else, and in a burst of
/// concurrent attempts it should appear exactly once.
#[instrument(skip_all, fields(invoice_id = %invoice_id, intent_id = %intent_id))]
pub(crate) async fn claim(
    pool: &PgPool,
    invoice_id: &str,
    intent_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *tx)
        .await?;

    // The intent this points at was committed by `record_intent` moments ago and nothing in
    // this service ever deletes one, so the foreign key cannot fail here. Should the row be
    // gone anyway — someone at a psql prompt — that is a broken invariant and deserves the
    // `500` the error path gives it, not a `409` telling the caller their invoice is unpayable.
    let claimed = sqlx::query(
        "UPDATE invoices
            SET state = 'processing', currently_processed_by_pi_id = $2
          WHERE id = $1::uuid
            AND state = 'ready'
            AND currently_processed_by_pi_id IS NULL",
    )
    .bind(invoice_id)
    .bind(intent_id)
    .execute(&mut *tx)
    .await;

    match claimed {
        Ok(result) if result.rows_affected() == 1 => {}
        // Not `ready`, or already being charged. Someone else has it.
        Ok(_) => {
            debug!("not claimed: the invoice is not ready, or another attempt holds it");
            return Ok(false);
        }
        // lock_not_available: another transaction held the row past the timeout. A conflict
        // with a change in flight, which for a charge means the same thing as losing outright.
        Err(err) if sqlstate(&err).as_deref() == Some("55P03") => {
            debug!("not claimed: the row was still locked after 100ms");
            return Ok(false);
        }
        Err(err) => return Err(err),
    }

    tx.commit().await?;
    info!(from = "ready", to = "processing", "invoice claimed");
    Ok(true)
}

/// Records how the charge ended and releases the invoice, returning it in its new state.
///
/// `Ok(None)` means there was nothing here to settle: the invoice has already moved on, or its
/// row could not be taken inside the timeout. Callers must not read that as failure — the
/// charge itself may well have succeeded, and settling it is somebody else's turn.
///
/// Two things this insists on beyond the obvious:
///
/// * The invoice must still point at *this* intent. `state = 'processing'` alone would let a
///   settlement land on a later attempt, which is reachable the moment the reconciler runs
///   alongside a live request.
/// * `invoices` is locked before `payment_intents`, never the other way round. This is the
///   only code that takes both, and a request and the reconciler both reach the two rows
///   through it, which is what makes a deadlock between them impossible rather than unlikely.
///
/// `Ok(None)` is `debug`, not `warn`. It is the reconciler and a live request arriving at the same
/// charge, which is the design working rather than anything to be woken up for.
#[instrument(skip_all, fields(invoice_id = %invoice_id, intent_id = %intent_id, outcome = ?outcome))]
pub(crate) async fn settle(
    pool: &PgPool,
    invoice_id: &str,
    intent_id: &str,
    outcome: Outcome,
) -> Result<Option<InvoiceRow>, sqlx::Error> {
    // Succeeded: the money moved, and the invoice is done. Failed: nothing moved, so the
    // invoice goes back to where it was and can be charged again with another card.
    let (status, state) = match outcome {
        Outcome::Succeeded => ("succeeded", InvoiceState::Processed),
        Outcome::Failed => ("failed", InvoiceState::Ready),
    };

    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *tx)
        .await?;

    let locked: Result<Option<(String,)>, _> = sqlx::query_as(
        "SELECT id::text FROM invoices
          WHERE id = $1::uuid
            AND state = 'processing'
            AND currently_processed_by_pi_id = $2
          FOR UPDATE",
    )
    .bind(invoice_id)
    .bind(intent_id)
    .fetch_optional(&mut *tx)
    .await;

    match locked {
        Ok(Some(_)) => {}
        Ok(None) => {
            debug!("nothing to settle: the invoice has already moved on");
            return Ok(None);
        }
        Err(err) if sqlstate(&err).as_deref() == Some("55P03") => {
            debug!("nothing to settle: the invoice row was still locked after 100ms");
            return Ok(None);
        }
        Err(err) => return Err(err),
    }

    // Classified exactly as the invoice lock above, and for the same reason: a row this
    // transaction could not take is a row somebody else is settling, which is not this
    // attempt's failure to report. Propagating it would answer one lock timeout with `500` and
    // the other with `504`, from a caller's point of view telling two different stories about
    // the same fact.
    let held: Result<Option<(String,)>, _> =
        sqlx::query_as("SELECT id FROM payment_intents WHERE id = $1 FOR UPDATE")
            .bind(intent_id)
            .fetch_optional(&mut *tx)
            .await;

    match held {
        Ok(_) => {}
        Err(err) if sqlstate(&err).as_deref() == Some("55P03") => {
            debug!("nothing to settle: the intent row was still locked after 100ms");
            return Ok(None);
        }
        Err(err) => return Err(err),
    }

    sqlx::query("UPDATE payment_intents SET status = $2::payment_intent_status WHERE id = $1")
        .bind(intent_id)
        .bind(status)
        .execute(&mut *tx)
        .await?;

    // The pointer is cleared in the same statement that leaves `processing`, so the invariant
    // "only a processing invoice names an intent" never has a window in which it is false.
    let row: InvoiceRow = sqlx::query_as(
        "UPDATE invoices
            SET state = $2, currently_processed_by_pi_id = NULL
          WHERE id = $1::uuid
      RETURNING id::text, customer_id::text, total_cents, due_date::text, state",
    )
    .bind(invoice_id)
    .bind(state)
    .fetch_one(&mut *tx)
    .await?;

    // Inside the transaction, so the event and the state change it describes commit together:
    // a settlement that rolls back tells nobody it happened, and one that commits cannot fail
    // to be announced because the process died a moment later.
    //
    // And inside *this function* rather than in `pay_invoice`, which is the reason `settle`
    // lives here at all. The daily reconciler settles through the same code, so a charge whose
    // response was lost emits its webhook when the reconciler closes it out — with no second
    // path to drift from this one.
    webhooks::enqueue(
        &mut tx,
        match outcome {
            Outcome::Succeeded => EventType::InvoicePaid,
            Outcome::Failed => EventType::InvoicePaymentFailed,
        },
        &json!({ "invoice": Invoice::from(row.clone()), "payment_intent_id": intent_id }),
    )
    .await?;

    tx.commit().await?;

    // After the commit, so this says the settlement happened rather than that it was attempted.
    // Both value changes, because they are two rows and a reader should not have to infer the
    // second from the first.
    info!(
        from = InvoiceState::Processing.as_str(),
        to = state.as_str(),
        intent_status = status,
        "invoice settled"
    );
    Ok(Some(row))
}
