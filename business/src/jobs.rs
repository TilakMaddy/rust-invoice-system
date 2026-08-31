//! The background loops, spawned at startup and detached from every request.
//!
//! Two of them, on very different clocks and for the same underlying reason: work that must
//! happen but must not happen inside a request. The reconciler runs daily and closes out charges
//! whose answer was lost; the webhook dispatcher runs every second and delivers what the outbox
//! owes. Neither is reachable over HTTP.
//!
//! ## The reconciler
//!
//! It exists because a charge can end without an answer: a request that never heard back from
//! the PSP writes nothing, leaving an invoice in `processing` that only the PSP can resolve.
//! Not reachable over HTTP — this is recovery, not an endpoint.
//!
//! It is also the *only* thing that resolves one. `GET /payment_intents/{id}` deliberately
//! reports the recorded row and does not settle, so that a read has no side effect and no
//! dependency on the PSP being up. The cost of that choice is paid here: until this runs, an
//! unresolved charge stays unresolved, which is why the first tick is at startup.
//!
//! Nothing collects the intents left behind by attempts that never got started. An attempt
//! that records its intent and then loses the claim leaves a `pending` row that no invoice
//! points at, and it stays: it is an accurate record that this service asked the PSP for an
//! intent and never charged it. Deleting it would buy a tidier table at the cost of a job that
//! has to tell an abandoned intent from one a request is a millisecond away from claiming —
//! a distinction the rows do not carry, and getting it wrong fails a charge that was fine.

use std::time::Duration;

use tokio::time::{Interval, MissedTickBehavior, interval};
use tracing::{error, info, instrument, warn};

use crate::payments::{self, Outcome};
use crate::psp::PaymentStatus;
use crate::state::AppState;

pub use crate::webhooks::delivery::deliver_once;

const DAILY: Duration = Duration::from_secs(24 * 60 * 60);

/// How often the dispatcher asks whether anything is due.
///
/// A second, so a webhook follows the change that caused it closely enough to feel immediate,
/// and an idle tick costs one indexed lookup against a partial index holding only the backlog.
/// `LISTEN`/`NOTIFY` would remove even that, at the price of a dedicated connection and a
/// fallback poll anyway for the retries a notification cannot wake — not worth it here.
const DISPATCH: Duration = Duration::from_secs(1);

/// Starts both loops. They own their state and are never awaited, so they outlive any request
/// and are unaffected by a client that disconnects mid-charge.
///
/// Separate tasks rather than one loop doing both: a daily sweep of the PSP and a per-second
/// drain of the outbox have nothing in common but the fact that no request is waiting, and
/// putting them on one timer would mean choosing whose interval to get wrong.
pub fn spawn(state: AppState) {
    let reconciler = state.clone();
    tokio::spawn(async move {
        let mut ticks = daily();
        loop {
            ticks.tick().await;
            reconcile_once(&reconciler).await;
        }
    });

    tokio::spawn(async move {
        let mut ticks = every(DISPATCH);
        loop {
            ticks.tick().await;
            deliver_once(&state).await;
        }
    });
}

/// A day, with the first tick landing immediately.
///
/// Firing at startup is the point rather than an accident of `interval`: a process that died
/// mid-charge comes back with invoices stuck in `processing`, and they should be resolved on
/// boot rather than up to a day later.
fn daily() -> Interval {
    every(DAILY)
}

/// A timer that fires immediately and then every `period`.
///
/// `Delay` keeps a pass that ran long from being followed by a burst of catch-up ticks — which
/// matters most for the dispatcher: a batch that took three seconds must not be answered with
/// three immediate passes, each claiming rows the last one is still delivering.
fn every(period: Duration) -> Interval {
    let mut ticks = interval(period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks
}

/// Asks the PSP how every unresolved charge actually ended, and records it.
///
/// Errors are logged and skipped rather than propagated: one invoice the PSP will not answer
/// for must not stop the other invoices from being reconciled, and nothing here is worth
/// killing the task over — the next pass tries again.
#[instrument(skip_all, name = "reconciler")]
pub async fn reconcile_once(state: &AppState) {
    let in_flight: Result<Vec<(String, String)>, _> = sqlx::query_as(
        "SELECT id::text, currently_processed_by_pi_id FROM invoices
          WHERE state = 'processing' AND currently_processed_by_pi_id IS NOT NULL",
    )
    .fetch_all(&state.pool)
    .await;

    let in_flight = match in_flight {
        Ok(rows) => rows,
        Err(err) => return error!(%err, "could not list the invoices in flight"),
    };

    for (invoice_id, intent_id) in in_flight {
        let outcome = match state.psp.status(&intent_id).await {
            Ok(Some(PaymentStatus::Succeeded)) => Outcome::Succeeded,
            Ok(Some(PaymentStatus::Failed)) => Outcome::Failed,

            // Still being charged, or charged and not yet settled. The PSP guarantees a
            // terminal state within 24 hours, so a daily pass finds it settled next time.
            Ok(Some(PaymentStatus::Pending)) => continue,

            // The PSP has no record of an intent we know it issued — its state is in memory
            // and does not survive a restart. There is nothing safe to infer: it may have been
            // charged before the restart. Left in `processing` for a human, because guessing
            // either way is how an invoice gets charged twice or written off unpaid.
            Ok(None) => {
                warn!(
                    invoice_id,
                    intent_id, "left in flight: the intent is unknown to the payment service"
                );
                continue;
            }

            Err(err) => {
                warn!(%err, intent_id, "could not read the intent");
                continue;
            }
        };

        match payments::settle(&state.pool, &invoice_id, &intent_id, outcome).await {
            // Someone settled it first — a live request finishing the charge it started, most
            // likely. Not an error: the outcome is recorded either way.
            Ok(None) => {}
            // `settle` logs the transition itself, under this span. What it cannot say is that
            // the reconciler is what closed it out rather than a request that came back in time.
            Ok(Some(_)) => info!(invoice_id, "recovered an invoice left in flight"),
            Err(err) => error!(%err, invoice_id, "could not settle the invoice"),
        }
    }
}
