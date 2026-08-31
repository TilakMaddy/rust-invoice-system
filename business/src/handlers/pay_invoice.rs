use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use super::{error, internal};
use crate::payments::{self, InvoiceState, Outcome};
use crate::psp::Charge;
use crate::sqlstate;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct PayInvoice {
    card_token: String,
}

/// `POST /invoices/{id}/pay`
///
/// Charges a `ready` invoice once, and records what happened. Four phases, in this order for
/// reasons that do not survive rearranging:
///
/// 0. Check the invoice exists and looks payable, touching nothing. This is what keeps a
///    mistyped id from minting payment intents at the PSP. It is not authoritative — phase 2
///    decides — but it is free and it bounds the damage a bad caller can do.
/// 1. Ask the PSP for an intent and record it as `pending`. The PSP owns the id, so this
///    cannot be deferred until after the invoice is claimed.
/// 2. Claim the invoice under a row lock, pointing it at that intent. Exactly one concurrent
///    attempt can win this, and only from `ready`.
/// 3. Charge, then record the outcome — or, if the PSP never gave one, record nothing at all.
///
/// **An unresolved charge is left unresolved.** A timeout or a dropped connection means the
/// card may already have been charged, so the invoice stays in `processing` still pointing at
/// its intent, and the caller is told as much. The alternative — assuming a decline — releases
/// the invoice and lets the same money be taken twice.
///
/// Every answer that has an intent behind it carries that intent's id, and the status code says
/// how the charge ended. The `504` needs the id most, because `GET /payment_intents/{id}` is
/// then the only way left to find out what actually happened — the daily reconciler will get
/// there eventually, but the caller should not have to wait a day to ask.
///
/// **Every failure names itself.** `200` is the only answer here without an `error`, because it
/// is the only one that is not a failure. A caller parses `error` the same way on this endpoint
/// as on every other, and there is no case to special-case.
///
/// What the codes do *not* carry is the PSP's vocabulary. It separates `insufficient_funds` from
/// `card_declined`; both answer `charge_declined`, so no caller ends up depending on a taxonomy
/// that is really between the cardholder and their bank. `charge_unresolved` is likewise the
/// honest width of what is known: not that the charge failed — it may well have succeeded — but
/// that this service cannot say, and the id is how the caller finds out.
pub async fn pay_invoice(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<PayInvoice>,
) -> Response {
    // Phase 0. `22P02` here is what turns a path segment that is not a uuid into a 400 without
    // a uuid parser in this crate, exactly as the other invoice endpoints do it.
    let existing: Result<Option<(InvoiceState,)>, _> =
        sqlx::query_as("SELECT state FROM invoices WHERE id = $1::uuid")
            .bind(&id)
            .fetch_optional(&state.pool)
            .await;

    match existing {
        Ok(Some((InvoiceState::Ready,))) => {}
        // Draft, void, already paid, or already being charged. Phase 2 would reject it too;
        // rejecting here just avoids an intent nobody will ever use.
        Ok(Some(_)) => return error(StatusCode::CONFLICT, "invoice_not_payable"),
        Ok(None) => return error(StatusCode::NOT_FOUND, "invoice_not_found"),
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            return error(StatusCode::BAD_REQUEST, "invalid_invoice_id");
        }
        Err(err) => return internal(err, "reading the invoice before charging it"),
    }

    // Phase 1. Nothing has been charged and no invoice has moved, so an unreachable PSP is
    // simply a request that did not happen.
    let intent_id = match state.psp.create_intent().await {
        Ok(id) => id,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "payment_service_unavailable"),
    };
    if let Err(err) = payments::record_intent(&state.pool, &id, &intent_id).await {
        return internal(err, "recording the payment intent");
    }

    // Phase 2. Losing covers every way this attempt can fail to get the invoice — it moved out
    // of `ready`, another attempt already holds it, or its row could not be taken inside the
    // lock timeout — because to a caller they are the same fact: no charge was made.
    match payments::claim(&state.pool, &id, &intent_id).await {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::CONFLICT, "invoice_not_payable"),
        Err(err) => return internal(err, "claiming the invoice for this charge"),
    }

    // Phase 3.
    let outcome = match state.psp.pay(&intent_id, &request.card_token).await {
        Charge::Succeeded => Outcome::Succeeded,
        Charge::Failed => Outcome::Failed,
        Charge::Unknown => {
            return attempt(
                StatusCode::GATEWAY_TIMEOUT,
                Some("charge_unresolved"),
                &intent_id,
            );
        }
    };

    match payments::settle(&state.pool, &id, &intent_id, outcome).await {
        Ok(Some(_)) => match outcome {
            Outcome::Succeeded => attempt(StatusCode::OK, None, &intent_id),
            // A decline is not a transition that succeeded, and a `200` invites a caller to
            // read it as one. The id rides along with the error exactly as it does on the
            // `500`: the charge is over and the caller can act on that, and the handle is
            // still how they read back where it landed.
            Outcome::Failed => attempt(
                StatusCode::PAYMENT_REQUIRED,
                Some("charge_declined"),
                &intent_id,
            ),
        },
        // The outcome could not be recorded: the reconciler got there first, or the row could
        // not be locked in time. Reported as unknown rather than as a conflict, because
        // `invoice_not_payable` would be a lie to someone whose charge this service just ran.
        // The id goes back because following it is how the caller sees where it landed.
        Ok(None) => attempt(
            StatusCode::GATEWAY_TIMEOUT,
            Some("charge_unresolved"),
            &intent_id,
        ),

        // This service's own database failed, which is not the same fact as the payment service
        // failing to answer, and must not be reported as one: a `504` here sends whoever is on
        // call to look at the PSP during an incident that is ours. The state left behind is the
        // same as above — the transaction rolled back, so the invoice is still `processing` and
        // still pointing at this intent, and the reconciler will resolve it — but what broke is
        // different, and the status code is what says so.
        //
        // Note what is *not* reported: `outcome`. This request knows exactly how the charge
        // ended — the PSP answered, or it would not have got here — and says nothing about it,
        // because nothing was written down. Reporting it would put this response at odds with
        // both read endpoints, which go on saying `processing` and `pending` until the
        // reconciler catches up. On a decline it would be worse than vague: "try another card"
        // while the invoice is still held, and the retry that follows gets `409`.
        //
        // So the answer is the failure and the handle. The id rides along with the error rather
        // than replacing it: following it is how the caller watches the record catch up, and
        // dropping `error` would make this the one failure they cannot parse like the others.
        //
        // Not `internal`, which the rest of this handler uses: that answers a bare `internal_error`
        // and this one has to carry the intent id for the reason spelled out above. The cause is
        // written down the same way regardless — it is the same message, and this `500` is no less
        // worth diagnosing for having a handle attached to it.
        Err(err) => {
            tracing::error!(%err, failed = "settling the charge this request ran", "internal error");
            attempt(
                StatusCode::INTERNAL_SERVER_ERROR,
                Some("internal_error"),
                &intent_id,
            )
        }
    }
}

/// Every answer that has an intent behind it: the id, and the failure's name when there is one.
///
/// `error` is `None` only on the `200`, where there is nothing to name. Everything else this
/// endpoint can answer once a charge has reached the PSP is a failure, and names itself, so a
/// caller parses this endpoint exactly like the rest of the API.
///
/// The id is always there, and is not the invoice: a caller that wants the invoice can read it,
/// but the handle to *this attempt* is available nowhere else — and on `charge_unresolved` it is
/// the only thing standing between the caller and never finding out whether the card was
/// charged.
fn attempt(status: StatusCode, error: Option<&str>, intent_id: &str) -> Response {
    let body = match error {
        Some(error) => json!({ "error": error, "payment_intent_id": intent_id }),
        None => json!({ "payment_intent_id": intent_id }),
    };
    (status, Json(body)).into_response()
}
