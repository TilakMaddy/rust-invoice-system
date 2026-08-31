use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct AppState {
    intents: Arc<Mutex<HashMap<String, IntentState>>>,
}

impl AppState {
    pub fn create_intent(&self) -> String {
        let id = format!("pi_{}", Uuid::new_v4().simple());
        self.lock().insert(id.clone(), IntentState::Created);
        id
    }

    pub fn get(&self, id: &str) -> Option<IntentState> {
        self.lock().get(id).copied()
    }

    /// Claims the intent's one and only charge attempt.
    ///
    /// This is what makes the at-most-once guarantee hold under concurrency: the check and
    /// the transition to `Processing` happen under a single lock, so of two simultaneous
    /// pay requests exactly one can win.
    pub fn begin_charge(&self, id: &str) -> Result<(), BeginChargeError> {
        let mut intents = self.lock();
        let Some(state) = intents.get_mut(id) else {
            return Err(BeginChargeError::NotFound);
        };
        match *state {
            IntentState::Created => {
                *state = IntentState::Processing;
                Ok(())
            }
            IntentState::Processing => Err(BeginChargeError::InProgress),
            IntentState::Succeeded | IntentState::Failed(_) => Err(BeginChargeError::AlreadyPaid),
        }
    }

    /// Records the outcome of a charge previously claimed with [`AppState::begin_charge`].
    pub fn settle(&self, id: &str, outcome: IntentState) {
        if let Some(state) = self.lock().get_mut(id) {
            *state = outcome;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, IntentState>> {
        // A poisoned lock means another request panicked mid-update. There is nothing to
        // recover in a mock, so take the map as-is rather than propagating the panic.
        self.intents.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Why an intent could not be charged. Every variant means no charge was started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BeginChargeError {
    NotFound,
    InProgress,
    AlreadyPaid,
}

/// Internal state of a payment intent. `Created` and `Processing` are distinct here so a
/// second pay request can be told apart from a first, but both are reported as `pending`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentState {
    Created,
    Processing,
    Succeeded,
    Failed(FailureCode),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    InsufficientFunds,
    CardDeclined,
}

/// The three statuses the API exposes. `Processing` is deliberately not leaked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentStatus {
    Pending,
    Succeeded,
    Failed,
}

/// The public shape of a payment intent, shared by all three endpoints.
#[derive(Debug, Serialize)]
pub struct PaymentView {
    pub payment_intent_id: String,
    pub status: PaymentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<FailureCode>,
}

impl PaymentView {
    pub fn new(payment_intent_id: impl Into<String>, state: IntentState) -> Self {
        let (status, code) = match state {
            IntentState::Created | IntentState::Processing => (PaymentStatus::Pending, None),
            IntentState::Succeeded => (PaymentStatus::Succeeded, None),
            IntentState::Failed(code) => (PaymentStatus::Failed, Some(code)),
        };
        Self {
            payment_intent_id: payment_intent_id.into(),
            status,
            code,
        }
    }
}
