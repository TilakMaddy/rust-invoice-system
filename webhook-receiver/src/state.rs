//! What every handler is given: the configured secrets, and what has arrived so far.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One delivery path and the key it is signed with.
///
/// A key per path, because the business signs each configured endpoint with its own secret. One
/// receiver serving two endpoints therefore holds two keys, and checking a delivery against the
/// wrong one is exactly the mistake this shape prevents.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Secret {
    pub path: String,
    pub secret: String,
}

/// Reads `WEBHOOK_SECRETS`.
pub fn parse(raw: &str) -> Result<Vec<Secret>, String> {
    let secrets: Vec<Secret> = serde_json::from_str(raw)
        .map_err(|err| format!("not a JSON array of {{path, secret}}: {err}"))?;
    for secret in &secrets {
        if !secret.path.starts_with('/') {
            return Err(format!("{}: expected a path beginning with /", secret.path));
        }
        if secret.secret.is_empty() {
            return Err(format!("{}: the signing secret is empty", secret.path));
        }
    }
    Ok(secrets)
}

/// One delivery as it arrived, in the shape `GET /received` answers with.
#[derive(Clone, Debug, Serialize)]
pub struct Received {
    pub webhook_id: String,
    pub r#type: String,
    pub attempt: u32,
    /// Whether the signature checked out. **A real receiver acts only on `true`.**
    pub verified: bool,
    /// Whether this `webhook_id` had already been seen. Delivery is at-least-once, so this is
    /// the flag a real receiver would branch on to skip work it has already done.
    pub duplicate: bool,
    /// Why verification failed, or `null`.
    pub rejected: Option<&'static str>,
    pub body: Value,
}

#[derive(Clone)]
pub struct AppState {
    secrets: Arc<HashMap<String, String>>,
    log: Arc<Mutex<Vec<Received>>>,
    /// Every `(path, webhook_id)` seen.
    ///
    /// Scoped by path, not by id alone: one process can serve several configured endpoints, and
    /// an event fans out to all of them. They are separate receivers that happen to share an
    /// address, so one endpoint's first delivery is not a duplicate of another's.
    ///
    /// In memory, so it empties on restart — the one way this harness differs from a receiver
    /// that would keep the record beside whatever the webhook updated.
    seen: Arc<Mutex<HashSet<(String, String)>>>,
}

impl AppState {
    pub fn new(secrets: Vec<Secret>) -> Self {
        Self {
            secrets: Arc::new(
                secrets
                    .into_iter()
                    .map(|configured| (configured.path, configured.secret))
                    .collect(),
            ),
            log: Arc::new(Mutex::new(Vec::new())),
            seen: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn secret(&self, path: &str) -> Option<&str> {
        self.secrets.get(path).map(String::as_str)
    }

    /// Records a delivery and says whether this endpoint had seen its id before.
    pub fn record(&self, path: &str, mut received: Received) -> bool {
        let duplicate = !self
            .lock(&self.seen)
            .insert((path.to_owned(), received.webhook_id.clone()));
        received.duplicate = duplicate;
        self.lock(&self.log).push(received);
        duplicate
    }

    pub fn received(&self) -> Vec<Received> {
        self.lock(&self.log).clone()
    }

    /// A poisoned lock means another request panicked mid-update. There is nothing to recover in
    /// a harness, so take the value as it is rather than propagating the panic.
    fn lock<'a, T>(&self, what: &'a Mutex<T>) -> std::sync::MutexGuard<'a, T> {
        what.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery(webhook_id: &str) -> Received {
        Received {
            webhook_id: webhook_id.to_owned(),
            r#type: "invoice.paid".to_owned(),
            attempt: 1,
            verified: true,
            duplicate: false,
            rejected: None,
            body: Value::Null,
        }
    }

    /// Two configured endpoints are two receivers that happen to share an address, so the same
    /// event arriving at each is not a redelivery to either. The same id twice on one path is.
    #[test]
    fn duplicates_are_scoped_to_the_path_they_arrived_on() {
        let state = AppState::new(Vec::new());

        assert!(!state.record("/webhooks", delivery("evt_1")));
        assert!(!state.record("/hooks", delivery("evt_1")));
        assert!(state.record("/webhooks", delivery("evt_1")));

        let log = state.received();
        assert_eq!(
            log.iter()
                .map(|received| received.duplicate)
                .collect::<Vec<_>>(),
            [false, false, true]
        );
    }
}
