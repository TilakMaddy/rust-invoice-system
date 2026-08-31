//! What every handler is given.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRef;
use sqlx::PgPool;

use crate::psp::Psp;
use crate::webhooks::Webhooks;

#[derive(Clone)]
pub struct AppState {
    pub(crate) pool: PgPool,
    pub(crate) psp: Psp,
    pub(crate) webhooks: Webhooks,
    pub(crate) api_token: ApiToken,
}

impl AppState {
    /// `psp_timeout` bounds every call to the payment service. Five seconds in production —
    /// long enough for the PSP's ~100ms charges and short enough that its 30 second one does
    /// not hold a request open — and milliseconds in tests.
    ///
    /// `webhook_timeout` bounds one delivery attempt the same way, and is separate rather than
    /// shared: ten seconds in production, because a receiver is a third party and deserves more
    /// room than the payment service, and a receiver being slow says nothing about the PSP.
    ///
    /// `api_token` is what the operator endpoints require in `X-API-Token`. It is taken as it
    /// is given: rejecting a weak one is a policy this service has no way to enforce well, and
    /// `main` already refuses to start without one at all.
    pub fn new(
        pool: PgPool,
        payment_service_url: &str,
        psp_timeout: Duration,
        webhook_timeout: Duration,
        api_token: &str,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            pool,
            psp: Psp::new(payment_service_url, psp_timeout)?,
            webhooks: Webhooks::new(webhook_timeout)?,
            api_token: ApiToken(api_token.into()),
        })
    }
}

/// The token the operator endpoints are gated on.
///
/// A newtype rather than a bare `String` so it cannot be passed where any other string is
/// wanted, and `Arc<str>` so cloning it per request costs a refcount rather than a copy of a
/// secret. The field is private to this module: `auth` compares it, nothing else reads it.
#[derive(Clone)]
pub struct ApiToken(Arc<str>);

impl ApiToken {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Lets the endpoints that only ever touch the database keep their `State<PgPool>`
/// extractor. Adding the PSP to the state is a change to how invoices get charged, and it has
/// no business rewriting the signature of every handler that does not charge anything.
impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> Self {
        state.pool.clone()
    }
}

/// The same trick for the auth layer, which needs the token and nothing else. Handing it the
/// whole `AppState` would give a middleware whose one job is a string comparison a database
/// pool and a payment client it has no business holding.
impl FromRef<AppState> for ApiToken {
    fn from_ref(state: &AppState) -> Self {
        state.api_token.clone()
    }
}
