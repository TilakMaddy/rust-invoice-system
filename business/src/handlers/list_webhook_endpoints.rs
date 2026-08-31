use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use sqlx::PgPool;

use super::internal;

/// Every column an endpoint exposes, in the order the query lists them. Note what is missing.
type EndpointRow = (String, String, String, Option<String>);

/// The public shape of a registered endpoint.
///
/// **There is no `secret` field, and that is not redaction.** A redacted field tells a caller
/// there is a secret here and this is where it lives; the operator token is one shared string,
/// so anything it can read is readable by everyone who holds it. The secret is in the
/// deployment's configuration, which is the only place that needs it.
#[derive(Serialize)]
struct Endpoint {
    id: String,
    url: String,
    created_at: String,
    /// `null` while the endpoint is receiving. Set when `WEBHOOK_ENDPOINTS` stopped listing it:
    /// the row survives because its deliveries reference it, and the history of what was sent
    /// there is worth more than a tidy table.
    disabled_at: Option<String>,
}

impl From<EndpointRow> for Endpoint {
    fn from((id, url, created_at, disabled_at): EndpointRow) -> Self {
        Self {
            id,
            url,
            created_at,
            disabled_at,
        }
    }
}

/// `GET /webhook_endpoints`
///
/// What this process synced from `WEBHOOK_ENDPOINTS` at startup, oldest first.
///
/// Read-only, and there is no endpoint to write one: registration is configuration. Adding a
/// receiver is a change to the deployment, which is where a shared signing secret belongs and
/// where a change to who gets told about invoices is reviewable.
pub async fn list_webhook_endpoints(State(pool): State<PgPool>) -> Response {
    let found: Result<Vec<EndpointRow>, _> = sqlx::query_as(
        "SELECT id::text, url,
                to_char(created_at AT TIME ZONE 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                to_char(disabled_at AT TIME ZONE 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
           FROM webhook_endpoints
          ORDER BY id",
    )
    .fetch_all(&pool)
    .await;

    match found {
        Ok(rows) => Json(
            rows.into_iter()
                .map(Endpoint::from)
                .collect::<Vec<Endpoint>>(),
        )
        .into_response(),
        Err(err) => internal(err, "listing the webhook endpoints"),
    }
}
