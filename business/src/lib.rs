mod auth;
mod handlers;
mod idempotency;
mod payments;
mod psp;
mod state;

pub mod jobs;
pub mod logging;
pub mod webhooks;

pub use state::AppState;

use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{get, post};

/// The state is a struct rather than the bare pool because charging an invoice needs the
/// payment service as well. Every other endpoint reads or writes the database and nothing else,
/// and still extracts a `PgPool`, via the `FromRef` impl in `state`.
pub fn app(state: AppState) -> Router {
    // What the business itself operates, behind `X-API-Token`. Grouped rather than layered
    // seven times over, so that which audience a route belongs to is a property of where it is
    // written down and not of a line that can be forgotten when the eighth is added.
    let operator = Router::new()
        .route(
            "/customers",
            post(handlers::create_customer).get(handlers::list_customers),
        )
        .route("/customers/{id}", get(handlers::get_customer))
        .route(
            "/invoices",
            post(handlers::create_invoice).get(handlers::list_invoices),
        )
        .route("/invoices/{id}", get(handlers::invoice_status))
        .route("/invoices/{id}/draft", post(handlers::draft_invoice))
        .route("/invoices/{id}/ready", post(handlers::ready_invoice))
        .route("/invoices/{id}/void", post(handlers::void_invoice))
        // The webhook surface is three reads and no writes. Registration is configuration, so
        // there is nothing here to create an endpoint with — see `webhooks::sync`.
        .route("/webhook_endpoints", get(handlers::list_webhook_endpoints))
        .route(
            "/webhook_deliveries",
            get(handlers::list_webhook_deliveries),
        )
        // The business's own event log, and its way of catching up on anything a receiver
        // missed. Operator-gated like the rest of the back office: it carries every invoice
        // this service has ever raised, which is strictly more than any one payer may see.
        .route("/events", get(handlers::list_events))
        // `route_layer`, not `layer`: a path matching none of the above is a `404`, and there
        // is nothing there to protect. Answering it `401` would also tell an unauthenticated
        // caller which paths exist.
        .route_layer(from_fn_with_state(state.clone(), auth::require_api_token));

    Router::new()
        // Open, and deliberately so — see the module docs in `handlers::docs`. A spec does
        // reveal which paths exist, which is the one thing `route_layer` above is careful not
        // to; documentation nobody can reach is the worse trade.
        .route("/openapi.yaml", get(handlers::openapi_spec))
        // Swagger UI's own assets, embedded in the binary by `utoipa-swagger-ui`'s `vendored`
        // feature, pointed at the spec route above.
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs")
                .config(utoipa_swagger_ui::Config::new(["/openapi.yaml"])),
        )
        .merge(operator)
        // The payer's two endpoints. Whoever is being billed has no token and cannot be given
        // one, so these are open by necessity — which is why neither of them exposes anything
        // about an invoice beyond the charge the caller is already making.
        .route(
            "/payment_intents/{id}",
            get(handlers::payment_intent_status),
        )
        .route(
            "/invoices/{id}/pay",
            post(handlers::pay_invoice)
                .route_layer(from_fn_with_state(state.clone(), idempotency::idempotent)),
        )
        // `layer`, and the note above is the reason why rather than the exception to it: `auth`
        // uses `route_layer` because a path that matched nothing has nothing to protect, and this
        // uses `layer` because a path that matched nothing is still a request worth recording.
        //
        // Added after every route, so it wraps both of the middlewares above and sees what they
        // answer: the `401`s from `auth`, and the replays and `409`s from `idempotency`. Those
        // never reach a handler, so nothing else in this crate is in a position to log them.
        .layer(from_fn(logging::trace_requests))
        // Open, and it has to be: the container's HEALTHCHECK runs this binary with `--health`,
        // which writes a bare `GET /health` over a socket and carries no headers at all.
        .route("/health", get(handlers::health))
        .with_state(state)
}

/// The SQLSTATE a statement was rejected with, or `None` for a client-side failure (a dead
/// pool, a decode error). Callers match on it to turn a constraint the schema already enforces
/// into a status code, rather than pre-checking with a SELECT that another transaction could
/// invalidate between the check and the insert.
///
/// At the crate root because the charge transactions in `payments` need it as much as the
/// handlers do.
pub(crate) fn sqlstate(err: &sqlx::Error) -> Option<String> {
    Some(err.as_database_error()?.code()?.into_owned())
}
