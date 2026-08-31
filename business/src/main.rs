use std::fmt::Display;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = from_env("PORT", 3001)?;

    // What the container's HEALTHCHECK runs. Having the binary probe itself keeps the image
    // free of an HTTP client — one fewer package to keep patched. Blocking I/O is fine on
    // this path: the process does nothing else and exits immediately.
    if std::env::args().any(|arg| arg == "--health") {
        return health_probe(port);
    }

    // After the probe, so the healthcheck the container runs every ten seconds stays silent, and
    // before everything else, so every line below this one goes through the same subscriber.
    business::logging::init();

    // Loopback by default so a locally run server is not exposed to the network. The Docker
    // image overrides HOST to 0.0.0.0, where binding all interfaces means "all interfaces
    // inside the container".
    let host: IpAddr = from_env("HOST", Ipv4Addr::LOCALHOST.into())?;

    // No default, unlike HOST and PORT. Every fallback would be a guess at which database
    // holds the invoices, and a service that quietly reads and writes the wrong one is a far
    // worse failure than one that will not start.
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is not set: expected a postgres:// connection string")?;

    // No default, for the same reason DATABASE_URL has none: every fallback would be a guess
    // at which payment service gets charged, and quietly charging the wrong one is worse than
    // refusing to start.
    let payment_service_url = std::env::var("PAYMENT_SERVICE_URL")
        .map_err(|_| "PAYMENT_SERVICE_URL is not set: expected the payment service's base URL")?;

    // No default, for the same reason the two above have none, and more sharply: a fallback
    // token would be a token everyone knows, which is authentication that only looks like it is
    // there. Empty is refused too — it would let every request carrying an empty header past.
    let api_token = std::env::var("BUSINESS_API_KEY")
        .ok()
        .filter(|token| !token.is_empty())
        .ok_or("BUSINESS_API_KEY is not set: expected the token the operator endpoints require")?;

    // A default, unlike the three above, and it is the honest one: no endpoints configured means
    // no webhooks delivered. That is not a guess at anything — events are still recorded, so a
    // receiver added later catches up through `GET /events` and nothing was lost by starting
    // without one. A *malformed* value is still fatal, as every malformed value here is.
    let endpoints = business::webhooks::parse(
        &std::env::var("WEBHOOK_ENDPOINTS").unwrap_or_else(|_| String::from("[]")),
    )
    .map_err(|err| format!("invalid WEBHOOK_ENDPOINTS: {err}"))?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await?;

    // Before the listener binds, so an unmigrated schema can never serve a request. sqlx holds
    // a Postgres advisory lock while it works, so several replicas booting at once is safe:
    // one applies the migrations and the rest wait, then find nothing left to do.
    sqlx::migrate!().run(&pool).await?;
    tracing::info!("migrations applied");

    // Also before the listener binds, so no request is ever served against a stale endpoint set:
    // an invoice raised in the first millisecond of this process fans out to exactly what this
    // deployment configures, not to what the last one did.
    business::webhooks::sync(&pool, &endpoints).await?;

    // Five seconds for the PSP: comfortably past its ~100ms charges, and well short of the 30
    // second one, which no request should be held open for. Ten for a webhook, because a
    // receiver is somebody else's server and nothing is waiting on its answer.
    let state = business::AppState::new(
        pool,
        &payment_service_url,
        Duration::from_secs(5),
        Duration::from_secs(10),
        &api_token,
    )?;

    // Before the listener binds, so the first reconciler pass runs against invoices left in
    // flight by a previous process rather than waiting behind traffic, and the dispatcher starts
    // draining whatever the outbox still owes. Detached: these outlive every request and are
    // never awaited.
    business::jobs::spawn(state.clone());

    let listener = TcpListener::bind(SocketAddr::new(host, port)).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");

    axum::serve(listener, business::app(state)).await?;
    Ok(())
}

/// A one-shot `GET /health` against the running server, over loopback so it works whatever
/// HOST is bound to. Exits non-zero unless the server answers `200`.
///
/// Deliberately a copy of the same function in mock-payment-service. Extracting a shared crate
/// to hold twenty lines of blocking I/O would couple the two services' build graphs together
/// for less than it costs.
fn health_probe(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(2);
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;

    let mut status_line = String::new();
    BufReader::new(stream).read_line(&mut status_line)?;

    // "HTTP/1.1 200 OK" — only the middle token matters, so the check does not care which
    // HTTP version the server answered with.
    match status_line.split_whitespace().nth(1) {
        Some("200") => Ok(()),
        _ => Err(format!("unhealthy: {}", status_line.trim()).into()),
    }
}

/// Reads `key`, falling back to `default` when it is unset.
///
/// A malformed value is a hard error rather than a silent fallback: a container that quietly
/// binds the wrong interface is far harder to diagnose than one that refuses to start.
fn from_env<T>(key: &str, default: T) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(raw) => raw
            .parse()
            .map_err(|e| format!("invalid {key}={raw:?}: {e}")),
    }
}
