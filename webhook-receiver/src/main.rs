use std::fmt::Display;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::str::FromStr;
use std::time::Duration;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = from_env("PORT", 3002)?;

    // What the container's HEALTHCHECK runs. Having the binary probe itself keeps the image
    // free of an HTTP client — one fewer package to keep patched. Blocking I/O is fine on
    // this path: the process does nothing else and exits immediately.
    if std::env::args().any(|arg| arg == "--health") {
        return health_probe(port);
    }

    // Loopback by default so a locally run receiver is not exposed to the network. The Docker
    // image overrides HOST to 0.0.0.0, where binding all interfaces means "all interfaces
    // inside the container".
    let host: IpAddr = from_env("HOST", Ipv4Addr::LOCALHOST.into())?;

    // Defaults to nothing configured, which means every delivery is recorded as unverified. That
    // is the honest failure: a receiver holding no key cannot tell a real webhook from anything
    // else that can reach it, and pretending otherwise is worse than saying so on every request.
    let secrets = webhook_receiver::parse(
        &std::env::var("WEBHOOK_SECRETS").unwrap_or_else(|_| String::from("[]")),
    )
    .map_err(|err| format!("invalid WEBHOOK_SECRETS: {err}"))?;

    let listener = TcpListener::bind(SocketAddr::new(host, port)).await?;
    println!(
        "webhook-receiver listening on http://{} with {} secret(s) configured",
        listener.local_addr()?,
        secrets.len()
    );

    let state = webhook_receiver::AppState::new(secrets);
    axum::serve(listener, webhook_receiver::app(state)).await?;
    Ok(())
}

/// A one-shot `GET /health` against the running server, over loopback so it works whatever
/// HOST is bound to. Exits non-zero unless the server answers `200`.
///
/// Deliberately a copy of the same function in the other two services. Extracting a shared crate
/// to hold twenty lines of blocking I/O would couple three build graphs together for less than
/// it costs.
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
