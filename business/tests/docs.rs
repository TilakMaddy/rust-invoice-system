//! The published API description, and the viewer that renders it.
//!
//! The interesting test here is the last one. Serving a spec is easy; serving a spec that still
//! describes the service is the part that rots, so the routes it documents are checked against
//! the router that answers them.
//!
//! ```sh
//! docker compose up -d postgres    # from the repo root
//! cargo test --test docs           # from business/
//! ```

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use business::AppState;
use http_body_util::BodyExt;
use sqlx::PgPool;
use tower::ServiceExt;

const TOKEN: &str = "test-token";

fn app(pool: PgPool) -> Router {
    business::app(
        AppState::new(
            pool,
            "http://127.0.0.1:1",
            Duration::from_millis(200),
            Duration::from_millis(200),
            TOKEN,
        )
        .unwrap(),
    )
}

/// Status, `content-type` and body as text. Not JSON, unlike every other suite's helper: what
/// these two endpoints serve is YAML and HTML.
async fn get(app: &Router, path: &str) -> (StatusCode, String, String) {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, content_type, String::from_utf8_lossy(&bytes).into())
}

/// Open, like `/health`. A spec behind the token it documents is a spec nobody reads.
#[sqlx::test]
async fn the_spec_is_served_without_a_token(pool: PgPool) {
    let (status, content_type, body) = get(&app(pool), "/openapi.yaml").await;

    assert_eq!(status, StatusCode::OK);
    // Not `text/yaml` and not `application/octet-stream`: a browser shows the first and
    // downloads the last.
    assert!(
        content_type.starts_with("application/yaml"),
        "served as {content_type}"
    );
    assert!(body.starts_with("openapi: 3.1.0"), "not the spec");
}

/// Byte for byte the file in the repo, which `include_str!` guarantees at compile time. Asserted
/// anyway, because what it really pins is that the *file the Dockerfile copies* is the file the
/// binary serves — the two are separate lines in separate places and only agree on purpose.
#[sqlx::test]
async fn the_spec_served_is_the_spec_in_the_repo(pool: PgPool) {
    let (_, _, body) = get(&app(pool), "/openapi.yaml").await;

    assert_eq!(body, include_str!("../openapi.yaml"));
}

/// Swagger UI is mounted at `/docs`, which redirects to `/docs/` — its assets are relative, so
/// the trailing slash is what makes them resolve.
#[sqlx::test]
async fn the_viewer_redirects_to_its_index(pool: PgPool) {
    let (status, _, _) = get(&app(pool), "/docs").await;

    assert_eq!(status, StatusCode::SEE_OTHER);
}

#[sqlx::test]
async fn the_viewer_is_served_without_a_token(pool: PgPool) {
    let (status, content_type, body) = get(&app(pool), "/docs/").await;

    assert_eq!(status, StatusCode::OK);
    assert!(content_type.starts_with("text/html"), "{content_type}");

    // **No CDN.** The whole point of paying for `utoipa-swagger-ui`'s vendored assets is that
    // the viewer works on a machine with no route to the internet, so every asset the page
    // pulls must be a relative path served by this binary.
    assert!(
        !body.contains("http://") && !body.contains("https://"),
        "the viewer reaches off-box: {body}"
    );
}

/// The UI has to be pointed at *this* service's spec, and the bundle has to actually be there —
/// a viewer that loads and then cannot find its spec is a blank page with no error.
#[sqlx::test]
async fn the_viewer_is_wired_to_the_spec_this_service_serves(pool: PgPool) {
    let app = app(pool);

    let (status, _, initializer) = get(&app, "/docs/swagger-initializer.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        initializer.contains(r#""url": "/openapi.yaml""#),
        "{initializer}"
    );

    let (status, content_type, bundle) = get(&app, "/docs/swagger-ui-bundle.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(content_type.contains("javascript"), "{content_type}");
    assert!(bundle.len() > 100_000, "bundle is {} bytes", bundle.len());
}

/// The spec's `servers:` URL must stay **relative**.
///
/// This is not style. That URL is the origin Swagger UI's "Try it out" sends to, and a browser
/// counts `localhost` and `127.0.0.1` as different origins — so an absolute one means the viewer
/// works when reached on the host it happens to name and silently fails everywhere else, with a
/// bare `TypeError` and no status code to explain it. A relative URL resolves against whatever
/// host the page was loaded from, which is always this binary.
#[sqlx::test]
async fn the_spec_targets_whatever_host_serves_it(pool: PgPool) {
    let (_, _, spec) = get(&app(pool), "/openapi.yaml").await;

    let servers = spec
        .split("\nservers:")
        .nth(1)
        .expect("the spec has a servers: block");
    let urls: Vec<&str> = servers
        .lines()
        .take_while(|line| line.starts_with(' ') || line.starts_with('#') || line.trim().is_empty())
        .filter_map(|line| line.trim().strip_prefix("- url:"))
        .map(str::trim)
        .collect();

    assert!(!urls.is_empty(), "no server url found in:\n{servers}");
    for url in urls {
        assert!(
            url.starts_with('/'),
            "server url {url} is absolute; \"Try it out\" will break off that host"
        );
    }
}

/// **Every path the spec documents is a path this service routes.**
///
/// A description that lists an endpoint the router does not have is worse than no description:
/// it sends a caller to build against something that answers `404`. This walks the `paths:` block
/// and calls each one.
///
/// The discriminator is that axum answers an unrouted path with a `404` and an *empty* body,
/// while a handler's own "not found" carries `{"error": ...}`. So a `404` with a body means the
/// route exists and the fixture-less database simply had nothing to return, which is fine here —
/// this is a test about routing, not about data.
///
/// Parsed by scanning lines rather than with a YAML crate: the `paths:` block is two levels of
/// plain keys, and a parser dependency for that would cost more than it explains.
#[sqlx::test]
async fn every_documented_path_exists(pool: PgPool) {
    let app = app(pool);
    let spec = include_str!("../openapi.yaml");

    let mut documented = Vec::new();
    let mut in_paths = false;
    for line in spec.lines() {
        if line.starts_with("paths:") {
            in_paths = true;
            continue;
        }
        // The next top-level key ends the block.
        if in_paths && !line.starts_with(' ') && !line.trim().is_empty() {
            break;
        }
        // Exactly two spaces of indent, starting with a slash: `  /invoices/{id}/pay:`
        if in_paths && line.starts_with("  /") && line.ends_with(':') {
            documented.push(line.trim().trim_end_matches(':').to_owned());
        }
    }
    assert!(
        documented.len() >= 13,
        "only found {} paths — the scan broke, not the spec",
        documented.len()
    );

    for path in &documented {
        // Any syntactically valid uuid will do; whether the row exists is not what is being
        // tested, and a handler that answers `invoice_not_found` has already proved it is routed.
        let concrete = path.replace("{id}", "00000000-0000-4000-8000-000000000001");
        let (status, _, body) = get(&app, &concrete).await;

        assert!(
            !(status == StatusCode::NOT_FOUND && body.is_empty()),
            "{path} is documented but not routed"
        );
    }
}
