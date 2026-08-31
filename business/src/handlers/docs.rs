//! The API description this service publishes.
//!
//! The viewer that renders it is Swagger UI, whose assets `utoipa-swagger-ui` embeds in the
//! binary; `lib.rs` mounts it at `/docs` and points it here. This module owns only the spec.
//!
//! Both are **open**, like `/health` and unlike everything else the operator touches. That is a
//! deliberate exception to the reasoning in `lib.rs`, which keeps the auth layer off unmatched
//! paths so a `401` cannot tell an unauthenticated caller which paths exist: a published spec
//! tells them exactly that, in detail. The trade is accepted because documentation nobody can
//! reach is documentation nobody reads, and because the spec describes the *shape* of the API
//! rather than granting any access to it — every operator endpoint it lists still answers `401`.
//! A deployment that would rather not publish it should gate these two routes, which is one
//! `route_layer` and no other change.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

/// The spec, read at *compile* time and embedded in the binary.
///
/// The same trick `sqlx::migrate!()` plays with the migrations directory, and for the same
/// reason: the runtime image is distroless — the binary and glibc, no shell and no other files —
/// so a spec read from disk at startup would be a file that is simply not there. Embedding also
/// means the description cannot drift from the build it documents, and that there is no
/// not-found path to handle.
const SPEC: &str = include_str!("../../openapi.yaml");

/// `GET /openapi.yaml`
///
/// Served as `application/yaml` (registered with IANA in 2024), so a browser shows it rather
/// than downloading it, and `curl | yq` works without a flag.
pub async fn openapi_spec() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/yaml; charset=utf-8")],
        SPEC,
    )
        .into_response()
}
