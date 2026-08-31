use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::{error, internal, sqlstate};

#[derive(Deserialize)]
pub struct CreateCustomer {
    name: String,
    email: String,
}

#[derive(Serialize)]
struct Customer {
    id: String,
    name: String,
    email: String,
}

/// `POST /customers`
///
/// The email is stored as given beyond trimming, and its format is not validated: the column's
/// UNIQUE constraint is what this endpoint actually guarantees, and a homegrown pattern would
/// reject deliverable addresses while still admitting undeliverable ones.
pub async fn create_customer(
    State(pool): State<PgPool>,
    Json(request): Json<CreateCustomer>,
) -> Response {
    let name = request.name.trim();
    let email = request.email.trim();
    if name.is_empty() || email.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_customer");
    }

    // `id::text` rather than a uuid bind: the id is only ever echoed back as JSON, so casting
    // in SQL keeps a uuid decode feature out of the dependency list.
    let inserted: Result<(String,), _> =
        sqlx::query_as("INSERT INTO customers (name, email) VALUES ($1, $2) RETURNING id::text")
            .bind(name)
            .bind(email)
            .fetch_one(&pool)
            .await;

    match inserted {
        Ok((id,)) => (
            StatusCode::CREATED,
            Json(Customer {
                id,
                name: name.to_owned(),
                email: email.to_owned(),
            }),
        )
            .into_response(),
        // unique_violation on customers.email.
        Err(err) if sqlstate(&err).as_deref() == Some("23505") => {
            error(StatusCode::CONFLICT, "email_already_exists")
        }
        Err(err) => internal(err, "inserting the customer"),
    }
}
