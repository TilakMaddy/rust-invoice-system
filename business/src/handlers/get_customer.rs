use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;

use super::{Customer, CustomerRow, error, internal};
use crate::sqlstate;

/// `GET /customers/{id}`
pub async fn get_customer(State(pool): State<PgPool>, Path(id): Path<String>) -> Response {
    // `22P02` is what turns a path segment that is not a uuid into a 400 without a uuid parser
    // in this crate, exactly as the invoice endpoints do it.
    let found: Result<Option<CustomerRow>, _> =
        sqlx::query_as("SELECT id::text, name, email FROM customers WHERE id = $1::uuid")
            .bind(&id)
            .fetch_optional(&pool)
            .await;

    match found {
        Ok(Some(row)) => Json(Customer::from(row)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "customer_not_found"),
        Err(err) if sqlstate(&err).as_deref() == Some("22P02") => {
            error(StatusCode::BAD_REQUEST, "invalid_customer_id")
        }
        Err(err) => internal(err, "reading the customer"),
    }
}
