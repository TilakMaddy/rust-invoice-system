use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;

use super::{Customer, CustomerRow, internal};

/// `GET /customers`
///
/// Every customer, oldest first. `id` is a uuidv7, whose leading bits are a timestamp, so
/// ordering by the primary key *is* ordering by creation time — no second column to sort on and
/// no index beyond the one the primary key already provides.
///
/// **Unpaginated, deliberately and only for now.** Nothing here caps the result, so the
/// response grows with the table and a large enough `customers` would be a slow query and a
/// large body. A `limit`/`offset` pair is the obvious next step; what would be worse than
/// leaving it out is capping silently, which turns "every customer" into "some customers" with
/// nothing in the response to say which.
pub async fn list_customers(State(pool): State<PgPool>) -> Response {
    let found: Result<Vec<CustomerRow>, _> =
        sqlx::query_as("SELECT id::text, name, email FROM customers ORDER BY id")
            .fetch_all(&pool)
            .await;

    match found {
        // A bare array, and an empty one when there are no customers — not an object wrapping
        // it, and not a 404. "No customers" is a successful answer to "which customers?".
        Ok(rows) => Json(
            rows.into_iter()
                .map(Customer::from)
                .collect::<Vec<Customer>>(),
        )
        .into_response(),
        Err(err) => internal(err, "listing the customers"),
    }
}
