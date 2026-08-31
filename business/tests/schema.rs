//! Schema-level tests over the seed fixtures.
//!
//! `#[sqlx::test]` provisions a fresh database per test, applies `migrations/`, then executes
//! the named fixtures from `tests/fixtures/` in the order given. Nothing is shared between
//! tests, so there is no cleanup step and no ordering dependency between them.
//!
//! These use the runtime `sqlx::query` API rather than the `query!` macros on purpose: the
//! macros verify SQL against a live database at *compile* time, which would make `cargo build`
//! require either a reachable Postgres or a committed `.sqlx/` cache. Nothing here needs that.
//!
//! Running them needs Postgres up — `DATABASE_URL` is read from `business/.env` by dotenvy:
//!
//! ```sh
//! docker compose up -d postgres    # from the repo root
//! cargo test                       # from business/
//! ```

use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{PgPool, Postgres};

const ADA: &str = "00000000-0000-4000-8000-000000000001";
const TURING: &str = "00000000-0000-4000-8000-000000000003";
const INVOICE_RETRIED: &str = "00000000-0000-4000-8000-000000000104";
const INVOICE_PROCESSING: &str = "00000000-0000-4000-8000-000000000103";

/// Runs a statement that must be rejected, and returns the SQLSTATE it was rejected with.
///
/// Takes a prepared query rather than a SQL string: sqlx 0.9's `SqlSafeStr` accepts only
/// `&'static str`, so a value reaches a statement as a bind instead of being formatted in.
async fn rejection_code(pool: &PgPool, query: Query<'_, Postgres, PgArguments>) -> String {
    let err = query
        .execute(pool)
        .await
        .expect_err("statement should have been rejected");
    err.as_database_error()
        .expect("expected a database error, not a client-side one")
        .code()
        .expect("expected a SQLSTATE")
        .into_owned()
}

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn fixtures_load_the_whole_dataset(pool: PgPool) {
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM customers),
                (SELECT count(*) FROM invoices),
                (SELECT count(*) FROM payment_intents)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (3, 7, 5));
}

/// Exact coverage, not merely "at least one": a state added to the enum without a matching
/// fixture row should fail here rather than silently go untested.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn every_invoice_state_is_represented(pool: PgPool) {
    let states: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT state::text FROM invoices ORDER BY 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        states,
        ["draft", "processed", "processing", "ready", "void"]
    );
}

#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn every_payment_status_is_represented(pool: PgPool) {
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT status::text FROM payment_intents ORDER BY 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses, ["failed", "pending", "succeeded"]);
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_customer_may_hold_no_invoices(pool: PgPool) {
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM invoices WHERE customer_id = $1::uuid")
            .bind(TURING)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(held, 0);
}

/// One invoice, a failed attempt and then the attempt that settled it. Worth asserting because
/// it is exactly the shape a uniqueness constraint on `invoice_id` would forbid.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn an_invoice_may_carry_a_retry_history(pool: PgPool) {
    let statuses: Vec<String> = sqlx::query_scalar(
        "SELECT status::text FROM payment_intents WHERE invoice_id = $1::uuid ORDER BY 1",
    )
    .bind(INVOICE_RETRIED)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(statuses, ["failed", "succeeded"]);
}

/// The fixtures date relative to `current_date`, so both sides stay populated indefinitely.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn due_dates_straddle_today(pool: PgPool) {
    let (overdue, upcoming): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE due_date < current_date),
                count(*) FILTER (WHERE due_date > current_date)
           FROM invoices",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(overdue > 0, "no overdue invoice to test collection against");
    assert!(upcoming > 0, "no invoice that is not yet due");
}

#[sqlx::test(fixtures("customers"))]
async fn a_negative_total_is_rejected(pool: PgPool) {
    let stmt = sqlx::query(
        "INSERT INTO invoices (customer_id, total_cents, due_date)
         VALUES ($1::uuid, -1, current_date)",
    )
    .bind(ADA);
    assert_eq!(rejection_code(&pool, stmt).await, "23514"); // check_violation
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn an_unknown_state_label_is_rejected(pool: PgPool) {
    let stmt = sqlx::query("UPDATE invoices SET state = 'paid'");
    assert_eq!(rejection_code(&pool, stmt).await, "22P02"); // invalid_text_representation
}

/// 'successful' is the word the requirements used; the column speaks the PSP's 'succeeded'.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn the_psp_status_vocabulary_is_authoritative(pool: PgPool) {
    let stmt = sqlx::query("UPDATE payment_intents SET status = 'successful'");
    assert_eq!(rejection_code(&pool, stmt).await, "22P02");
}

/// Asserts 23001 (restrict_violation) rather than the more general 23503
/// (foreign_key_violation): only an explicit `ON DELETE RESTRICT` raises it, so this fails if
/// the constraint is ever weakened to the `NO ACTION` default.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn a_customer_holding_invoices_cannot_be_deleted(pool: PgPool) {
    let stmt = sqlx::query("DELETE FROM customers");
    assert_eq!(rejection_code(&pool, stmt).await, "23001"); // restrict_violation
}

#[sqlx::test(fixtures("customers"))]
async fn a_duplicate_email_is_rejected(pool: PgPool) {
    let stmt =
        sqlx::query("INSERT INTO customers (name, email) VALUES ('Ada Again', 'ada@example.com')");
    assert_eq!(rejection_code(&pool, stmt).await, "23505"); // unique_violation
}

#[sqlx::test(fixtures("customers"))]
async fn an_invoice_needs_a_real_customer(pool: PgPool) {
    let stmt = sqlx::query(
        "INSERT INTO invoices (customer_id, total_cents, due_date)
         VALUES (gen_random_uuid(), 100, current_date)",
    );
    assert_eq!(rejection_code(&pool, stmt).await, "23503");
}

/// Only an invoice actually mid-charge carries a pointer, and it names the pending intent.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn only_a_processing_invoice_points_at_an_intent(pool: PgPool) {
    let pointing: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT i.id::text, i.state::text, p.id
           FROM invoices i
           JOIN payment_intents p ON p.id = i.currently_processed_by_pi_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        pointing,
        [(
            INVOICE_PROCESSING.to_string(),
            "processing".to_string(),
            "pi_seed_103_inflight".to_string(),
        )]
    );
}

/// The pointer is nullable by design: an invoice with no charge in flight carries none.
#[sqlx::test(fixtures("customers", "invoices"))]
async fn the_intent_pointer_starts_null(pool: PgPool) {
    let unset: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM invoices WHERE currently_processed_by_pi_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unset, 7, "every invoice starts with no charge in flight");
}

#[sqlx::test(fixtures("customers", "invoices"))]
async fn the_intent_pointer_must_name_a_real_intent(pool: PgPool) {
    let stmt = sqlx::query("UPDATE invoices SET currently_processed_by_pi_id = 'pi_nonexistent'");
    assert_eq!(rejection_code(&pool, stmt).await, "23503"); // foreign_key_violation
}

/// The cycle between the tables is closed with RESTRICT, so an intent cannot be deleted out
/// from under the invoice that is pointing at it.
#[sqlx::test(fixtures("customers", "invoices", "payment_intents"))]
async fn an_intent_in_flight_cannot_be_deleted(pool: PgPool) {
    let stmt = sqlx::query("DELETE FROM payment_intents WHERE id = 'pi_seed_103_inflight'");
    assert_eq!(rejection_code(&pool, stmt).await, "23001"); // restrict_violation
}
