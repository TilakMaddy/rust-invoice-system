# List available recipes.
default:
    @just --list

# What `just test` points the business crate's tests at: the port Compose publishes from the
# postgres container. Mirrors business/.env, which is gitignored and so absent in CI.
database_url := env('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/business')

# How many tests run at once, per binary. Left to itself libtest uses one thread per core, which
# on a two-core CI runner is a different shape of run than the eight-way machine these were
# written on -- and the payment safety tests are timing-sensitive enough to care: the barrier in
# `concurrent_charges_of_one_invoice_charge_the_card_once` parks eight in-flight charges while
# other tests hold pools of their own against the same Postgres. Pinning it makes the run the
# same everywhere rather than a function of the runner. Raise it with `TEST_THREADS=8 just test`.
test_threads := env('TEST_THREADS', '4')

# Build images and start the whole stack in the background.
up:
    docker compose up -d --build

# Stop the stack. The Postgres volume survives; `docker compose down -v` wipes it.
down:
    docker compose down

# Open a psql shell in Postgres, starting it first if it is not already up.
db:
    docker compose up -d --wait postgres
    docker compose exec postgres psql -U postgres -d business

# Wipe the database and bring up a fresh, empty one. Destructive: every table and row is gone.
[confirm("Wipe the Postgres database? Every table and row will be deleted. [y/N]")]
reset-db:
    docker compose down -v postgres
    docker compose up -d --wait postgres
    docker compose up -d --wait --build business

# Follow logs for every service, or one: `just logs postgres`.
logs *services:
    docker compose logs -f {{services}}

# Rebuild one service's image and restart it: `just rebuild business`.
rebuild service:
    docker compose up -d --build --wait {{service}}

# Format every crate in place. No workspace, so cargo runs once per crate rather than at the root.
fmt:
    cd business && cargo fmt
    cd mock-payment-service && cargo fmt
    cd webhook-receiver && cargo fmt

# Every test in every crate. Starts Postgres first and waits for it: `#[sqlx::test]` in the
# business crate provisions a fresh, migrated database per test off DATABASE_URL, so a run
# without it up fails at connect rather than skipping. The other two crates need nothing.
# Override the connection with `DATABASE_URL=... just test`, and the parallelism with
# `TEST_THREADS=...`.
test:
    docker compose up -d --wait postgres
    cd business && DATABASE_URL={{database_url}} cargo test -- --test-threads={{test_threads}}
    cd mock-payment-service && cargo test -- --test-threads={{test_threads}}
    cd webhook-receiver && cargo test -- --test-threads={{test_threads}}

# Fail on unformatted code or any clippy warning, changing nothing. What CI would run.
lint:
    cd business && cargo fmt --check
    cd business && cargo clippy --all-targets -- -D warnings
    cd mock-payment-service && cargo fmt --check
    cd mock-payment-service && cargo clippy --all-targets -- -D warnings
    cd webhook-receiver && cargo fmt --check
    cd webhook-receiver && cargo clippy --all-targets -- -D warnings

# Drive the whole webhook flow and show the deliveries, both sides. Needs the stack up, plus jq.
demo:
    ./scripts/demo.sh

# Drive the five payment failure modes end to end. Needs the stack up, plus jq.
verify-payments:
    ./scripts/verify-payments.sh
