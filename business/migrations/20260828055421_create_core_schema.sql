-- The invoice system's core schema: customers, the invoices raised against them, the payment
-- intents that settle those invoices at the PSP, and the idempotency keys that keep a retried
-- charge from becoming a second one.

CREATE TYPE invoice_state AS ENUM ('draft', 'ready', 'processing', 'processed', 'void');

-- Mirrors the mock PSP's public status vocabulary byte for byte (`PaymentStatus` in
-- mock-payment-service/src/state.rs), so a status read back from GET /payment_intents/{id}
-- is persisted without a translation step that could drift.
CREATE TYPE payment_intent_status AS ENUM ('pending', 'succeeded', 'failed');

CREATE TABLE customers (
    id    uuid PRIMARY KEY DEFAULT uuidv7(),
    name  text NOT NULL,
    email text NOT NULL UNIQUE
);

CREATE TABLE invoices (
    id          uuid PRIMARY KEY DEFAULT uuidv7(),
    -- RESTRICT, not CASCADE: deleting a customer who still has invoices is a mistake worth
    -- failing loudly on, not something to silently take the financial records down with.
    customer_id uuid NOT NULL REFERENCES customers (id) ON DELETE RESTRICT,
    -- Minor units. Integer-only by construction, so no float and no NUMERIC decode crate.
    total_cents bigint NOT NULL CHECK (total_cents >= 0),
    due_date    date NOT NULL,
    state       invoice_state NOT NULL DEFAULT 'draft',
    -- The intent currently being charged: set while the invoice is 'processing', cleared once
    -- that attempt settles. Nullable because an invoice spends most of its life with no charge
    -- in flight, and every invoice starts that way. Its foreign key is declared further down,
    -- once payment_intents exists.
    currently_processed_by_pi_id text
);

-- Postgres indexes a primary key automatically but never the referencing side of a foreign
-- key, and every "invoices for this customer" lookup goes through this column.
CREATE INDEX invoices_customer_id_idx ON invoices (customer_id);

CREATE TABLE payment_intents (
    -- The PSP issues this id ('pi_<uuid>'), which makes it the natural key: no second
    -- identifier to keep in sync, and no row can exist before the PSP agrees one does.
    id         text PRIMARY KEY,
    invoice_id uuid NOT NULL REFERENCES invoices (id) ON DELETE RESTRICT,
    status     payment_intent_status NOT NULL DEFAULT 'pending'
);

CREATE INDEX payment_intents_invoice_id_idx ON payment_intents (invoice_id);

-- What a caller has already been told, so telling them again costs nothing and charges nobody.
--
-- Written and read only by the middleware in src/idempotency.rs, which is why nothing here
-- references invoices: a key is a fact about one HTTP request, not about the invoice that
-- request happened to name.
CREATE TABLE idempotency_keys (
    -- The caller's Idempotency-Key verbatim. Primary key because the whole mechanism is one
    -- lookup by it, and because the uniqueness is the thing being enforced: an INSERT that
    -- conflicts here is how a duplicate is detected at all.
    key           text PRIMARY KEY,
    -- sha256 of "METHOD path\n<body>", hex, computed by Postgres. The path is in the hash and
    -- not just the body, so one key sent against two different invoices is caught as the
    -- different request it is rather than replaying the first invoice's answer for the second.
    --
    -- A hash rather than the request itself: the body carries a card token, and this table has
    -- no business being the place one is kept.
    request_hash  text NOT NULL,
    -- The answer, once there is one. Both NULL in between: the row is inserted before the
    -- handler runs, so that a duplicate arriving mid-charge has something to collide with, and
    -- filled in once the handler has produced a response. The status is stored beside the body
    -- because a replay has to reproduce the whole answer, and a 402 and a 200 differ only here.
    status_code   smallint,
    response_body text,
    -- Half an answer is not an answer. The two columns are written by one UPDATE and are only
    -- ever both absent or both present; anything else means a replay would invent a status or
    -- an empty body.
    CONSTRAINT idempotency_keys_response_is_whole
        CHECK ((status_code IS NULL) = (response_body IS NULL))
);

-- Closes the cycle between the two tables: every intent names its invoice, and an invoice
-- being charged names the intent doing it. Declared out of line because payment_intents did
-- not exist yet when invoices was created.
--
-- Nothing needs deferring despite the cycle, because the column is nullable: an invoice is
-- inserted with it NULL, its intent is inserted against that invoice, and only then is the
-- invoice updated to point back.
ALTER TABLE invoices
    ADD CONSTRAINT invoices_currently_processed_by_pi_id_fkey
    FOREIGN KEY (currently_processed_by_pi_id)
    REFERENCES payment_intents (id) ON DELETE RESTRICT;
