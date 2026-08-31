-- Outbound webhooks: the endpoints that receive them, the events worth sending, and the
-- per-endpoint delivery attempts that carry one to the other.
--
-- The shape is a transactional outbox. An event row is written in the *same* transaction as the
-- invoice change that caused it, so an event exists exactly when that change committed — and a
-- background dispatcher, not the request, does the HTTP.

CREATE TYPE webhook_delivery_status AS ENUM ('pending', 'succeeded', 'exhausted');

-- Synced from the WEBHOOK_ENDPOINTS environment variable at every startup, before the listener
-- binds. Nothing writes here over HTTP: registration is configuration, so the endpoint set is
-- reviewable and deploys like the rest of the service.
CREATE TABLE webhook_endpoints (
    id  uuid PRIMARY KEY DEFAULT uuidv7(),
    -- UNIQUE is load-bearing rather than hygiene: it is what makes the startup sync an upsert,
    -- so a restart with unchanged configuration is a no-op instead of a second row.
    url text NOT NULL UNIQUE,
    -- The HMAC key, verbatim as configured. It lives here as well as in the environment so that
    -- a delivery already queued can still be signed after the configuration drops its endpoint.
    secret text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- Set when the URL is no longer in the configuration. A flag rather than a DELETE, because
    -- deliveries reference this row and their history is worth more than a tidy table — the same
    -- reading every ON DELETE RESTRICT in the core schema takes.
    disabled_at timestamptz
);

-- What happened, independent of who was told. Recorded even when no endpoint is configured at
-- all, which is what lets a business added later catch up through GET /events?after=. That is
-- also the reason this is its own table rather than a column on the deliveries below: an event
-- with no deliveries would otherwise leave no trace of having occurred.
CREATE TABLE webhook_events (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    -- text, not an enum: a new event type should be a line of Rust, not a migration. The core
    -- schema's enums describe state machines this service must handle exhaustively; an event
    -- vocabulary only grows, and a receiver already has to ignore types it does not know.
    type text NOT NULL,
    -- Serialised once, at enqueue, and sent byte for byte on every attempt. The body is what the
    -- signature covers, so a payload rebuilt per attempt would be a signature that has to be
    -- rebuilt with it, for no gain.
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (event, endpoint): the unit of work the dispatcher claims, and the unit of
-- retrying. Two endpoints failing independently is two rows failing independently.
CREATE TABLE webhook_deliveries (
    id          uuid PRIMARY KEY DEFAULT uuidv7(),
    event_id    uuid NOT NULL REFERENCES webhook_events (id) ON DELETE RESTRICT,
    endpoint_id uuid NOT NULL REFERENCES webhook_endpoints (id) ON DELETE RESTRICT,
    status      webhook_delivery_status NOT NULL DEFAULT 'pending',
    attempts    int NOT NULL DEFAULT 0,
    -- Due immediately by default, so the first attempt goes out on the next dispatcher tick.
    -- Every retry pushes this forward by the backoff interval; the dispatcher's whole claim is
    -- "pending and due", which keeps the schedule in the row rather than in a timer somewhere.
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    -- How the last attempt ended: a status when the receiver answered, an error when nothing
    -- did. Exactly one of them is set, which is the difference between "your endpoint said 500"
    -- and "your endpoint did not answer" — the two need different fixes.
    last_status smallint,
    last_error  text,
    CONSTRAINT webhook_deliveries_one_failure_kind
        CHECK (last_status IS NULL OR last_error IS NULL),
    delivered_at timestamptz,
    -- An event is owed to an endpoint once. Nothing in this service inserts twice, but the fan
    -- out is an INSERT ... SELECT over whatever endpoints are enabled at the time, and this is
    -- what makes that statement safe to reason about rather than merely correct today.
    UNIQUE (event_id, endpoint_id)
);

-- The dispatcher's poll runs once a second and asks exactly one question: what is pending and
-- due? Partial, so the index holds only the rows still in play — a succeeded delivery leaves it
-- and never comes back, which keeps the index the size of the backlog rather than of the table.
CREATE INDEX webhook_deliveries_due_idx
    ON webhook_deliveries (next_attempt_at)
    WHERE status = 'pending';

-- Postgres indexes a primary key automatically but never the referencing side of a foreign key,
-- and both of these are filters on GET /webhook_deliveries.
CREATE INDEX webhook_deliveries_event_id_idx ON webhook_deliveries (event_id);
CREATE INDEX webhook_deliveries_endpoint_id_idx ON webhook_deliveries (endpoint_id);
