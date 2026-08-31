-- Two receivers, standing in for what `WEBHOOK_ENDPOINTS` would have synced at startup. Ids are
-- fixed rather than left to uuidv7() so a test can name the row it expects, as the other
-- fixtures do.
--
-- Secrets are the literal strings the signature is keyed on: a test that verifies a delivery
-- recomputes the HMAC with exactly these bytes, so they are readable rather than realistic.
INSERT INTO webhook_endpoints (id, url, secret, disabled_at) VALUES
    ('00000000-0000-4000-8000-000000000201', 'http://127.0.0.1:1/hooks/primary',
        'whsec_primary', NULL),
    ('00000000-0000-4000-8000-000000000202', 'http://127.0.0.1:1/hooks/secondary',
        'whsec_secondary', NULL),
    -- Dropped from the configuration at some earlier boot. Nothing new fans out to it, and the
    -- row stays because a delivery may still reference it -- the case that separates "disabled"
    -- from "deleted".
    ('00000000-0000-4000-8000-000000000203', 'http://127.0.0.1:1/hooks/retired',
        'whsec_retired', now());

-- The URLs point at port 1, which nothing listens on. A test that wants a delivery to actually
-- land rewrites the row to its own ephemeral stub; a test that wants one to fail leaves it, and
-- gets a refused connection without waiting for a timeout.
