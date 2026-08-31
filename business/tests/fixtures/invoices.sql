-- Requires the `customers` fixture. Every value of invoice_state appears at least once, so a
-- non-exhaustive state machine has something to fail against.
--
-- Due dates are relative to current_date, so "overdue" and "not yet due" stay true however
-- long after the fixture was written the suite is run.
INSERT INTO invoices (id, customer_id, total_cents, due_date, state) VALUES
    ('00000000-0000-4000-8000-000000000101', '00000000-0000-4000-8000-000000000001',
        4999,      current_date + 45, 'draft'),
    ('00000000-0000-4000-8000-000000000102', '00000000-0000-4000-8000-000000000001',
        125000,    current_date + 14, 'ready'),
    ('00000000-0000-4000-8000-000000000103', '00000000-0000-4000-8000-000000000001',
        25000,     current_date +  3, 'processing'),
    ('00000000-0000-4000-8000-000000000104', '00000000-0000-4000-8000-000000000001',
        899,       current_date - 27, 'processed'),
    -- Overdue and void: must never be chased for payment despite being past due.
    ('00000000-0000-4000-8000-000000000105', '00000000-0000-4000-8000-000000000001',
        30000,     current_date - 58, 'void'),
    -- Zero is a legal total under CHECK (total_cents >= 0).
    ('00000000-0000-4000-8000-000000000106', '00000000-0000-4000-8000-000000000002',
        0,         current_date + 90, 'ready'),
    -- Nine digits of cents, comfortably past what an i32 would hold.
    ('00000000-0000-4000-8000-000000000107', '00000000-0000-4000-8000-000000000002',
        999999999, current_date - 74, 'processed');
