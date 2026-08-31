-- Ids are fixed rather than left to uuidv7(), so a test can name the row it expects without
-- querying for it first.
INSERT INTO customers (id, name, email) VALUES
    ('00000000-0000-4000-8000-000000000001', 'Ada Lovelace', 'ada@example.com'),
    ('00000000-0000-4000-8000-000000000002', 'Grace Hopper', 'grace@example.com'),
    -- Holds no invoices in any fixture: the empty-collection case list endpoints get wrong.
    ('00000000-0000-4000-8000-000000000003', 'Alan Turing',  'alan@example.com');
