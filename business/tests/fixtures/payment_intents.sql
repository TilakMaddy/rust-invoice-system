-- Requires the `invoices` fixture. Ids mimic the PSP's 'pi_' prefix (mock-payment-service
-- issues 'pi_<uuid simple>') but stay readable, so a failing assertion names its row.
INSERT INTO payment_intents (id, invoice_id, status) VALUES
    -- A failed attempt with nothing after it: invoice 102 is back to 'ready', awaiting retry.
    ('pi_seed_102_declined', '00000000-0000-4000-8000-000000000102', 'failed'),
    -- In flight, which is why invoice 103 sits in 'processing'.
    ('pi_seed_103_inflight', '00000000-0000-4000-8000-000000000103', 'pending'),
    -- Retry history on a single invoice: a failure, then the attempt that settled it.
    ('pi_seed_104_declined', '00000000-0000-4000-8000-000000000104', 'failed'),
    ('pi_seed_104_paid',     '00000000-0000-4000-8000-000000000104', 'succeeded'),
    ('pi_seed_107_paid',     '00000000-0000-4000-8000-000000000107', 'succeeded');

-- Invoice 103 is mid-charge, so it points back at the intent doing the work. This has to run
-- after the INSERT above: the foreign key needs that intent to exist first.
UPDATE invoices
   SET currently_processed_by_pi_id = 'pi_seed_103_inflight'
 WHERE id = '00000000-0000-4000-8000-000000000103';

-- Invoices 101 (draft), 105 (void) and 106 (ready) deliberately have no intent at all. 102
-- awaits a retry and 104/107 have settled, so all of them keep a NULL pointer -- only an
-- invoice actually being charged carries one.
