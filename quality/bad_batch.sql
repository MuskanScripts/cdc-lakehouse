-- Week 5 drill (`make bad-batch`): an upstream bug writes orders with NEGATIVE totals.
--
-- The source table has CHECK (total >= 0), which would stop it, so the drill first drops that
-- constraint, the way a careless migration would. This changes the live source schema for the
-- duration of the drill only; `make bad-batch-fix` corrects the rows and puts the constraint back
-- with the same definition as postgres/init.sql.
--
-- The rows are new orders (status 'created', no items, no payment). The fix finds them by
-- total < 0: no other row can be negative, because the CHECK is only off during the drill. (Until
-- Week 6 they were tagged coupon_code='BAD_BATCH', but that column only exists after `make drift`,
-- so the drill failed on a fresh clone.) A running simulator does not touch them: it only advances
-- orders it created itself or loaded when it started.
\set ON_ERROR_STOP on
BEGIN;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_total_check;
INSERT INTO orders (user_id, status, total)
SELECT user_id, 'created', -round((100 + random() * 900)::numeric, 2)
FROM (SELECT user_id FROM users ORDER BY random() LIMIT 5) u
RETURNING order_id, user_id, total, updated_at;
COMMIT;
