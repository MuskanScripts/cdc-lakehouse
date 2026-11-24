-- Week 5 drill recovery (`make bad-batch-fix`): correct the bad rows AT THE SOURCE and restore the
-- constraint. The fix flows through CDC like any other change (new LSN), silver MERGEs it, the DQ
-- gate passes again and the next gold build is published. Nothing is patched in silver or gold.
\set ON_ERROR_STOP on
BEGIN;
UPDATE orders SET total = -total, updated_at = now()
WHERE total < 0   -- only the drill's rows: the CHECK below forbids negatives at all other times
RETURNING order_id, total, updated_at;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_total_check;            -- safe to run twice
ALTER TABLE orders ADD CONSTRAINT orders_total_check CHECK (total >= 0);   -- as in postgres/init.sql
COMMIT;
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = 'orders'::regclass AND contype = 'c' ORDER BY conname;
