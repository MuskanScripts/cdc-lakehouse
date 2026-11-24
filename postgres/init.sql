-- Source OLTP schema for the CDC lakehouse.
-- Runs once, on first start of the postgres container.

CREATE TABLE users (
    user_id     BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    city        TEXT        NOT NULL,
    tier        TEXT        NOT NULL CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id  BIGSERIAL PRIMARY KEY,
    name        TEXT          NOT NULL,
    category    TEXT          NOT NULL,
    price       NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id    BIGSERIAL PRIMARY KEY,
    user_id     BIGINT        NOT NULL REFERENCES users(user_id),
    status      TEXT          NOT NULL CHECK (status IN
                  ('created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    total       NUMERIC(12,2) NOT NULL CHECK (total >= 0),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id    BIGINT        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id  BIGINT        NOT NULL REFERENCES products(product_id),
    qty         INT           NOT NULL CHECK (qty > 0),
    price       NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE payments (
    payment_id  BIGSERIAL PRIMARY KEY,
    order_id    BIGINT        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    method      TEXT          NOT NULL CHECK (method IN ('upi', 'card', 'netbanking', 'wallet', 'cod')),
    status      TEXT          NOT NULL CHECK (status IN ('pending', 'success', 'failed', 'refunded')),
    amount      NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX ON orders (status);
CREATE INDEX ON orders (user_id);
CREATE INDEX ON payments (order_id);
CREATE INDEX ON order_items (product_id);

-- REPLICA IDENTITY FULL: the WAL carries the full "before" row for updates and deletes,
-- not just the primary key. Costs a bit more WAL, but bronze keeps complete history.
ALTER TABLE users       REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments    REPLICA IDENTITY FULL;

-- Debezium signal table: used later for incremental snapshots (re-snapshot a table
-- chunk by chunk while streaming continues).
CREATE TABLE debezium_signal (
    id   VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048)
);

-- Debezium heartbeat table (ADR-037). Every 10 s the connector UPSERTs one row here
-- (heartbeat.action.query), so the slot always has a change of its own to confirm, even when
-- the shop is idle and only other databases on this server (Airflow) write WAL. Without it
-- the slot pins that WAL and the retained size grows forever. It lives in its own schema,
-- so its topic is ecom.heartbeat.debezium_heartbeat, outside bronze's ecom.public.* pattern.
CREATE SCHEMA heartbeat;
CREATE TABLE heartbeat.debezium_heartbeat (
    id INT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL
);

-- Explicit publication (connector uses publication.autocreate.mode=disabled), so the
-- CDC user does not need superuser rights to create it in production.
CREATE PUBLICATION dbz_publication
    FOR TABLE users, products, orders, order_items, payments, debezium_signal,
              heartbeat.debezium_heartbeat;
