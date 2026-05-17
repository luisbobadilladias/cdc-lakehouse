-- ─── LOGICAL REPLICATION PUBLICATION ────────────────────────────────────────
-- A "publication" is Postgres's way of declaring which tables Debezium is
-- allowed to subscribe to. FOR ALL TABLES means any new table you create is
-- automatically included without reconfiguring the connector.
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- ─── SCHEMA ──────────────────────────────────────────────────────────────────
CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    full_name   VARCHAR(255) NOT NULL,
    address     TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER      NOT NULL REFERENCES users(id),
    -- status lifecycle: pending → processing → shipped → delivered | cancelled
    status       VARCHAR(50)  NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(10,2) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─── SEED DATA ───────────────────────────────────────────────────────────────
INSERT INTO users (email, full_name, address) VALUES
    ('alice@example.com', 'Alice Johnson', '123 Main St, Springfield'),
    ('bob@example.com',   'Bob Smith',     '456 Oak Ave, Shelbyville'),
    ('carol@example.com', 'Carol White',   '789 Pine Rd, Ogdenville');

INSERT INTO orders (user_id, status, total_amount) VALUES
    (1, 'delivered',  129.99),
    (1, 'processing',  59.50),
    (2, 'pending',    249.00),
    (3, 'shipped',     89.95);
