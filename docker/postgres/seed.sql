-- Demo "customer" source database: a small CRM-style schema the COA stack
-- scans, induces an ontology from, and serves Tier-1/Tier-2 queries against.
-- The scan workers connect with these credentials (see demo-source seed in
-- the gateway/provisioner flow).

CREATE TABLE IF NOT EXISTS customers (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(200) NOT NULL,
    email         VARCHAR(320) NOT NULL,
    region        VARCHAR(80) NOT NULL,
    signup_date   DATE NOT NULL,
    lifetime_value NUMERIC(12, 2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS products (
    id            SERIAL PRIMARY KEY,
    sku           VARCHAR(64) NOT NULL,
    name          VARCHAR(200) NOT NULL,
    category      VARCHAR(80) NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES customers(id),
    order_date    DATE NOT NULL,
    status        VARCHAR(40) NOT NULL,
    total_amount  NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id            SERIAL PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES orders(id),
    product_id    INTEGER NOT NULL REFERENCES products(id),
    quantity      INTEGER NOT NULL,
    line_total    NUMERIC(12, 2) NOT NULL
);

INSERT INTO customers (name, email, region, signup_date, lifetime_value) VALUES
  ('Acme Corp',     'contact@acme.example',    'US-EAST',  '2025-01-15', 125000.00),
  ('Globex Ltd',    'info@globex.example',     'EU-WEST',  '2025-02-20', 84200.50),
  ('Initech',       'hello@initech.example',   'US-WEST',  '2025-03-05', 43100.75),
  ('Umbrella Health','admin@umbrella.example','US-EAST',  '2025-04-11', 512300.00),
  ('Stark Industries','procurement@stark.example','US-EAST','2025-05-30', 989000.00)
ON CONFLICT DO NOTHING;

INSERT INTO products (sku, name, category, unit_price) VALUES
  ('SUB-ENT-01', 'Enterprise Subscription', 'Subscription', 1200.00),
  ('SUB-PRO-02', 'Professional Plan',       'Subscription', 400.00),
  ('HW-EDGE-03', 'Edge Gateway Device',     'Hardware',     2500.00),
  ('HW-CORE-04', 'Core Controller',         'Hardware',     5200.00),
  ('SUP-PREM-05','Premium Support',         'Services',     900.00)
ON CONFLICT DO NOTHING;

INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES
  (1, '2025-06-01', 'PAID',     2400.00),
  (1, '2025-07-15', 'PAID',     5200.00),
  (2, '2025-06-22', 'SHIPPED',  1800.00),
  (3, '2025-08-03', 'PENDING',   900.00),
  (4, '2025-08-14', 'PAID',    25000.00),
  (5, '2025-09-01', 'SHIPPED',  61000.00)
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, product_id, quantity, line_total) VALUES
  (1, 1, 2, 2400.00),
  (2, 4, 1, 5200.00),
  (3, 2, 3, 1200.00),
  (3, 5, 1,  600.00),
  (4, 5, 1,  900.00),
  (5, 1, 5, 6000.00),
  (5, 3, 4, 10000.00),
  (6, 4, 6, 31200.00),
  (6, 1, 10, 12000.00)
ON CONFLICT DO NOTHING;
