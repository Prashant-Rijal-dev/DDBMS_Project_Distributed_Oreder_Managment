-- =====================================================================
-- Local schema installed at EVERY site (North / West / South).
-- psql variables expected: region (N|W|S), site_name, idbase
-- Each site stores only ITS fragments of the partitioned relations and a
-- full copy of the replicated ones (PRODUCT, WAREHOUSE).
-- =====================================================================
\set ON_ERROR_STOP on

CREATE SCHEMA secure;      -- vertical fragment holding sensitive attributes

CREATE TABLE site_info (
    region    char(1) PRIMARY KEY,
    site_name text NOT NULL
);
INSERT INTO site_info VALUES (:'region', :'site_name');

-- ---------- replicated relations --------------------------------------
CREATE TABLE product (
    prod_id  varchar(8)    PRIMARY KEY,
    name     text          NOT NULL,
    category text          NOT NULL,
    price    numeric(10,2) NOT NULL CHECK (price > 0)
);

CREATE TABLE warehouse (
    warehouse_id varchar(8) PRIMARY KEY,
    city         text       NOT NULL,
    region       char(1)    NOT NULL
);

-- ---------- CUSTOMER: horizontal (by region) + vertical (public/secure) -
CREATE TABLE customer_public (
    cust_id int     PRIMARY KEY,
    name    text    NOT NULL,
    region  char(1) NOT NULL CHECK (region = :'region')     -- fragmentation predicate
);

CREATE TABLE secure.customer_secure (
    cust_id    int  PRIMARY KEY REFERENCES customer_public(cust_id) ON DELETE CASCADE,
    email      text NOT NULL UNIQUE,
    card_last4 char(4)
);

-- ---------- ORDERS: primary horizontal fragment ------------------------
CREATE TABLE orders (
    order_id   int     PRIMARY KEY,
    cust_id    int     NOT NULL REFERENCES customer_public(cust_id),
    order_date date    NOT NULL DEFAULT current_date,
    status     text    NOT NULL DEFAULT 'PLACED'
               CHECK (status IN ('PLACED','SHIPPED','DELIVERED','CANCELLED')),
    region     char(1) NOT NULL CHECK (region = :'region')   -- fragmentation predicate
);

-- ---------- ORDER_ITEM: derived horizontal fragment (semi-join ORDERS) -
CREATE TABLE order_item (
    order_id     int         NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    prod_id      varchar(8)  NOT NULL REFERENCES product(prod_id),
    qty          int         NOT NULL CHECK (qty > 0),
    unit_price   numeric(10,2) NOT NULL,          -- price snapshot at order time
    warehouse_id varchar(8)  NOT NULL REFERENCES warehouse(warehouse_id),
    PRIMARY KEY (order_id, prod_id)
);

-- ---------- INVENTORY: derived from WAREHOUSE.region -------------------
CREATE TABLE inventory (
    prod_id       varchar(8) NOT NULL REFERENCES product(prod_id),
    warehouse_id  varchar(8) NOT NULL REFERENCES warehouse(warehouse_id),
    stock         int        NOT NULL CHECK (stock >= 0),     -- no negative stock, ever
    reorder_level int        NOT NULL DEFAULT 5,
    PRIMARY KEY (prod_id, warehouse_id)
);

-- ---------- Active-database support tables ------------------------------
CREATE TABLE restock_request (
    req_id        serial PRIMARY KEY,
    prod_id       varchar(8) NOT NULL,
    warehouse_id  varchar(8) NOT NULL,
    qty_requested int        NOT NULL,
    status        text       NOT NULL DEFAULT 'OPEN',
    created_at    timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (prod_id, warehouse_id) REFERENCES inventory(prod_id, warehouse_id)
);

CREATE TABLE price_audit (
    audit_id   serial PRIMARY KEY,
    prod_id    varchar(8)    NOT NULL,
    old_price  numeric(10,2) NOT NULL,
    new_price  numeric(10,2) NOT NULL,
    changed_by text          NOT NULL DEFAULT current_user,
    changed_at timestamptz   NOT NULL DEFAULT now()
);

-- before/after images of every stock change (evidence for undo/redo)
CREATE TABLE stock_log (
    log_id       serial PRIMARY KEY,
    txn          text,
    prod_id      varchar(8) NOT NULL,
    warehouse_id varchar(8) NOT NULL,
    old_stock    int NOT NULL,
    new_stock    int NOT NULL,
    logged_at    timestamptz NOT NULL DEFAULT now()
);

-- maps a database login (customer role) to a customer id (access control)
CREATE TABLE customer_login (
    role_name text PRIMARY KEY,
    cust_id   int  NOT NULL REFERENCES customer_public(cust_id)
);

-- globally unique order ids: every site draws from its own range
CREATE SEQUENCE order_seq START :idbase;

-- =====================================================================
-- TRIGGERS (Active database)
-- =====================================================================
CREATE FUNCTION fn_stock_log() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO stock_log(txn, prod_id, warehouse_id, old_stock, new_stock)
    VALUES (current_setting('ddbms.txn', true), NEW.prod_id, NEW.warehouse_id, OLD.stock, NEW.stock);
    RETURN NEW;
END $$;

CREATE TRIGGER trg_stock_log
AFTER UPDATE OF stock ON inventory
FOR EACH ROW WHEN (OLD.stock IS DISTINCT FROM NEW.stock)
EXECUTE FUNCTION fn_stock_log();

CREATE FUNCTION fn_auto_restock() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.stock < NEW.reorder_level
       AND NOT EXISTS (SELECT 1 FROM restock_request r
                        WHERE r.prod_id = NEW.prod_id
                          AND r.warehouse_id = NEW.warehouse_id
                          AND r.status = 'OPEN')
    THEN
        INSERT INTO restock_request(prod_id, warehouse_id, qty_requested)
        VALUES (NEW.prod_id, NEW.warehouse_id, GREATEST(NEW.reorder_level * 4 - NEW.stock, 1));
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER trg_auto_restock
AFTER UPDATE OF stock ON inventory
FOR EACH ROW EXECUTE FUNCTION fn_auto_restock();

CREATE FUNCTION fn_price_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO price_audit(prod_id, old_price, new_price) VALUES (NEW.prod_id, OLD.price, NEW.price);
    RETURN NEW;
END $$;

CREATE TRIGGER trg_price_audit
AFTER UPDATE OF price ON product
FOR EACH ROW WHEN (OLD.price IS DISTINCT FROM NEW.price)
EXECUTE FUNCTION fn_price_audit();

-- =====================================================================
-- VIEWS (view management / access control)
-- =====================================================================
CREATE VIEW v_order_summary AS
SELECT o.order_id, o.cust_id, o.order_date, o.status, o.region,
       COALESCE(SUM(oi.qty * oi.unit_price), 0) AS total
FROM orders o
LEFT JOIN order_item oi ON oi.order_id = o.order_id
GROUP BY o.order_id;

CREATE VIEW v_customer_masked AS
SELECT c.cust_id, c.name, c.region,
       regexp_replace(s.email, '^(.).*@', '\1***@') AS email_masked
FROM customer_public c
JOIN secure.customer_secure s ON s.cust_id = c.cust_id;

-- a customer sees ONLY their own orders (current_user is mapped via customer_login)
CREATE VIEW v_my_orders AS
SELECT v.*
FROM v_order_summary v
JOIN customer_login l ON l.cust_id = v.cust_id
WHERE l.role_name = current_user;
