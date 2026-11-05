-- =====================================================================
-- GLOBAL DATA DICTIONARY (database ddbms_catalog)
-- Consulted by the global query processor at run time for localization.
-- =====================================================================
\set ON_ERROR_STOP on

CREATE TABLE site (
    site_id     char(1) PRIMARY KEY,
    db_name     text NOT NULL,
    site_name   text NOT NULL
);

CREATE TABLE global_relation (
    rel_name    text PRIMARY KEY,
    attributes  text NOT NULL,
    description text
);

CREATE TABLE fragment (
    frag_name   text PRIMARY KEY,
    frag_group  text NOT NULL,              -- name used by the query processor
    rel_name    text NOT NULL REFERENCES global_relation(rel_name),
    frag_type   text NOT NULL CHECK (frag_type IN
                ('HORIZONTAL','DERIVED_HORIZONTAL','HYBRID','REPLICA')),
    predicate   text,                       -- fragmentation predicate / definition
    columns     text,                       -- projected attributes (vertical part)
    region_code char(1) REFERENCES site(site_id)   -- NULL for replicated fragments
);

CREATE TABLE allocation (
    frag_name   text NOT NULL REFERENCES fragment(frag_name),
    site_id     char(1) NOT NULL REFERENCES site(site_id),
    local_table text NOT NULL,
    access_freq text,
    reason      text,
    PRIMARY KEY (frag_name, site_id)
);

INSERT INTO site VALUES
 ('N','ddbms_north','North (Delhi / Amritsar)'),
 ('W','ddbms_west' ,'West (Mumbai)'),
 ('S','ddbms_south','South (Bengaluru)');

INSERT INTO global_relation VALUES
 ('CUSTOMER'  ,'cust_id, name, email, card_last4, region','Registered customers'),
 ('PRODUCT'   ,'prod_id, name, category, price'          ,'Product catalogue'),
 ('WAREHOUSE' ,'warehouse_id, city, region'              ,'Regional warehouses'),
 ('ORDERS'    ,'order_id, cust_id, order_date, status, region','Customer orders'),
 ('ORDER_ITEM','order_id, prod_id, qty, unit_price, warehouse_id','Order lines'),
 ('INVENTORY' ,'prod_id, warehouse_id, stock, reorder_level','Stock per warehouse');

-- ---- fragments ------------------------------------------------------
INSERT INTO fragment
SELECT 'CUSTOMER_PUB_'||site_id,'CUSTOMER_PUB','CUSTOMER','HYBRID',
       format('region = %L', site_id),'cust_id, name, region', site_id FROM site;

INSERT INTO fragment
SELECT 'CUSTOMER_SEC_'||site_id,'CUSTOMER_SEC','CUSTOMER','HYBRID',
       format('region = %L', site_id),'cust_id, email, card_last4', site_id FROM site;

INSERT INTO fragment
SELECT 'ORDERS_'||site_id,'ORDERS','ORDERS','HORIZONTAL',
       format('region = %L', site_id), NULL, site_id FROM site;

INSERT INTO fragment
SELECT 'ORDER_ITEM_'||site_id,'ORDER_ITEM','ORDER_ITEM','DERIVED_HORIZONTAL',
       format('ORDER_ITEM SEMIJOIN ORDERS_%s ON order_id', site_id), NULL, site_id FROM site;

INSERT INTO fragment
SELECT 'INVENTORY_'||site_id,'INVENTORY','INVENTORY','DERIVED_HORIZONTAL',
       format('warehouse_id IN (SELECT warehouse_id FROM WAREHOUSE WHERE region = %L)', site_id),
       NULL, site_id FROM site;

INSERT INTO fragment VALUES
 ('PRODUCT_R'  ,'PRODUCT'  ,'PRODUCT'  ,'REPLICA','full copy',NULL,NULL),
 ('WAREHOUSE_R','WAREHOUSE','WAREHOUSE','REPLICA','full copy',NULL,NULL);

-- ---- allocation of partitioned fragments (one site each) -------------
INSERT INTO allocation
SELECT f.frag_name, f.region_code,
  CASE f.frag_group WHEN 'CUSTOMER_PUB' THEN 'customer_public'
                    WHEN 'CUSTOMER_SEC' THEN 'secure.customer_secure'
                    WHEN 'ORDERS'       THEN 'orders'
                    WHEN 'ORDER_ITEM'   THEN 'order_item'
                    WHEN 'INVENTORY'    THEN 'inventory' END,
  CASE f.frag_group WHEN 'CUSTOMER_PUB' THEN 'high'
                    WHEN 'CUSTOMER_SEC' THEN 'low'
                    WHEN 'ORDERS'       THEN 'very high (local writes)'
                    WHEN 'ORDER_ITEM'   THEN 'very high (local writes)'
                    WHEN 'INVENTORY'    THEN 'high (local), medium (remote)' END,
  CASE f.frag_group WHEN 'CUSTOMER_PUB' THEN 'Customers browse and order from their own region'
                    WHEN 'CUSTOMER_SEC' THEN 'Sensitive data kept at the home site under stricter privileges'
                    WHEN 'ORDERS'       THEN 'Orders are created and updated where the customer lives'
                    WHEN 'ORDER_ITEM'   THEN 'Co-located with ORDERS so the join never crosses sites'
                    WHEN 'INVENTORY'    THEN 'Stock lives at the warehouse that physically holds it' END
FROM fragment f WHERE f.region_code IS NOT NULL;

-- ---- allocation of replicated fragments (every site) -----------------
INSERT INTO allocation
SELECT f.frag_name, s.site_id, lower(f.frag_group),
       'very high (read-mostly)',
       'Read at every site for every order and report; updates are rare, so replication is cheap'
FROM fragment f CROSS JOIN site s WHERE f.region_code IS NULL;
