-- =====================================================================
-- Access control for ONE site. psql vars: dbname, mgr, cust
-- Roles: ddbms_admin (everything) | regional_manager (own region, masked PII)
--        customer_role (own orders only)
-- =====================================================================
\set ON_ERROR_STOP on

REVOKE CONNECT ON DATABASE :"dbname" FROM PUBLIC;
GRANT  CONNECT ON DATABASE :"dbname" TO ddbms_admin, :"mgr", :"cust";

GRANT USAGE ON SCHEMA public TO ddbms_admin, regional_manager, customer_role;
GRANT ALL ON ALL TABLES    IN SCHEMA public TO ddbms_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO ddbms_admin;
GRANT ALL ON SCHEMA secure TO ddbms_admin;
GRANT ALL ON ALL TABLES IN SCHEMA secure TO ddbms_admin;

-- Regional manager: no access to the secure schema, only the masked customer view
GRANT SELECT ON orders, order_item, inventory, restock_request, product,
                warehouse, customer_public, price_audit, stock_log,
                v_order_summary, v_customer_masked TO regional_manager;
GRANT UPDATE (status) ON orders TO regional_manager;
GRANT UPDATE ON restock_request TO regional_manager;

-- Customer: product catalogue + their own orders through a view
GRANT SELECT ON product, v_my_orders TO customer_role;
