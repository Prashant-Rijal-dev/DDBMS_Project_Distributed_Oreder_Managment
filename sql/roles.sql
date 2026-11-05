-- Cluster-level roles (created once, shared by all sites).
-- Lab passwords only. Never reuse these outside a classroom demo.
DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['regional_manager','customer_role'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ddbms_admin') THEN
    CREATE ROLE ddbms_admin LOGIN PASSWORD 'ddbms';
  END IF;

  FOREACH r IN ARRAY ARRAY['mgr_north','mgr_west','mgr_south'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L IN ROLE regional_manager', r, 'ddbms');
    END IF;
  END LOOP;

  FOREACH r IN ARRAY ARRAY['cust_1001','cust_2001','cust_3001'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L IN ROLE customer_role', r, 'ddbms');
    END IF;
  END LOOP;
END $$;
