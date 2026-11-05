#!/usr/bin/env bash
# Removes everything the project created.
PSQL="psql -X -q -d postgres"
for db in ddbms_north ddbms_west ddbms_south ddbms_catalog; do
  $PSQL -c "DROP DATABASE IF EXISTS $db WITH (FORCE)"
done
for r in mgr_north mgr_west mgr_south cust_1001 cust_2001 cust_3001 ddbms_admin regional_manager customer_role; do
  $PSQL -c "DROP ROLE IF EXISTS $r"
done
echo "Removed."
