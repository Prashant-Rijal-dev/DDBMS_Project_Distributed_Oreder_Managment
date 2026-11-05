#!/usr/bin/env bash
# Creates the 3 site databases + catalog + roles on a local PostgreSQL (macOS / Homebrew).
set -euo pipefail
cd "$(dirname "$0")"
PSQL="psql -X -q -v ON_ERROR_STOP=1"

command -v psql >/dev/null || { echo "psql not found. Run: brew install postgresql@16 && brew services start postgresql@16"; exit 1; }
pg_isready -q || { echo "PostgreSQL is not running. Run: brew services start postgresql@16"; exit 1; }

# Two-phase commit needs max_prepared_transactions > 0 (default is 0 on a fresh install)
MPT=$($PSQL -At -d postgres -c "SHOW max_prepared_transactions")
if [ "$MPT" = "0" ]; then
  echo "Enabling max_prepared_transactions = 20 (needs a server restart) ..."
  $PSQL -d postgres -c "ALTER SYSTEM SET max_prepared_transactions = 20"
  SVC=$(brew services list 2>/dev/null | awk '/postgresql/ {print $1; exit}')
  if [ -n "${SVC:-}" ]; then
    brew services restart "$SVC"
    for i in $(seq 1 20); do pg_isready -q && break; sleep 1; done
  else
    echo "Please restart PostgreSQL yourself (e.g. brew services restart postgresql@16), then re-run ./setup.sh"
    exit 1
  fi
fi

echo "Creating roles ..."
$PSQL -d postgres -f sql/roles.sql

echo "Creating catalog database ..."
$PSQL -d postgres -c "DROP DATABASE IF EXISTS ddbms_catalog WITH (FORCE)"
$PSQL -d postgres -c "CREATE DATABASE ddbms_catalog"
$PSQL -d ddbms_catalog -f sql/catalog.sql

setup_site() {  # region db site_name mgr cust idbase
  echo "Creating site $3 ($2) ..."
  $PSQL -d postgres -c "DROP DATABASE IF EXISTS $2 WITH (FORCE)"
  $PSQL -d postgres -c "CREATE DATABASE $2"
  $PSQL -d "$2" -v region="$1" -v site_name="$3" -v idbase="$6" -f sql/site_schema.sql
  $PSQL -d "$2" -v dbname="$2" -v mgr="$4" -v cust="$5" -f sql/security.sql
}
setup_site N ddbms_north "North (Delhi/Amritsar)" mgr_north cust_1001 10101
setup_site W ddbms_west  "West (Mumbai)"          mgr_west  cust_2001 20101
setup_site S ddbms_south "South (Bengaluru)"      mgr_south cust_3001 30101

echo "Loading sample data ..."
python3 seed.py
echo
echo "Setup complete. Next:  python3 demo.py check   then   python3 demo.py all"
