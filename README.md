# Distributed Order Management System (DDBMS mini project)

Three PostgreSQL databases act as autonomous sites (North, West, South) plus a
catalog database holding the global data dictionary. A Python layer is the
global query processor, two-phase-commit coordinator, deadlock detector and
recovery manager.

## 1. Setup on macOS (about 5 minutes)

```bash
# 1. PostgreSQL via Homebrew
brew install postgresql@16
brew services start postgresql@16
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc            # Intel Mac: replace /opt/homebrew with /usr/local

# 2. Python dependencies
cd ddbms_project
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Create sites, catalog, roles, data (also turns on max_prepared_transactions)
./setup.sh

# 4. Verify, then run
python3 demo.py check
python3 demo.py all            # or: python3 demo.py 3   (one scenario)
./run_all.sh                   # saves output to results/demo_output.txt for the report
```

`setup.sh` sets `max_prepared_transactions = 20` and restarts PostgreSQL via
`brew services`. Two-phase commit (`PREPARE TRANSACTION`) does not work without this.
If it cannot restart the service, run `ALTER SYSTEM SET max_prepared_transactions = 20;`
yourself, restart PostgreSQL, and re-run `./setup.sh`.

If your Mac login is not a PostgreSQL superuser, export `PGUSER` / `PGPASSWORD` before running.

## 2. Scenarios

| # | Command | Shows | Syllabus |
|---|---------|-------|----------|
| 1 | `python3 demo.py 1` | data dictionary, fragmentation, allocation | 1.3 |
| 2 | `python3 demo.py 2` | global query to fragment queries (7 steps, 5 queries) | 1.5, 2.1 |
| 3 | `python3 demo.py 3` | cross-site order with 2PC; restock trigger fires | 2.2, 3.1 |
| 4 | `python3 demo.py 4` | lost update without locks vs strict 2PL | 2.2 |
| 5 | `python3 demo.py 5` | cross-site deadlock found by global wait-for graph | 2.2 |
| 6 | `python3 demo.py 6` | coordinator crash: undo and redo recovery | 2.3 |
| 7 | `python3 demo.py 7` | triggers, replicated price update via 2PC | 3.1 |
| 8 | `python3 demo.py 8` | roles, views, GRANT / REVOKE | self-study |

## 2b. Web dashboard (optional GUI)

```bash
pip install -r requirements.txt     # adds Flask
python3 app.py                      # then open http://127.0.0.1:5000
```

Start it from the same terminal where `PGUSER` / `PGPASSWORD` are exported. Tabs:
**Sites** (live stock, orders, restock requests, in-doubt 2PC transactions per site),
**Place order** (cross-site 2PC with optional crash injection and a Run recovery button),
**Global queries** (the 7-step query processor), **Scenarios** (runs the 8 demos in the browser),
**Catalog & log** (data dictionary and coordinator log). Stop with Ctrl+C.

## 3. Files

```
setup.sh / run_all.sh / teardown.sh   lifecycle scripts
sql/roles.sql        cluster roles          sql/catalog.sql   global data dictionary
sql/site_schema.sql  tables, triggers, views (installed at every site)
sql/security.sql     GRANT / REVOKE per site
ddbms.py             query processor, 2PC, 2PL helpers, deadlock detector, recovery
seed.py              sample data / reset    demo.py           the 8 scenarios
app.py + templates/  web dashboard (Flask)
docs/REPORT.md       full project report
```

## 4. Handy psql commands during the viva

```bash
psql -d ddbms_north -c "SELECT * FROM inventory WHERE prod_id='P012'"
psql -d ddbms_west  -c "SELECT * FROM pg_prepared_xacts"      # in-doubt 2PC branches
psql -d ddbms_catalog -c "SELECT * FROM allocation"
cat logs/coordinator.log                                      # the coordinator's log
```

## 5. Troubleshooting

| Problem | Fix |
|---------|-----|
| `prepared transactions are disabled` | `max_prepared_transactions` is 0; see section 1 |
| `role "<you>" does not exist` | `createuser -s $USER` or set `PGUSER` |
| `permission denied for database` in scenarios 1-7 | you ran as a lab role; use your own superuser login |
| A demo hangs | a prepared transaction holds locks: `python3 seed.py` rolls all back |
| Start over completely | `./teardown.sh && ./setup.sh` |

## 6. Honest limitations

* The three "sites" are databases in one PostgreSQL server on one machine. Distribution is
  logical (separate databases, separate connections, separate failures), not physical.
  Running three clusters on different ports needs only changes in `connect()`.
* The query processor handles a fixed set of global queries, not arbitrary SQL. Each phase of
  processing is real (catalog lookup, fragment elimination, partial aggregation, merge).
* Deadlock detection reads `pg_stat_activity` and `pg_blocking_pids`, which see the whole
  cluster; with separate clusters the detector would poll each cluster the same way.
