#!/usr/bin/env python3
"""
Demo driver.   python demo.py check | all | 1..8
  1 data dictionary, fragmentation, allocation
  2 global query processing (decomposition -> localization -> merge)
  3 distributed transaction with two-phase commit
  4 concurrency control: lost update vs strict 2PL
  5 distributed deadlock: global wait-for graph
  6 recovery after a coordinator/site crash (redo / undo)
  7 triggers (active database) + replicated price update
  8 access control (roles, views, GRANT / REVOKE)
"""
import sys
import threading
import time

import psycopg2

import seed
from ddbms import (REGIONS, SITE_NAME, SITE_DB, QUERIES, BusinessError, Catalog,
                   CoordinatorLog, DeadlockDetector, DistTxn, SimulatedCrash,
                   connect, inventory_snapshot, pgerr, place_order, prepared_xacts,
                   q, recover, run_global_query, table, indent)

LOG = CoordinatorLog()
T0 = time.time()
_print_lock = threading.Lock()


def tr(msg):
    with _print_lock:
        print(f"    {time.time() - T0:5.2f}s  {msg}", flush=True)


def hdr(title):
    global T0
    T0 = time.time()
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def show(sql, region, title=None, params=None, user=None):
    c = connect(region, user=user, app="inspect")
    cur = c.cursor()
    cur.execute(sql, params)
    rows, cols = cur.fetchall(), [d[0] for d in cur.description]
    c.close()
    if title:
        print(f"  {title}")
    print(indent(table(cols, rows), 4))


def first_line(e):
    return str(e).strip().splitlines()[0]


# ----------------------------------------------------------------------
def check():
    hdr("ENVIRONMENT CHECK")
    for r in REGIONS:
        c = connect(r)
        v = q(c, "SHOW server_version")[0][0]
        m = q(c, "SHOW max_prepared_transactions")[0][0]
        c.close()
        print(f"  {SITE_NAME[r]:5s} {SITE_DB[r]:12s} PostgreSQL {v}, max_prepared_transactions={m}")
        if m == "0":
            print("   !! set max_prepared_transactions > 0 (see README) - 2PC needs it")
    Catalog()
    print("  catalog reachable: OK")


# ----------------------------------------------------------------------
def s1():
    hdr("SCENARIO 1  Data dictionary, fragmentation and allocation  (Syllabus 1.3)")
    c = connect(None)
    cur = c.cursor()
    cur.execute("SELECT rel_name, attributes FROM global_relation ORDER BY 1")
    print("  GLOBAL SCHEMA (what the user sees):")
    print(indent(table(["relation", "attributes"], cur.fetchall()), 4))
    cur.execute("""SELECT frag_group AS fragment_set, frag_type, coalesce(predicate,'-') AS predicate,
                          coalesce(columns,'all columns') AS columns
                   FROM fragment WHERE region_code = 'N' OR region_code IS NULL ORDER BY 1""")
    print("\n  FRAGMENTATION (shown for site N; W and S use region 'W' / 'S'):")
    print(indent(table([d[0] for d in cur.description], cur.fetchall()), 4))
    cur.execute("""SELECT a.frag_name, a.site_id, a.local_table, a.access_freq
                   FROM allocation a JOIN fragment f USING (frag_name)
                   ORDER BY f.frag_group, a.site_id""")
    print("\n  ALLOCATION (fragment -> site -> local table):")
    print(indent(table(["fragment", "site", "local table", "access frequency"], cur.fetchall()), 4))
    c.close()
    print("\n  Architecture class: MPMD - several processes (coordinator + 3 DBMS site processes)")
    print("  working on multiple data sites (three autonomous databases).")


def s2():
    hdr("SCENARIO 2  Global query processing  (Syllabus 1.5 / 2.1)")
    seed.reset_all(verbose=False)
    cat = Catalog()
    run_global_query(cat, QUERIES["Q1"])
    run_global_query(cat, QUERIES["Q2"])
    run_global_query(cat, QUERIES["Q3"], region="N", params=(3000,))
    run_global_query(cat, QUERIES["Q4"])
    run_global_query(cat, QUERIES["Q5"])


def s3():
    hdr("SCENARIO 3  Distributed transaction with two-phase commit  (Syllabus 2.2)")
    seed.reset_all(verbose=False)
    print("  North customer 1001 orders: 1 x Smart Watch (P011, stocked ONLY in the West)")
    print("                              2 x Green Tea   (P010, available in the North)")
    print("  Stock before:")
    print(indent(inventory_snapshot("P011"), 4))
    res = place_order(LOG, 1001, "N", [("P011", 1), ("P010", 2)], trace=tr)
    print(f"\n  RESULT: order {res['order_id']} committed atomically at sites {res['sites']}")
    show("SELECT * FROM order_item WHERE order_id = %s", "N", "  Order lines stored at North:", (res["order_id"],))
    print("\n  Stock after (West stock dropped 5 -> 4, below reorder level 5):")
    print(indent(inventory_snapshot("P011"), 4))
    show("SELECT prod_id, warehouse_id, qty_requested, status FROM restock_request", "W",
         "\n  Trigger fired at West - automatic restock request:")
    print("\n  Coordinator log:")
    print(indent(LOG.pretty(), 4))


def _two_buyers(use_locking):
    seed.reset_all(verbose=False)
    out = {}

    def buyer(cust, delay):
        time.sleep(delay)
        try:
            r = place_order(LOG, cust, "S", [("P012", 1)], hold=2.0,
                            use_locking=use_locking, trace=tr)
            out[cust] = f"order {r['order_id']} CONFIRMED"
        except (BusinessError, pgerr.Error) as e:
            out[cust] = f"REJECTED ({first_line(e)})"

    global T0
    T0 = time.time()
    ts = [threading.Thread(target=buyer, args=(3001, 0.0)),
          threading.Thread(target=buyer, args=(3002, 0.4))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    print("\n  Outcome:")
    for cust, msg in out.items():
        print(f"    customer {cust}: {msg}")
    show("SELECT coalesce(sum(qty),0) AS units_sold_of_P012 FROM order_item WHERE prod_id='P012'", "S")
    show("SELECT stock FROM inventory WHERE prod_id='P012'", "S", "  Physical stock of P012 at South:")
    c = connect("S")
    sold = q(c, "SELECT coalesce(sum(qty),0) FROM order_item WHERE prod_id='P012'")[0][0]
    c.close()
    print("  >>> OVERSOLD: sold 2 units but only 1 existed!" if sold > 1
          else "  >>> CORRECT: exactly 1 unit sold, no overselling.")


def s4():
    hdr("SCENARIO 4a  WITHOUT locking: two buyers, ONE unit left  (lost update)")
    _two_buyers(False)
    hdr("SCENARIO 4b  WITH strict two-phase locking (SELECT ... FOR UPDATE)")
    _two_buyers(True)
    print("\n  Why it works: locks are held until the 2PC commit finishes (strict 2PL), so the")
    print("  second buyer waits, then re-reads stock = 0 and is rejected. Schedules are serialisable.")


def s5():
    hdr("SCENARIO 5  Distributed deadlock and global wait-for graph  (Syllabus 2.2)")
    seed.reset_all(verbose=False)
    print("  T-a moves 1 unit of P001 North -> West, T-b moves 1 unit West -> North.")
    print("  Each locks its source row first, then wants the other one: a cross-site cycle.")
    print("  PostgreSQL cannot see it: every site only sees ONE wait.\n")
    det = DeadlockDetector(trace=tr)
    det.start()
    outcome = []

    def mover(src, dst, delay, label):
        time.sleep(delay)
        for attempt in (1, 2):
            txn = DistTxn(LOG, trace=tr)
            try:
                tr(f"[{txn.id}] ({label}) locking {SITE_NAME[src]} row of P001")
                txn.query(src, "SELECT stock FROM inventory WHERE prod_id='P001' AND warehouse_id=%s FOR UPDATE",
                          ("WH-" + src,))
                time.sleep(1.0)
                tr(f"[{txn.id}] ({label}) now requesting {SITE_NAME[dst]} row of P001")
                txn.query(dst, "SELECT stock FROM inventory WHERE prod_id='P001' AND warehouse_id=%s FOR UPDATE",
                          ("WH-" + dst,))
                txn.query(src, "UPDATE inventory SET stock = stock - 1 WHERE prod_id='P001' AND warehouse_id=%s",
                          ("WH-" + src,))
                txn.query(dst, "UPDATE inventory SET stock = stock + 1 WHERE prod_id='P001' AND warehouse_id=%s",
                          ("WH-" + dst,))
                txn.commit_2pc()
                outcome.append((txn.id, label, "COMMITTED" + (" (retry)" if attempt == 2 else "")))
                return
            except pgerr.QueryCanceled:
                txn.abort("deadlock victim")
                outcome.append((txn.id, label, "ABORTED (deadlock victim)"))
                time.sleep(2.5)       # back off, then retry as a new transaction

    ts = [threading.Thread(target=mover, args=("N", "W", 0.0, "N->W")),
          threading.Thread(target=mover, args=("W", "N", 0.2, "W->N"))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    det.stop()
    print("\n  Outcome:")
    print(indent(table(["txn", "transfer", "result"], sorted(outcome)), 4))
    print("  Victims chosen by the detector:", det.victims)
    for r in ("N", "W"):
        show("SELECT txn, prod_id, warehouse_id, old_stock, new_stock FROM stock_log ORDER BY log_id",
             r, f"  Stock log at {SITE_NAME[r]}:")


def s6():
    hdr("SCENARIO 6  Crash and recovery  (Syllabus 2.3)")
    seed.reset_all(verbose=False)

    print("  6a. Coordinator crashes AFTER sites voted YES but BEFORE a decision was logged")
    try:
        place_order(LOG, 1001, "N", [("P011", 1), ("P010", 1)], trace=tr, fail_at="after_prepare")
    except SimulatedCrash:
        pass
    print("\n  In-doubt (prepared) branches still holding locks:")
    print(indent(table(["site", "gid"], prepared_xacts()), 4))
    c = connect("W")
    q(c, "SET lock_timeout = '700ms'")
    try:
        q(c, "SELECT stock FROM inventory WHERE prod_id='P011' FOR UPDATE")
    except psycopg2.errors.LockNotAvailable:
        print("  A new transaction cannot touch P011 at West: it is BLOCKED by the in-doubt branch")
        print("  (this is the classic blocking problem of 2PC).")
    c.close()

    print("\n  6b. Coordinator crashes AFTER logging COMMIT but BEFORE telling the sites")
    try:
        place_order(LOG, 1002, "N", [("P005", 2)], trace=tr, fail_at="after_decision")
    except SimulatedCrash:
        pass
    print("\n  Prepared branches now:")
    print(indent(table(["site", "gid"], prepared_xacts()), 4))
    print("\n  Coordinator log at the moment of the crash:")
    print(indent(LOG.pretty(), 4))

    print("\n  --- RESTART: recovery manager reads the log ---")
    recover(LOG, say=print)
    print("\n  Prepared branches after recovery:", prepared_xacts() or "none")
    print("\n  Final state:")
    print(indent(inventory_snapshot("P011"), 4))
    print(indent(inventory_snapshot("P005"), 4))
    show("SELECT order_id, cust_id, status FROM orders WHERE order_id > 10100", "N",
         "  Orders created after the seed data at North (6a undone, 6b redone):")
    show("SELECT txn, prod_id, warehouse_id, old_stock, new_stock FROM stock_log ORDER BY log_id", "N",
         "  Before/after images at North:")
    print("\n  Final coordinator log:")
    print(indent(LOG.pretty(), 4))


def s7():
    hdr("SCENARIO 7  Triggers and replicated update  (Syllabus 3.1)")
    seed.reset_all(verbose=False)
    print("  7a. Price change of P003 must be applied at all 3 replicas atomically (2PC)")
    t = DistTxn(LOG, trace=tr)
    for r in REGIONS:
        t.query(r, "UPDATE product SET price = 649 WHERE prod_id = 'P003'")
    t.commit_2pc()
    for r in REGIONS:
        show("SELECT prod_id, old_price, new_price, changed_by FROM price_audit", r,
             f"  price_audit trigger at {SITE_NAME[r]}:")

    print("\n  7b. Auto-restock trigger: North stock of P005 is 6, reorder level 5")
    place_order(LOG, 1002, "N", [("P005", 2)], trace=tr)
    show("SELECT prod_id, warehouse_id, qty_requested, status FROM restock_request", "N",
         "\n  Restock request created by trigger (stock 6 -> 4):")
    place_order(LOG, 1003, "N", [("P005", 1)], trace=tr)
    show("SELECT prod_id, warehouse_id, qty_requested, status FROM restock_request", "N",
         "\n  Another sale (4 -> 3): no duplicate request while one is OPEN:")


def attempt(label, fn):
    try:
        res = fn()
        print(f"    [ALLOWED] {label}" + (f"  -> {res}" if res is not None else ""))
    except Exception as e:
        print(f"    [DENIED ] {label}\n               {first_line(e)}")


def as_user(user, region, sql):
    def run():
        c = connect(region, user=user, app="acl")
        try:
            rows = q(c, sql)
            return f"{len(rows)} row(s)" + (f", e.g. {rows[0]}" if rows else "")
        finally:
            c.close()
    return run


def s8():
    hdr("SCENARIO 8  Access control: roles, views, GRANT/REVOKE  (self-study topic)")
    seed.reset_all(verbose=False)
    print("  Role ddbms_admin")
    attempt("admin reads secure.customer_secure @North",
            as_user("ddbms_admin", "N", "SELECT cust_id, email FROM secure.customer_secure"))
    print("\n  Role mgr_north (regional manager)")
    attempt("reads v_order_summary @North", as_user("mgr_north", "N", "SELECT * FROM v_order_summary"))
    attempt("reads v_customer_masked @North (e-mail masked)",
            as_user("mgr_north", "N", "SELECT * FROM v_customer_masked"))
    attempt("reads secure.customer_secure @North", as_user("mgr_north", "N", "SELECT * FROM secure.customer_secure"))
    attempt("connects to the WEST site", as_user("mgr_north", "W", "SELECT 1"))
    print("\n  Role cust_1001 (customer)")
    attempt("reads own orders (v_my_orders) @North", as_user("cust_1001", "N", "SELECT * FROM v_my_orders"))
    attempt("reads all orders (orders table) @North", as_user("cust_1001", "N", "SELECT * FROM orders"))
    attempt("reads product catalogue @North", as_user("cust_1001", "N", "SELECT * FROM product"))
    attempt("connects to the SOUTH site", as_user("cust_1001", "S", "SELECT 1"))

    print("\n  REVOKE / GRANT demonstration on restock_request @North")
    c = connect("N")
    q(c, "REVOKE SELECT ON restock_request FROM regional_manager")
    print("    admin: REVOKE SELECT ON restock_request FROM regional_manager")
    attempt("mgr_north reads restock_request", as_user("mgr_north", "N", "SELECT * FROM restock_request"))
    q(c, "GRANT SELECT ON restock_request TO regional_manager")
    print("    admin: GRANT SELECT ON restock_request TO regional_manager")
    attempt("mgr_north reads restock_request", as_user("mgr_north", "N", "SELECT * FROM restock_request"))
    c.close()


SCENARIOS = {"1": s1, "2": s2, "3": s3, "4": s4, "5": s5, "6": s6, "7": s7, "8": s8}

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    if arg == "check":
        check()
    elif arg == "all":
        check()
        for k in sorted(SCENARIOS):
            SCENARIOS[k]()
    elif arg in SCENARIOS:
        SCENARIOS[arg]()
    else:
        print(__doc__)
