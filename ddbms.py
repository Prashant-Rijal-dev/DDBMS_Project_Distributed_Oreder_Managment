"""
ddbms.py - mini Distributed DBMS layer on top of three PostgreSQL databases.

Components
  Catalog               reads the global data dictionary (ddbms_catalog)
  run_global_query()    global query processor: decomposition -> localization
                        -> reduction -> optimisation -> execution -> merge
  CoordinatorLog        write-ahead log of the 2PC coordinator (JSON lines)
  DistTxn               distributed transaction with two-phase commit
  place_order()         business transaction using DistTxn (+ strict 2PL)
  DeadlockDetector      global wait-for-graph deadlock detection
  recover()             log-based recovery (redo committed / undo undecided)
"""
import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import psycopg2
from psycopg2 import errors as pgerr  # noqa: F401  (re-exported for demo.py)

HOST = os.getenv("PGHOST", "localhost")
PORT = int(os.getenv("PGPORT", "5432"))
ADMIN = os.getenv("PGUSER", os.getenv("USER", "postgres"))
ADMIN_PASSWORD = os.getenv("PGPASSWORD") or None
ROLE_PASSWORD = "ddbms"                      # password of lab roles (roles.sql)

REGIONS = ["N", "W", "S"]
SITE_DB = {"N": "ddbms_north", "W": "ddbms_west", "S": "ddbms_south"}
SITE_NAME = {"N": "North", "W": "West", "S": "South"}
HOME_WH = {"N": "WH-N", "W": "WH-W", "S": "WH-S"}
CATALOG_DB = "ddbms_catalog"
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "coordinator.log")


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def connect(region=None, user=None, app="ddbms", autocommit=True):
    """Connect to a site (region N/W/S) or to the catalog (region=None)."""
    conn = psycopg2.connect(
        host=HOST, port=PORT,
        dbname=SITE_DB[region] if region else CATALOG_DB,
        user=user or ADMIN,
        password=ROLE_PASSWORD if user else ADMIN_PASSWORD,
        application_name=app,
    )
    conn.autocommit = autocommit
    return conn


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def table(headers, rows):
    rows = [["" if v is None else str(v) for v in r] for r in rows]
    headers = [str(h) for h in headers]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(v))
    line = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    out = [line, "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |", line]
    for r in rows:
        out.append("| " + " | ".join(v.ljust(w) for v, w in zip(r, widths)) + " |")
    out.append(line)
    return "\n".join(out)


def indent(text, n=4):
    pad = " " * n
    return "\n".join(pad + l for l in str(text).splitlines())


# ----------------------------------------------------------------------
# 1. Data dictionary + global query processor
# ----------------------------------------------------------------------
class Catalog:
    def __init__(self):
        c = connect(None, app="catalog")
        self.rows = [
            dict(frag_name=a, group=b, ftype=c_, predicate=d, region=e, site=f, local=g)
            for a, b, c_, d, e, f, g in q(c, """
                SELECT f.frag_name, f.frag_group, f.frag_type, f.predicate,
                       f.region_code, a.site_id, a.local_table
                FROM fragment f JOIN allocation a USING (frag_name)
                ORDER BY f.frag_group, a.site_id""")]
        c.close()

    def fragments(self, group):
        return [r for r in self.rows if r["group"] == group]

    def is_partitioned(self, group):
        return any(r["region"] for r in self.fragments(group))

    def local_table(self, group, site):
        for r in self.fragments(group):
            if r["site"] == site:
                return r["local"]
        raise KeyError(f"{group} not allocated at {site}")


def _merge_sum_by_key(parts):
    agg = defaultdict(Decimal)
    for _, rows in parts:
        for k, v in rows:
            agg[k] += v
    return ["category", "total_sales"], sorted(agg.items(), key=lambda kv: -kv[1])


def _merge_avg_order(parts):
    tot, cnt = defaultdict(Decimal), defaultdict(int)
    for _, rows in parts:
        for region, total, n in rows:
            tot[region] += total
            cnt[region] += n
    out = [(r, cnt[r], tot[r], round(tot[r] / cnt[r], 2)) for r in sorted(tot)]
    T, N = sum(tot.values()), sum(cnt.values())
    if N:
        out.append(("ALL", N, T, round(T / N, 2)))
    return ["region", "orders", "total", "avg_order_value"], out


def _merge_concat_sorted(headers, key):
    def f(parts):
        rows = [r for _, rs in parts for r in rs]
        return headers, sorted(rows, key=key)
    return f


def _merge_sum_counts(parts):
    agg = defaultdict(int)
    for _, rows in parts:
        for k, v in rows:
            agg[k] += v
    return ["region", "customers"], sorted(agg.items())


class GQ:
    """A global query: algebra text, relations (fragment groups), local SQL template."""
    def __init__(self, key, title, algebra, relations, sql, merge, notes=()):
        self.key, self.title, self.algebra = key, title, algebra
        self.relations, self.sql, self.merge, self.notes = relations, sql, merge, notes


QUERIES = {
    "Q1": GQ("Q1", "Total sales per product category across all regions",
             "GAMMA category; SUM(qty*unit_price) ( SIGMA status<>'CANCELLED' (ORDERS JOIN ORDER_ITEM JOIN PRODUCT) )",
             ["ORDERS", "ORDER_ITEM", "PRODUCT"],
             """SELECT p.category, SUM(oi.qty * oi.unit_price)
FROM {ORDERS} o
JOIN {ORDER_ITEM} oi ON oi.order_id = o.order_id
JOIN {PRODUCT} p ON p.prod_id = oi.prod_id
WHERE o.status <> 'CANCELLED'
GROUP BY p.category""",
             _merge_sum_by_key,
             ["SUM is distributive: each site returns a partial SUM per category, the coordinator adds them."]),
    "Q2": GQ("Q2", "Average order value per region (and overall)",
             "GAMMA region; SUM(total), COUNT(order_id) ( ORDERS JOIN ORDER_ITEM )  then  AVG = SUM / COUNT",
             ["ORDERS", "ORDER_ITEM"],
             """SELECT o.region, SUM(oi.qty * oi.unit_price), COUNT(DISTINCT o.order_id)
FROM {ORDERS} o
JOIN {ORDER_ITEM} oi ON oi.order_id = o.order_id
WHERE o.status <> 'CANCELLED'
GROUP BY o.region""",
             _merge_avg_order,
             ["AVG is NOT distributive: averaging per-site averages is wrong.",
              "Rewrite AVG as (SUM, COUNT) at each site and divide at the coordinator."]),
    "Q3": GQ("Q3", "Orders above a threshold in region N only",
             "SIGMA total > X ( GAMMA order_id; SUM(qty*unit_price) ( SIGMA region='N' (ORDERS) JOIN ORDER_ITEM ) )",
             ["ORDERS", "ORDER_ITEM"],
             """SELECT o.order_id, o.cust_id, SUM(oi.qty * oi.unit_price) AS total
FROM {ORDERS} o
JOIN {ORDER_ITEM} oi ON oi.order_id = o.order_id
GROUP BY o.order_id, o.cust_id
HAVING SUM(oi.qty * oi.unit_price) > %s""",
             _merge_concat_sorted(["order_id", "cust_id", "total"], lambda r: -r[2]),
             ["The predicate region='N' contradicts the predicates of ORDERS_W and ORDERS_S, so those fragments are eliminated."]),
    "Q4": GQ("Q4", "Low-stock items across all warehouses",
             "PI city,prod_id,name,stock ( SIGMA stock < reorder_level (INVENTORY JOIN PRODUCT JOIN WAREHOUSE) )",
             ["INVENTORY", "PRODUCT", "WAREHOUSE"],
             """SELECT w.city, i.prod_id, p.name, i.stock, i.reorder_level
FROM {INVENTORY} i
JOIN {PRODUCT} p ON p.prod_id = i.prod_id
JOIN {WAREHOUSE} w ON w.warehouse_id = i.warehouse_id
WHERE i.stock < i.reorder_level""",
             _merge_concat_sorted(["city", "prod_id", "name", "stock", "reorder_level"], lambda r: (r[3], r[0])),
             ["PRODUCT and WAREHOUSE are replicated, so the joins are evaluated locally with no data shipping."]),
    "Q5": GQ("Q5", "Customers per region (name/region only, no e-mail)",
             "GAMMA region; COUNT(*) ( PI cust_id,name,region (CUSTOMER) )",
             ["CUSTOMER_PUB"],
             """SELECT c.region, COUNT(*) FROM {CUSTOMER_PUB} c GROUP BY c.region""",
             _merge_sum_counts,
             ["Vertical reduction: the attributes needed {cust_id,name,region} are all in CUSTOMER_PUB,",
              "so the sensitive fragment CUSTOMER_SEC_* is never touched."]),
}


def run_global_query(cat, gq, region=None, params=None, say=print):
    """Process a global query step by step, printing every phase."""
    say(f"\n  QUERY {gq.key}: {gq.title}")
    say("  " + "-" * 70)
    say("  STEP 1  Global query (relational algebra)")
    say(indent(gq.algebra + (f"   [region = '{region}']" if region else ""), 6))

    say("  STEP 2  Decomposition: normalise, analyse, simplify")
    say(f"      relations used : {', '.join(gq.relations)}")
    joins = max(len(gq.relations) - 1, 0)
    say(f"      join graph     : {joins} equi-join(s) (order_id / prod_id / warehouse_id), no cross products")
    say("      simplification : redundant predicates removed, query is conjunctive and valid")

    say("  STEP 3  Data localization (using the data dictionary)")
    target_sites = [s for s in REGIONS if region in (None, s)]
    partitioned = [g for g in gq.relations if cat.is_partitioned(g)]
    for g in gq.relations:
        frs = cat.fragments(g)
        if cat.is_partitioned(g):
            say(f"      {g} = " + " U ".join(sorted({f['frag_name'] for f in frs})))
        else:
            say(f"      {g} = {frs[0]['frag_name']}  (replicated at every site -> read the local copy)")

    say("  STEP 4  Reduction")
    if region:
        gone = [f["frag_name"] for g in partitioned for f in cat.fragments(g) if f["region"] != region]
        say(f"      fragment elimination: predicate region='{region}' contradicts -> removed {len(gone)}: {', '.join(gone)}")
    else:
        say("      no selection on the fragmentation attribute -> all sites must be contacted")
    if len(partitioned) > 1:
        say(f"      join reduction: ORDERS_i JOIN ORDER_ITEM_j = empty for i<>j (derived fragmentation),")
        say(f"      so {len(target_sites)} local joins replace {len(target_sites) ** 2} fragment joins")
    for n in gq.notes:
        say(f"      note: {n}")

    say("  STEP 5  Optimisation: push selections, joins and partial aggregation to the sites;")
    say("          ship only small partial results to the coordinator")

    say("  STEP 6  Execution (one thread per site, in parallel)")

    def at_site(s):
        mapping = {g: cat.local_table(g, s) for g in gq.relations}
        sql = gq.sql.format(**mapping)
        t0 = time.time()
        c = connect(s, app="gqp")
        try:
            rows = q(c, sql, params)
        finally:
            c.close()
        return s, sql, rows, (time.time() - t0) * 1000

    with ThreadPoolExecutor(max_workers=len(target_sites)) as ex:
        results = list(ex.map(at_site, target_sites))
    for s, sql, rows, ms in results:
        say(f"      -- site {SITE_NAME[s]} ({SITE_DB[s]}): {len(rows)} row(s) in {ms:.1f} ms")
        say(indent(sql, 8))

    say("  STEP 7  Combine partial results at the coordinator")
    headers, rows = gq.merge([(s, rows) for s, _, rows, _ in results])
    say(indent(table(headers, rows), 6))
    return headers, rows


# ----------------------------------------------------------------------
# 2. Coordinator log + distributed transactions (2PC)
# ----------------------------------------------------------------------
class SimulatedCrash(Exception):
    pass


class BusinessError(Exception):
    pass


class CoordinatorLog:
    def __init__(self, path=LOG_PATH):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.lock = threading.Lock()

    def reset(self):
        with self.lock:
            open(self.path, "w").close()

    def _append(self, txn, event, **data):
        rec = {"ts": time.strftime("%H:%M:%S"), "txn": txn, "event": event}
        rec.update(data)
        with open(self.path, "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def write(self, txn, event, **data):
        with self.lock:
            self._append(txn, event, **data)

    def records(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [json.loads(l) for l in f if l.strip()]

    def new_txn(self):
        with self.lock:
            n = max([int(r["txn"][1:]) for r in self.records()] + [0]) + 1
            txn = f"T{n}"
            self._append(txn, "BEGIN")
            return txn

    def replay(self):
        st = {}
        for r in self.records():
            s = st.setdefault(r["txn"], dict(participants=[], decision=None, end=False))
            if r["event"] == "PREPARE_START":
                s["participants"] = r["participants"]
            elif r["event"] == "DECISION":
                s["decision"] = r["decision"]
            elif r["event"] == "END":
                s["end"] = True
        return st

    def pretty(self):
        rows = []
        for r in self.records():
            extra = {k: v for k, v in r.items() if k not in ("ts", "txn", "event")}
            rows.append((r["ts"], r["txn"], r["event"], json.dumps(extra) if extra else ""))
        return table(["time", "txn", "log record", "details"], rows)


class DistTxn:
    """One global transaction = one local branch per site touched."""

    def __init__(self, log, trace=None):
        self.log = log
        self.id = log.new_txn()
        self.conns = {}
        self.trace = trace or (lambda *_: None)

    def _conn(self, region):
        if region not in self.conns:
            c = connect(region, app=self.id)
            q(c, "BEGIN")
            q(c, "SELECT set_config('ddbms.txn', %s, true)", (self.id,))
            self.conns[region] = c
        return self.conns[region]

    def query(self, region, sql, params=None):
        return q(self._conn(region), sql, params)

    def gid(self, region):
        return f"{self.id}@{region}"

    def abort(self, reason=""):
        for c in self.conns.values():
            try:
                q(c, "ROLLBACK")
            except Exception:
                pass
            try:
                c.close()
            except Exception:
                pass
        self.conns = {}
        self.log.write(self.id, "END", decision="ABORT", reason=reason)
        self.trace(f"[{self.id}] ABORTED ({reason}) - all local branches rolled back")

    def _crash(self):
        for c in self.conns.values():
            try:
                c.close()          # prepared branches survive in PostgreSQL
            except Exception:
                pass
        self.conns = {}

    def commit_2pc(self, fail_at=None):
        t = self.trace
        parts = list(self.conns)
        self.log.write(self.id, "PREPARE_START", participants=parts)
        t(f"[{self.id}] 2PC PHASE 1 (voting): PREPARE sent to {[SITE_NAME[p] for p in parts]}")
        votes = {}
        for r in parts:
            try:
                q(self.conns[r], f"PREPARE TRANSACTION '{self.gid(r)}'")
                votes[r] = "YES"
            except Exception as e:
                votes[r] = "NO"
                try:
                    q(self.conns[r], "ROLLBACK")
                except Exception:
                    pass
                t(f"[{self.id}]   {SITE_NAME[r]} cannot prepare: {str(e).splitlines()[0]}")
            self.log.write(self.id, "VOTE", site=r, vote=votes[r])
            t(f"[{self.id}]   {SITE_NAME[r]} votes {votes[r]}")

        if fail_at == "after_prepare":
            t(f"[{self.id}] *** COORDINATOR CRASH after PREPARE, before logging a decision ***")
            self._crash()
            raise SimulatedCrash("crash after prepare")

        decision = "COMMIT" if all(v == "YES" for v in votes.values()) else "ABORT"
        self.log.write(self.id, "DECISION", decision=decision)
        t(f"[{self.id}] DECISION = {decision} (forced to the coordinator log)")

        if fail_at == "after_decision":
            t(f"[{self.id}] *** COORDINATOR CRASH after logging {decision}, before telling the sites ***")
            self._crash()
            raise SimulatedCrash("crash after decision")

        t(f"[{self.id}] 2PC PHASE 2: sending {decision} to participants")
        for r in parts:
            if votes[r] == "YES":
                verb = "COMMIT PREPARED" if decision == "COMMIT" else "ROLLBACK PREPARED"
                q(self.conns[r], f"{verb} '{self.gid(r)}'")
        self.log.write(self.id, "END", decision=decision)
        for c in self.conns.values():
            c.close()
        self.conns = {}
        t(f"[{self.id}] {decision}TED" if decision == "COMMIT" else f"[{self.id}] ABORTED")
        return decision


def place_order(log, cust_id, home, items, hold=0.0, use_locking=True,
                fail_at=None, trace=None):
    """
    Place an order for a customer of site `home`.
    items = [(prod_id, qty)].  Each item is shipped from the home warehouse if it has
    enough stock, otherwise from the first other warehouse that does.
    use_locking=True  -> SELECT ... FOR UPDATE (strict 2PL: locks held until commit)
    use_locking=False -> naive read-then-write (shows the lost-update anomaly)
    """
    trace = trace or (lambda *_: None)
    txn = DistTxn(log, trace)
    try:
        if not txn.query(home, "SELECT 1 FROM customer_public WHERE cust_id = %s", (cust_id,)):
            raise BusinessError(f"customer {cust_id} is not stored at site {SITE_NAME[home]}")
        order_id = txn.query(home, "SELECT nextval('order_seq')")[0][0]
        txn.query(home, "INSERT INTO orders(order_id, cust_id, region) VALUES (%s,%s,%s)",
                  (order_id, cust_id, home))
        trace(f"[{txn.id}] order {order_id} created at {SITE_NAME[home]} for customer {cust_id}")

        plan = []
        for prod, qty in items:
            price = txn.query(home, "SELECT price FROM product WHERE prod_id = %s", (prod,))[0][0]
            chosen = None
            for r in [home] + [x for x in REGIONS if x != home]:
                wh = HOME_WH[r]
                lock = " FOR UPDATE" if use_locking else ""
                rows = txn.query(r, "SELECT stock FROM inventory WHERE prod_id=%s AND warehouse_id=%s" + lock,
                                 (prod, wh))
                avail = rows[0][0] if rows else 0
                trace(f"[{txn.id}] {prod}: {SITE_NAME[r]} warehouse has {avail} "
                      f"({'row locked' if use_locking else 'NO lock'}), need {qty}")
                if avail >= qty:
                    chosen = (r, wh, avail)
                    break
            if not chosen:
                raise BusinessError(f"insufficient stock for {prod} in every warehouse")
            r, wh, avail = chosen
            if hold:
                time.sleep(hold)          # user "thinks" before confirming (makes races visible)
            if use_locking:
                txn.query(r, "UPDATE inventory SET stock = stock - %s WHERE prod_id=%s AND warehouse_id=%s",
                          (qty, prod, wh))
            else:
                txn.query(r, "UPDATE inventory SET stock = %s WHERE prod_id=%s AND warehouse_id=%s",
                          (avail - qty, prod, wh))
            txn.query(home, "INSERT INTO order_item VALUES (%s,%s,%s,%s,%s)",
                      (order_id, prod, qty, price, wh))
            plan.append((prod, qty, wh, SITE_NAME[r]))
            trace(f"[{txn.id}] {prod} x{qty} will ship from {wh} ({SITE_NAME[r]})")
    except BusinessError as e:
        txn.abort(str(e))
        raise
    except pgerr.QueryCanceled:
        txn.abort("cancelled by deadlock detector")
        raise
    except Exception as e:                      # any other DB error: abort every branch
        txn.abort(str(e).splitlines()[0])
        raise
    decision = txn.commit_2pc(fail_at)
    if decision != "COMMIT":
        raise BusinessError("a participant voted NO - order aborted")
    return dict(order_id=order_id, plan=plan, txn=txn.id, sites=sorted({SITE_NAME[home]} | {p[3] for p in plan}))


# ----------------------------------------------------------------------
# 3. Global deadlock detection (wait-for graph)
# ----------------------------------------------------------------------
WFG_SQL = """
SELECT w.application_name, w.pid, h.application_name
FROM pg_stat_activity w
CROSS JOIN LATERAL unnest(pg_blocking_pids(w.pid)) AS b(pid)
JOIN pg_stat_activity h ON h.pid = b.pid
WHERE w.datname = current_database()
"""
_TXN = re.compile(r"T\d+")


def find_cycle(edges):
    g = defaultdict(set)
    for w, h in edges:
        g[w].add(h)
    color, stack = {}, []

    def dfs(u):
        color[u] = 1
        stack.append(u)
        for v in g.get(u, ()):
            if color.get(v, 0) == 0:
                c = dfs(v)
                if c:
                    return c
            elif color.get(v) == 1:
                return stack[stack.index(v):] + [v]
        stack.pop()
        color[u] = 2
        return None

    for n in list(g):
        if color.get(n, 0) == 0:
            c = dfs(n)
            if c:
                return c
    return None


class DeadlockDetector(threading.Thread):
    """Periodically unions the local wait-for graphs of all sites; kills the youngest txn in a cycle."""

    def __init__(self, interval=0.4, trace=print):
        super().__init__(daemon=True)
        self.interval, self.trace = interval, trace
        self.stop_evt = threading.Event()
        self.victims = []

    def run(self):
        conns = {r: connect(r, app="deadlock-detector") for r in REGIONS}
        try:
            while not self.stop_evt.is_set():
                local, edges = {}, []
                for r, c in conns.items():
                    local[r] = []
                    for w, pid, h in q(c, WFG_SQL):
                        if w and h and _TXN.fullmatch(w) and _TXN.fullmatch(h):
                            local[r].append((w, h))
                            edges.append((w, h, r, pid))
                cyc = find_cycle([(w, h) for w, h, _, _ in edges])
                if cyc:
                    self.trace("DEADLOCK DETECTOR: local wait-for graphs (each one is cycle-free):")
                    for r in REGIONS:
                        txt = ", ".join(f"{w} -> {h}" for w, h in local[r]) or "(no waits)"
                        self.trace(f"     {SITE_NAME[r]:5s} site: {txt}")
                    self.trace("DEADLOCK DETECTOR: GLOBAL graph has a cycle: " + " -> ".join(cyc))
                    victim = max(set(cyc), key=lambda t: int(t[1:]))
                    self.trace(f"DEADLOCK DETECTOR: victim = {victim} (youngest transaction) - cancelling its wait")
                    for w, h, r, pid in edges:
                        if w == victim:
                            q(conns[r], "SELECT pg_cancel_backend(%s)", (pid,))
                            break
                    self.victims.append(victim)
                    self.stop_evt.wait(self.interval * 4)
                else:
                    self.stop_evt.wait(self.interval)
        finally:
            for c in conns.values():
                c.close()

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=3)


# ----------------------------------------------------------------------
# 4. Recovery
# ----------------------------------------------------------------------
def prepared_xacts():
    rows = []
    for r in REGIONS:
        c = connect(r, app="inspect")
        for (gid,) in q(c, "SELECT gid FROM pg_prepared_xacts WHERE database = current_database()"):
            rows.append((SITE_NAME[r], gid))
        c.close()
    return rows


def recover(log, say=print):
    """
    Restart recovery after a coordinator crash.
      decision = COMMIT logged            -> REDO  : COMMIT PREPARED at every participant
      decision = ABORT or none logged     -> UNDO  : ROLLBACK PREPARED (presumed abort)
      branch prepared but unknown to log  -> UNDO  (orphan)
    """
    state = log.replay()
    known = set()
    for txn, st in state.items():
        known.add(txn)
        if st["end"]:
            continue
        if not st["participants"]:
            say(f"  {txn}: crashed before PREPARE -> branches were rolled back automatically by the sites")
            log.write(txn, "END", decision="ABORT", recovered=True)
            continue
        redo = st["decision"] == "COMMIT"
        verb = "COMMIT PREPARED" if redo else "ROLLBACK PREPARED"
        why = "decision COMMIT is in the log -> REDO" if redo else "no COMMIT decision in the log -> UNDO (presumed abort)"
        say(f"  {txn}: in doubt; {why}")
        for r in st["participants"]:
            c = connect(r, app="recovery")
            gid = f"{txn}@{r}"
            if q(c, "SELECT 1 FROM pg_prepared_xacts WHERE gid=%s AND database=current_database()", (gid,)):
                q(c, f"{verb} '{gid}'")
                say(f"      {SITE_NAME[r]:5s}: {verb} '{gid}'")
            c.close()
        log.write(txn, "END", decision="COMMIT" if redo else "ABORT", recovered=True)
    for r in REGIONS:                      # orphan scan
        c = connect(r, app="recovery")
        for (gid,) in q(c, "SELECT gid FROM pg_prepared_xacts WHERE database=current_database()"):
            if gid.split("@")[0] not in known:
                q(c, f"ROLLBACK PREPARED '{gid}'")
                say(f"      {SITE_NAME[r]:5s}: orphan '{gid}' rolled back")
        c.close()


def inventory_snapshot(prod):
    rows = []
    for r in REGIONS:
        c = connect(r, app="inspect")
        for wh, stock in q(c, "SELECT warehouse_id, stock FROM inventory WHERE prod_id=%s", (prod,)):
            rows.append((SITE_NAME[r], wh, stock))
        c.close()
    return table(["site", "warehouse", f"stock of {prod}"], rows)
