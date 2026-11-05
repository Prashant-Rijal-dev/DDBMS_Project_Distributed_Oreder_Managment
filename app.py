#!/usr/bin/env python3
"""
Web dashboard for the Distributed Order Management System.

    pip install flask
    python3 app.py            ->  open http://127.0.0.1:5000

Uses the same engine as demo.py (ddbms.py), so everything shown here is real:
real 2PC against the three PostgreSQL sites, real locks, real recovery.
"""
import datetime
import os
import subprocess
import sys
import threading
from decimal import Decimal

from flask import Flask, jsonify, render_template, request

import seed
from ddbms import (REGIONS, SITE_DB, SITE_NAME, QUERIES, BusinessError, Catalog,
                   CoordinatorLog, SimulatedCrash, connect, place_order,
                   prepared_xacts, q, recover, run_global_query)

BASE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)
LOG = CoordinatorLog()
EXCLUSIVE = threading.Lock()          # scenarios / reset reload data, so run one at a time
SCENARIO_TITLES = {
    "1": "Data dictionary, fragmentation, allocation",
    "2": "Global query processing",
    "3": "Distributed transaction with 2PC",
    "4": "Concurrency: lost update vs strict 2PL",
    "5": "Distributed deadlock detection",
    "6": "Crash and recovery",
    "7": "Triggers and replicated update",
    "8": "Access control",
}


def js(v):
    return str(v) if isinstance(v, (Decimal, datetime.date, datetime.datetime)) else v


def fetch(region, sql, params=None):
    c = connect(region, app="dashboard")
    try:
        cur = c.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [{k: js(v) for k, v in zip(cols, row)} for row in cur.fetchall()]
    finally:
        c.close()


def fail(msg, code=500, **extra):
    return jsonify(ok=False, error=msg, **extra), code


@app.get("/")
def index():
    return render_template("index.html", scenarios=SCENARIO_TITLES)


@app.get("/api/state")
def api_state():
    try:
        pending = prepared_xacts()
        sites = []
        for r in REGIONS:
            sites.append(dict(
                region=r, name=SITE_NAME[r], db=SITE_DB[r],
                inventory=fetch(r, """SELECT i.prod_id, p.name, i.warehouse_id, i.stock, i.reorder_level
                                      FROM inventory i JOIN product p USING (prod_id) ORDER BY i.prod_id"""),
                order_count=fetch(r, "SELECT count(*) AS n FROM orders")[0]["n"],
                orders=fetch(r, "SELECT order_id, cust_id, order_date, status FROM orders ORDER BY order_id DESC LIMIT 6"),
                restocks=fetch(r, "SELECT req_id, prod_id, qty_requested, status FROM restock_request ORDER BY req_id DESC LIMIT 6"),
                prepared=[g for s, g in pending if s == SITE_NAME[r]],
            ))
        return jsonify(ok=True, sites=sites)
    except Exception as e:
        return fail(str(e).splitlines()[0])


@app.get("/api/customers")
def api_customers():
    try:
        out = []
        for r in REGIONS:
            for row in fetch(r, "SELECT cust_id, name FROM customer_public ORDER BY cust_id"):
                out.append(dict(row, region=r, site=SITE_NAME[r]))
        return jsonify(ok=True, customers=out)
    except Exception as e:
        return fail(str(e).splitlines()[0])


@app.post("/api/order")
def api_order():
    d = request.get_json(force=True)
    trace = []
    try:
        cust = int(d["cust_id"])
        home = {"1": "N", "2": "W", "3": "S"}[str(cust)[0]]
        fail_at = d.get("fail_at") or None
        res = place_order(LOG, cust, home, [(d["prod_id"], int(d["qty"]))],
                          use_locking=bool(d.get("use_locking", True)),
                          fail_at=fail_at, trace=trace.append)
        return jsonify(ok=True, trace=trace, result=res)
    except SimulatedCrash as e:
        return jsonify(ok=False, crashed=True, error=f"Coordinator crashed ({e}). Run recovery to resolve it.", trace=trace)
    except BusinessError as e:
        return jsonify(ok=False, error=str(e), trace=trace)
    except Exception as e:
        return jsonify(ok=False, error=str(e).splitlines()[0], trace=trace)


@app.post("/api/recover")
def api_recover():
    lines = []
    try:
        recover(LOG, say=lines.append)
        return jsonify(ok=True, lines=lines or ["Nothing to recover: no in-doubt transactions."])
    except Exception as e:
        return fail(str(e).splitlines()[0], lines=lines)


@app.post("/api/query")
def api_query():
    d = request.get_json(force=True)
    lines = []
    try:
        gq = QUERIES[d["key"]]
        params = (float(d.get("threshold") or 3000),) if gq.key == "Q3" else None
        headers, rows = run_global_query(Catalog(), gq, region=d.get("region") or None,
                                         params=params, say=lines.append)
        return jsonify(ok=True, steps="\n".join(lines), headers=headers,
                       rows=[[str(x) for x in r] for r in rows])
    except Exception as e:
        return fail(str(e).splitlines()[0], steps="\n".join(lines))


@app.post("/api/reset")
def api_reset():
    if not EXCLUSIVE.acquire(blocking=False):
        return fail("Another scenario is running.", 409)
    try:
        seed.reset_all(verbose=False)
        return jsonify(ok=True)
    except Exception as e:
        return fail(str(e).splitlines()[0])
    finally:
        EXCLUSIVE.release()


@app.post("/api/scenario")
def api_scenario():
    n = str(request.get_json(force=True).get("n"))
    if n not in SCENARIO_TITLES:
        return fail("Unknown scenario", 400)
    if not EXCLUSIVE.acquire(blocking=False):
        return fail("Another scenario is running. Wait for it to finish.", 409)
    try:
        p = subprocess.run([sys.executable, "demo.py", n], cwd=BASE, capture_output=True,
                           text=True, timeout=240, env=dict(os.environ, PYTHONUNBUFFERED="1"))
        return jsonify(ok=p.returncode == 0, output=p.stdout + (("\n" + p.stderr) if p.stderr else ""))
    except subprocess.TimeoutExpired:
        return fail("Scenario timed out after 240 s (a prepared transaction may be holding locks: use Reset).")
    finally:
        EXCLUSIVE.release()


@app.get("/api/catalog")
def api_catalog():
    try:
        c = connect(None, app="dashboard")
        cur = c.cursor()
        cur.execute("""SELECT f.frag_name, f.rel_name, f.frag_type, coalesce(f.predicate,'-') AS predicate,
                              coalesce(f.columns,'all') AS columns, a.site_id AS site, a.local_table
                       FROM fragment f JOIN allocation a USING (frag_name)
                       ORDER BY f.frag_group, a.site_id""")
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        c.close()
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        return fail(str(e).splitlines()[0])


@app.get("/api/log")
def api_log():
    recs = LOG.records()[-60:]
    return jsonify(ok=True, rows=[
        dict(ts=r["ts"], txn=r["txn"], event=r["event"],
             details=", ".join(f"{k}={v}" for k, v in r.items() if k not in ("ts", "txn", "event")))
        for r in recs])


if __name__ == "__main__":
    print("Dashboard running at http://127.0.0.1:5000  (Ctrl+C to stop)")
    app.run(host="127.0.0.1", port=5000, threaded=True, debug=False)
