#!/usr/bin/env python3
"""Load (or reset) deterministic sample data at all three sites."""
import datetime
import random

from ddbms import REGIONS, SITE_NAME, CoordinatorLog, connect, q

PRODUCTS = [
    ("P001", "Wireless Earbuds",          "Electronics", 2499),
    ("P002", "Bluetooth Speaker",         "Electronics", 3299),
    ("P003", "Python Programming Book",   "Books",        599),
    ("P004", "Database Systems Textbook", "Books",        899),
    ("P005", "Cotton T-Shirt",            "Clothing",     499),
    ("P006", "Denim Jacket",              "Clothing",    1999),
    ("P007", "Steel Water Bottle",        "Home",         399),
    ("P008", "LED Desk Lamp",             "Home",         749),
    ("P009", "Basmati Rice 5kg",          "Grocery",      650),
    ("P010", "Green Tea Pack",            "Grocery",      280),
    ("P011", "Smart Watch",               "Electronics", 5999),
    ("P012", "Limited Edition Headphones", "Electronics", 8999),
]
WAREHOUSES = [("WH-N", "Delhi", "N"), ("WH-W", "Mumbai", "W"), ("WH-S", "Bengaluru", "S")]
HOME_WH = {"N": "WH-N", "W": "WH-W", "S": "WH-S"}
CUSTOMERS = {
    "N": [(1001, "Gurpreet Singh"), (1002, "Simran Kaur"), (1003, "Rohit Sharma"),
          (1004, "Anjali Verma"), (1005, "Harpreet Gill"), (1006, "Neha Kapoor")],
    "W": [(2001, "Aarav Patel"), (2002, "Priya Desai"), (2003, "Rahul Joshi"),
          (2004, "Sneha Kulkarni"), (2005, "Karan Mehta"), (2006, "Isha Shah")],
    "S": [(3001, "Arjun Reddy"), (3002, "Divya Nair"), (3003, "Karthik Iyer"),
          (3004, "Meera Pillai"), (3005, "Suresh Kumar"), (3006, "Lakshmi Rao")],
}
ID_BASE = {"N": 10000, "W": 20000, "S": 30000}
STATUSES = ["DELIVERED"] * 5 + ["SHIPPED"] * 2 + ["PLACED"] + ["CANCELLED"]

# deliberate stock levels used by the demo scenarios: {prod: {warehouse: stock}}
OVERRIDES = {
    "P012": {"WH-N": 0, "WH-W": 0, "WH-S": 1},   # last unit lives in the South
    "P011": {"WH-N": 0, "WH-W": 5, "WH-S": 0},   # only the West stocks it
    "P005": {"WH-N": 6},                         # one sale drops North below reorder level
}


def load_site(region, rng):
    c = connect(region, app="seed")
    for (gid,) in q(c, "SELECT gid FROM pg_prepared_xacts WHERE database = current_database()"):
        q(c, f"ROLLBACK PREPARED '{gid}'")
    q(c, "TRUNCATE order_item, orders, restock_request, price_audit, stock_log, customer_login, "
         "inventory, secure.customer_secure, customer_public, warehouse, product RESTART IDENTITY CASCADE")
    cur = c.cursor()
    for p in PRODUCTS:
        cur.execute("INSERT INTO product VALUES (%s,%s,%s,%s)", p)
    for w in WAREHOUSES:
        cur.execute("INSERT INTO warehouse VALUES (%s,%s,%s)", w)
    for cid, name in CUSTOMERS[region]:
        cur.execute("INSERT INTO customer_public VALUES (%s,%s,%s)", (cid, name, region))
        email = name.lower().replace(" ", ".") + "@example.com"
        cur.execute("INSERT INTO secure.customer_secure VALUES (%s,%s,%s)",
                    (cid, email, f"{rng.randint(0, 9999):04d}"))
    first = CUSTOMERS[region][0][0]
    cur.execute("INSERT INTO customer_login VALUES (%s,%s)", (f"cust_{first}", first))

    wh = HOME_WH[region]                      # inventory fragment: only this site's warehouse
    for pid, *_ in PRODUCTS:
        stock = OVERRIDES.get(pid, {}).get(wh, rng.randint(15, 60))
        cur.execute("INSERT INTO inventory(prod_id, warehouse_id, stock) VALUES (%s,%s,%s)", (pid, wh, stock))

    base, prices = ID_BASE[region], {p[0]: p[3] for p in PRODUCTS}
    for i in range(1, 11):
        oid = base + i
        cust = rng.choice(CUSTOMERS[region])[0]
        day = datetime.date(2026, 9, 1) + datetime.timedelta(days=rng.randint(0, 60))
        cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s,%s)",
                    (oid, cust, day, rng.choice(STATUSES), region))
        for pid in rng.sample([p[0] for p in PRODUCTS if p[0] not in ("P011", "P012")], rng.randint(1, 3)):
            cur.execute("INSERT INTO order_item VALUES (%s,%s,%s,%s,%s)",
                        (oid, pid, rng.randint(1, 3), prices[pid], wh))
    q(c, f"ALTER SEQUENCE order_seq RESTART WITH {base + 101}")
    c.close()


def reset_all(verbose=True):
    CoordinatorLog().reset()
    rng = random.Random(2026)
    for r in REGIONS:
        load_site(r, rng)
        if verbose:
            c = connect(r, app="seed")
            n = [q(c, f"SELECT count(*) FROM {t}")[0][0]
                 for t in ("customer_public", "orders", "order_item", "inventory")]
            c.close()
            print(f"  loaded {SITE_NAME[r]:5s}: {n[0]} customers, {n[1]} orders, "
                  f"{n[2]} order items, {n[3]} inventory rows")


if __name__ == "__main__":
    print("Seeding all sites ...")
    reset_all()
    print("Done.")
