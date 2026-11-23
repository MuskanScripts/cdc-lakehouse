"""Schema drift drill: add a column to a live table and time how long silver takes to get it.

  python drift.py                       ALTER TABLE orders ADD COLUMN coupon_code TEXT
  python drift.py --column note         use another column name (the drill is only meaningful the
                                        first time a column is added)
  python drift.py --table products --column note

Steps and what is timed (t=0 is the moment the ALTER is sent):
  1. ALTER TABLE ... ADD COLUMN        (Postgres, instant for a nullable column)
  2. one UPDATE that fills the column  so the table emits an event even if the simulator is off
  3. Debezium sees the new column in the next row it decodes and registers a new schema version
  4. bronze stores the bytes, silver decodes them with the new schema id and MERGEs the column in
Then it asks the Schema Registry whether two *hypothetical* changes would be accepted, to show
why an additive change flows and a type change is stopped (without breaking the live connector).
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

import psycopg
import pyarrow.compute as pc
from deltalake import DeltaTable

from common import STORAGE

SR = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
PKS = {"orders": "order_id", "products": "product_id", "users": "user_id"}
CT ={"Content-Type": "application/vnd.schemaregistry.v1+json"}


def sr(path, body=None):
    req = urllib.request.Request(SR + path, data=None if body is None else json.dumps(body).encode(), headers=CT)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def silver_view(table, col, row_id):
    dt = DeltaTable(f"s3://lakehouse/silver/{table}", storage_options=STORAGE)
    if col not in [f.name for f in dt.schema().fields]:
        return False, None
    pk = PKS[table]
    t = dt.to_pyarrow_dataset().to_table(columns=[pk, col], filter=pc.field(pk) == row_id)
    return True, (t.column(col)[0].as_py() if t.num_rows else None)


def mutate(node, fn):
    """Apply fn to the first record that has a `qty`-style target; generic walk over Avro JSON."""
    if isinstance(node, dict):
        if fn(node):
            return True
        return any(mutate(v, fn) for v in node.values())
    if isinstance(node, list):
        return any(mutate(v, fn) for v in node)
    return False


def registry_verdicts():
    """Would the registry (BACKWARD compatibility) accept these changes to order_items?"""
    subject = "ecom.public.order_items-value"
    latest = json.loads(sr(f"/subjects/{subject}/versions/latest")["schema"])

    def break_type(rec):  # qty: int -> string
        for f in rec.get("fields", []) if rec.get("type") == "record" else []:
            if f["name"] == "qty":
                f["type"] = "string"
                return True
        return False

    def add_field(rec):  # add an optional field with a default
        if rec.get("type") == "record" and any(f["name"] == "qty" for f in rec.get("fields", [])):
            rec["fields"].append({"name": "gift_note", "type": ["null", "string"], "default": None})
            return True
        return False

    out = {}
    for label, fn in (("additive: new optional column", add_field), ("breaking: qty int -> string", break_type)):
        candidate = json.loads(json.dumps(latest))
        assert mutate(candidate, fn), "qty field not found in the registered schema"
        res = sr(f"/compatibility/subjects/{subject}/versions/latest", {"schema": json.dumps(candidate)})
        out[label] = res.get("is_compatible")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--column", default="coupon_code")
    ap.add_argument("--table", default="orders", choices=sorted(PKS))
    ap.add_argument("--timeout", type=int, default=300)
    args = ap.parse_args()
    col, table, pk = args.column, args.table, PKS[args.table]
    subject = f"ecom.public.{table}-value"

    with psycopg.connect(autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
                              (table, col)).fetchone()
        if exists:
            print(f"{table}.{col} already exists in Postgres; pick a new one with --column. Nothing to time.")
            return 2
        versions_before = len(sr(f"/subjects/{subject}/versions"))

        t0 = time.time()
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
        t_alter = time.time() - t0
        probe = f"DRIFT-{int(t0)}"
        order_id = conn.execute(
            f"UPDATE {table} SET {col} = %s, updated_at = now() "
            f"WHERE {pk} = (SELECT max({pk}) FROM {table}) RETURNING {pk}", (probe,)).fetchone()[0]
        t_probe = time.time() - t0
    print(f"ALTER TABLE {table} ADD COLUMN {col} TEXT  ({t_alter * 1000:.0f} ms); probe UPDATE of {pk}={order_id} committed at +{t_probe:.1f}s")

    seen = {}
    deadline = t0 + args.timeout
    while len(seen) < 3 and time.time() < deadline:
        now = time.time() - t0
        if "registry" not in seen and len(sr(f"/subjects/{subject}/versions")) > versions_before:
            seen["registry"] = now
        has, val = silver_view(table, col, order_id)
        if has and "silver column" not in seen:
            seen["silver column"] = now
        if has and val == probe and "silver value" not in seen:
            seen["silver value"] = now
        time.sleep(0.5)

    print("\nTimeline (seconds after the ALTER was sent):")
    for k, label in (("registry", "new schema version registered by Debezium"),
                     ("silver column", f"column `{col}` exists in the silver {table} Delta schema"),
                     ("silver value", f"probe value {probe!r} readable in silver")):
        print(f"  {seen[k]:6.1f} s  {label}" if k in seen else f"   TIMEOUT  {label}")

    print("\nSchema Registry (BACKWARD) verdict on hypothetical changes to order_items:")
    for label, ok in registry_verdicts().items():
        print(f"  {label:<34} compatible={ok}")
    return 0 if len(seen) == 3 else 1


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)  # delta-rs/arrow threads can abort at interpreter teardown (exit 139)
