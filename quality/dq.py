"""Data quality runner (Soda Core over DuckDB) for the gold gate and for Airflow's dq_checks.

  /opt/soda/bin/python /quality/dq.py --source build [--db /data/gold_build.duckdb] [--out f.json]
  /opt/soda/bin/python /quality/dq.py --source silver [--quarantine] [--out f.json]

--source build   the staging tables dbt just wrote into the BUILD file (attached read-only). This is
                 the gate: it checks exactly the data that is about to be published (ADR-029).
--source silver  silver Delta on MinIO (delta-rs), current state only. Used by Airflow's dq_checks;
                 with --quarantine, the rows behind every failed record-level rule are appended to
                 s3://lakehouse/quarantine/dq (once per check, key and LSN).

Exit code: 0 = PASS or WARN, 2 = FAIL (a blocking check failed, errored or was not evaluated),
1 = the runner itself crashed (the gate treats that as FAIL too: fail closed).

Runs in its own venv (/opt/soda) because Soda pins an old protobuf that breaks dbt (ADR-030).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import uuid

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
CHECKS = os.path.join(HERE, "checks.yml")
STAGING = ["stg_orders", "stg_payments", "stg_users"]
PK = {"stg_orders": ["order_id"], "stg_payments": ["payment_id"], "stg_users": ["user_id"]}
DQ_QUARANTINE = os.environ.get("DQ_QUARANTINE_PATH", "s3://lakehouse/quarantine/dq")
FAR_FUTURE = "9999-12-31 00:00:00"  # source max when Postgres can't be read: freshness then WARNs

# silver -> the same current-state shape as the dbt staging models (plus _lsn for the quarantine)
SILVER_STAGING = {
    "stg_orders": """select order_id, user_id, status, total, cast(created_at as timestamp) as created_at,
                            cast(updated_at as timestamp) as updated_at, _lsn
                     from _silver_orders where not _is_deleted""",
    "stg_payments": """select payment_id, order_id, method, status, amount,
                              cast(updated_at as timestamp) as updated_at, _lsn
                       from _silver_payments where not _is_deleted""",
    "stg_users": """select user_id, city, tier, cast(updated_at as timestamp) as updated_at, _lsn
                    from _silver_users where not _is_deleted""",
}


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# ------------------------------------------------------------------ rules (pure, unit-tested)

def load_rules(path: str = CHECKS) -> dict:
    """name -> {table, kind ('failed_rows' | 'metric'), blocking, condition, query} from checks.yml.

    Severity is read from the YAML itself: a check with `warn:` and no `fail:` never blocks.
    """
    with open(path) as f:
        doc = yaml.safe_load(f)
    rules = {}
    for header, checks in doc.items():
        table = header.split("checks for ", 1)[1].strip()
        for chk in checks:
            if isinstance(chk, str):
                rules[chk] = {"table": table, "kind": "metric", "blocking": True}
                continue
            (key, cfg), = chk.items()
            cfg = cfg or {}
            name = cfg.get("name", key)
            if key == "failed rows":
                rules[name] = {"table": table, "kind": "failed_rows", "blocking": True,
                               "condition": cfg.get("fail condition"), "query": cfg.get("fail query")}
            else:
                rules[name] = {"table": table, "kind": "metric",
                               "blocking": not ("warn" in cfg and "fail" not in cfg)}
    return rules


def classify(checks: list, rules: dict) -> dict:
    """Soda check results -> verdict {status PASS|WARN|FAIL, failed, warned, checks}.

    Fail closed: a blocking check that errored or that Soda did not report at all (typo, query
    error) counts as failed. A warn-only check that errored is a warning.
    """
    failed, warned, out = [], [], []
    seen = set()
    for c in checks:
        name = c.get("name")
        seen.add(name)
        rule = rules.get(name, {"blocking": True})
        outcome = c.get("outcome") or "error"
        value = (c.get("diagnostics") or {}).get("value")
        if outcome == "fail" or (outcome not in ("pass", "warn") and rule["blocking"]):
            failed.append(name)
        elif outcome != "pass":
            warned.append(name)
        out.append({"name": name, "table": c.get("table"), "outcome": outcome, "value": value,
                    "blocking": rule["blocking"]})
    for name in sorted(set(rules) - seen):
        failed.append(name)
        out.append({"name": name, "table": rules[name]["table"], "outcome": "not_evaluated",
                    "value": None, "blocking": True})
    status = "FAIL" if failed else "WARN" if warned else "PASS"
    return {"status": status, "failed": failed, "warned": warned, "checks": out}


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:.1f}"
    return "?" if v is None else str(v)


def summary_line(verdict: dict) -> str:
    values = {c["name"]: c["value"] for c in verdict["checks"]}
    parts = [f"DQ {verdict['status']}"]
    if verdict["failed"]:
        parts.append("blocking: " + ", ".join(f"{n}={_fmt(values.get(n))}" for n in verdict["failed"]))
    if verdict["warned"]:
        parts.append("warn: " + ", ".join(f"{n}={_fmt(values.get(n))}" for n in verdict["warned"]))
    parts.append(f"{len(verdict['checks'])} checks")
    if "duration_s" in verdict:
        parts[-1] += f" in {verdict['duration_s']:.1f}s"
    if "quarantined_new" in verdict:
        parts.append(f"quarantined {verdict['quarantined_new']} new of {verdict['bad_records']} bad rows")
    if verdict.get("source_reachable") is False:
        parts.append("Postgres unreachable: freshness not judged")
    return " | ".join(parts)


def new_records(rows: list, existing: set) -> list:
    """Rows not quarantined before. Key = (check, pk, lsn): the same bad version of a row is kept
    once, however many runs see it; a new bad version (higher LSN) is a new record."""
    out, seen = [], set(existing)
    for r in rows:
        k = (r["check"], r["pk"], r["lsn"])
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


# ------------------------------------------------------------------ data access

def storage(write: bool = False) -> dict:
    opts = {"AWS_ALLOW_HTTP": "true", "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "AWS_ENDPOINT_URL": os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")}
    if write:  # one writer only (Airflow dq_checks, max_active_runs=1), so no lock service is needed
        opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    return opts


def connect_build(db_path: str):
    import duckdb
    con = duckdb.connect()                      # in-memory; the build file is only ATTACHed read-only
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{db_path}' AS g (READ_ONLY)")
    for t in STAGING:
        schema = con.execute(
            "select table_schema from information_schema.tables where table_catalog = 'g' and table_name = ?",
            [t]).fetchone()
        if schema is None:
            raise RuntimeError(f"{t} not found in {db_path}")
        con.execute(f'create view {t} as select * from g."{schema[0]}".{t}')
    return con


def connect_silver():
    import duckdb
    from deltalake import DeltaTable
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for t in ("orders", "payments", "users"):
        ds = DeltaTable(f"s3://lakehouse/silver/{t}", storage_options=storage()).to_pyarrow_dataset()
        con.register(f"_silver_{t}", ds)
    for name, sql in SILVER_STAGING.items():
        con.execute(f"create table {name} as {sql}")
    return con


def source_max_orders():
    """max(orders.updated_at) in Postgres as naive UTC, or None if it can't be read."""
    try:
        import psycopg
        with psycopg.connect(connect_timeout=5) as c:  # PG* env vars
            return c.execute("select max(updated_at) at time zone 'UTC' from orders").fetchone()[0]
    except Exception as e:  # noqa: BLE001  the gate must not crash because the source is down
        log(f"source freshness: cannot read Postgres ({type(e).__name__}: {e})")
        return None


def run_scan(con, variables: dict, definition: str) -> list:
    from soda.scan import Scan
    scan = Scan()
    scan.set_data_source_name("lake")
    scan.add_duckdb_connection(con, "lake")
    scan.set_scan_definition_name(definition)
    scan.add_variables(variables)
    scan.add_sodacl_yaml_file(CHECKS)
    scan.execute()
    return scan.get_scan_results()["checks"]


def bad_records(con, rules: dict, failed: list) -> list:
    """The rows behind each failed record-level rule, using the rule's own condition/query."""
    rows = []
    for name in failed:
        r = rules.get(name)
        if not r or r["kind"] != "failed_rows":
            continue
        sql = r["query"] or f"select * from {r['table']} where {r['condition']}"
        for rec in con.execute(sql).arrow().to_pylist():
            pk = {c: rec.get(c) for c in PK[r["table"]]}
            rows.append({"check": name, "table": r["table"].removeprefix("stg_"),
                         "pk": json.dumps(pk, sort_keys=True, default=str), "lsn": rec.get("_lsn"),
                         "row": json.dumps(rec, sort_keys=True, default=str)})
    return rows


def write_quarantine(rows: list, run_id: str) -> int:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    existing = set()
    try:
        t = DeltaTable(DQ_QUARANTINE, storage_options=storage()).to_pyarrow_table(columns=["check", "pk", "lsn"])
        existing = set(zip(t["check"].to_pylist(), t["pk"].to_pylist(), t["lsn"].to_pylist()))
    except Exception as e:  # noqa: BLE001  first run: the table does not exist yet
        if "not a Delta table" not in str(e) and "TableNotFound" not in type(e).__name__:
            raise
    new = new_records(rows, existing)
    if new:
        now = dt.datetime.now(dt.timezone.utc)
        schema = pa.schema([("check", pa.string()), ("table", pa.string()), ("pk", pa.string()),
                            ("lsn", pa.int64()), ("row", pa.string()),
                            ("detected_at", pa.timestamp("us", tz="UTC")), ("run_id", pa.string())])
        data = pa.Table.from_pylist([{**r, "detected_at": now, "run_id": run_id} for r in new], schema=schema)
        write_deltalake(DQ_QUARANTINE, data, mode="append", storage_options=storage(write=True))
    return len(new)


def write_json(path: str, doc: dict) -> None:
    with open(path + ".tmp", "w") as f:
        json.dump(doc, f, default=str)
    os.replace(path + ".tmp", path)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["build", "silver"], required=True)
    p.add_argument("--db", default=os.environ.get("GOLD_BUILD_PATH", "/data/gold_build.duckdb"))
    p.add_argument("--quarantine", action="store_true")
    p.add_argument("--out")
    a = p.parse_args(argv)
    t0 = time.time()
    run_id = f"{a.source}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"

    con = connect_build(a.db) if a.source == "build" else connect_silver()
    src_max = source_max_orders()
    variables = {"SRC_ORDERS_MAX": src_max.strftime("%Y-%m-%d %H:%M:%S.%f") if src_max else FAR_FUTURE}
    rules = load_rules()
    verdict = classify(run_scan(con, variables, f"lakehouse_{a.source}"), rules)
    verdict.update(source=a.source, run_id=run_id, source_reachable=src_max is not None,
                   checked_at=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    if a.quarantine and verdict["failed"]:
        rows = bad_records(con, rules, verdict["failed"])
        verdict["bad_records"] = len(rows)
        verdict["quarantined_new"] = write_quarantine(rows, run_id)
    verdict["duration_s"] = round(time.time() - t0, 2)
    if a.out:
        write_json(a.out, verdict)
    print(summary_line(verdict), flush=True)
    return 2 if verdict["status"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
