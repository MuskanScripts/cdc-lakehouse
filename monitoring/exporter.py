"""Pipeline exporter for Prometheus (ADR-031): what no stock exporter can see here.

  GET :9108/metrics   computed on every scrape (Prometheus scrapes every 15 s)

- Kafka lag = Kafka end offsets - offsets committed in the BRONZE CHECKPOINT. Spark does not commit
  to a Kafka consumer group, so kafka-exporter's consumer-group lag would be empty for bronze. The
  maths is dashboard/health.py (unit-tested), shared with the Streamlit page.
- quarantine rows per table (decode quarantine + the DQ quarantine), from Delta log stats.
- replication slot lag in bytes (WAL Postgres keeps for Debezium), from pg_replication_slots.
- gold: published age, last cycle status, DQ gate verdict, from /data/last_build.json.
Each group is independent: if Kafka is down, the other metrics are still served (`*_up` = 0).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.environ.get("HEALTH_PATH", "/app"))
import health  # noqa: E402

PORT = int(os.environ.get("EXPORTER_PORT", "9108"))
STATUS = os.environ.get("GOLD_STATUS_PATH", "/data/last_build.json")
CYCLE = {"OK": 0, "BLOCKED": 1, "FAILED": 2}
DQ = {"PASS": 0, "WARN": 1, "FAIL": 2}


def esc(v) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


class Out:
    def __init__(self):
        self.lines, self.seen = [], set()

    def add(self, name, value, help_text, labels=None, kind="gauge"):
        if name not in self.seen:
            self.seen.add(name)
            self.lines += [f"# HELP {name} {help_text}", f"# TYPE {name} {kind}"]
        lab = "{" + ",".join(f'{k}="{esc(v)}"' for k, v in (labels or {}).items()) + "}" if labels else ""
        self.lines.append(f"{name}{lab} {float(value)}")


def parse_ts(s: str | None):
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


def gold_metrics(o: Out, status: dict | None, now: dt.datetime) -> None:
    """From last_build.json (pure, unit-tested)."""
    o.add("lakehouse_gold_status_up", 1 if status else 0, "1 if last_build.json could be read")
    if not status:
        return
    o.add("lakehouse_gold_last_cycle_status", CYCLE.get(status.get("status"), 2),
          "Last gold cycle: 0 published, 1 blocked by data quality, 2 dbt build failed")
    fin = parse_ts(status.get("finished_at"))
    if fin:
        o.add("lakehouse_gold_last_cycle_age_seconds", (now - fin).total_seconds(), "Seconds since the last gold cycle ended")
    pub = parse_ts(status.get("last_published_at"))
    if pub:
        o.add("lakehouse_gold_published_age_seconds", (now - pub).total_seconds(),
              "Seconds since gold was last published (what the dashboard shows)")
    o.add("lakehouse_gold_cycle_duration_seconds", status.get("duration_s", 0), "Duration of the last gold cycle")
    dq = status.get("dq") or {}
    if dq:
        o.add("lakehouse_dq_gate_status", DQ.get(dq.get("status"), 2), "DQ gate on the last build: 0 pass, 1 warn, 2 fail")
        for c in dq.get("checks") or []:
            state = 2 if c["name"] in (dq.get("failed") or []) else 1 if c["name"] in (dq.get("warned") or []) else 0
            o.add("lakehouse_dq_check_state", state, "Per DQ check on the last build: 0 pass, 1 warn, 2 fail",
                  {"check": c["name"], "blocking": str(bool(c.get("blocking"))).lower()})
            if isinstance(c.get("value"), (int, float)):
                o.add("lakehouse_dq_check_value", c["value"], "Measured value of each DQ check (rows, seconds, z)",
                      {"check": c["name"]})


def slot_metrics(o: Out) -> None:
    import psycopg
    with psycopg.connect(connect_timeout=3) as c:
        rows = c.execute("select slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                         "from pg_replication_slots").fetchall()
    for slot, active, lag in rows:
        o.add("lakehouse_replication_slot_lag_bytes", lag or 0, "WAL retained for the slot (bytes)", {"slot": slot})
        o.add("lakehouse_replication_slot_active", 1 if active else 0, "1 if a consumer is attached", {"slot": slot})


def kafka_metrics(o: Out) -> None:
    committed, end = health.bronze_committed(), health.kafka_end_offsets()
    per_topic: dict = {}
    for (topic, p), off in end.items():
        per_topic[topic] = per_topic.get(topic, 0) + max(0, off - committed.get((topic, p), 0))
    for topic, lag in sorted(per_topic.items()):
        o.add("lakehouse_kafka_lag_messages", lag, "Messages in Kafka not yet committed by bronze", {"topic": topic})


def quarantine_metrics(o: Out) -> None:
    for t, n in health.quarantine_rows(health.QUARANTINE_TABLES + ["dq"]).items():
        o.add("lakehouse_quarantine_rows", n, "Rows in quarantine/<table> (dq = data quality quarantine)", {"table": t})


def collect() -> str:
    t0 = time.time()
    o = Out()
    gold_metrics(o, health.read_status(STATUS), dt.datetime.now(dt.timezone.utc))
    for name, fn in (("kafka", kafka_metrics), ("postgres", slot_metrics), ("quarantine", quarantine_metrics)):
        try:
            fn(o)
            ok = 1
        except Exception as e:  # noqa: BLE001
            print(f"{name} metrics failed: {type(e).__name__}: {e}", flush=True)
            ok = 0
        o.add("lakehouse_exporter_source_up", ok, "1 if the source could be read on this scrape", {"source": name})
    o.add("lakehouse_exporter_scrape_seconds", time.time() - t0, "Time to compute all metrics")
    return "\n".join(o.lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            return
        body = collect().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"pipeline exporter on :{PORT}/metrics", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
