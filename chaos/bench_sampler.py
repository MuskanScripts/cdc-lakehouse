"""Lag trend during a benchmark: samples two lags every INTERVAL seconds for SECONDS.

  python bench_sampler.py --seconds 300 --interval 15 [--label x]

- kafka_lag: messages in Kafka not yet in bronze, summed over topics (from the pipeline exporter,
  `make monitoring-up`; it reads the bronze checkpoint, ADR-031).
- silver_lag_s: now - max(_source_ts) in silver/orders = how old the newest change in silver is.
  Under steady load (orders change every second) this is how far silver is behind Postgres.

At the end it prints first / median / max / last and a least-squares slope per minute. A slope
near 0 means the lag is flat (the pipeline keeps up); a positive slope means it falls behind.
"""
import argparse
import os
import re
import statistics as st
import sys
import time
import urllib.request
from datetime import datetime, timezone

import pyarrow.compute as pc
from deltalake import DeltaTable

from common import STORAGE

EXPORTER = os.environ.get("EXPORTER_URL", "http://pipeline-exporter:9108/metrics")


def kafka_lag():
    try:
        body = urllib.request.urlopen(EXPORTER, timeout=10).read().decode()
    except Exception:
        return None
    vals = re.findall(r"^lakehouse_kafka_lag_messages\{[^}]*\} ([\d.e+]+)$", body, re.M)
    return sum(float(v) for v in vals) if vals else None


def silver_lag_s():
    dt = DeltaTable("s3://lakehouse/silver/orders", storage_options=STORAGE)
    mx = pc.max(dt.to_pyarrow_table(columns=["_source_ts"])["_source_ts"]).as_py()
    if mx is None:
        return None
    mx = mx.replace(tzinfo=timezone.utc) if mx.tzinfo is None else mx
    return (datetime.now(timezone.utc) - mx).total_seconds()


def slope_per_min(pts):
    pts = [(t, v) for t, v in pts if v is not None]
    if len(pts) < 3:
        return float("nan")
    mt, mv = st.mean(t for t, _ in pts), st.mean(v for _, v in pts)
    den = sum((t - mt) ** 2 for t, _ in pts)
    return 60 * sum((t - mt) * (v - mv) for t, v in pts) / den if den else float("nan")


def summary(name, pts, fmt):
    v = [x for _, x in pts if x is not None]
    if not v:
        return f"{name}: no samples"
    k = max(1, len(v) // 3)   # medians of the first and last third: robust to the trigger sawtooth
    return (f"{name}: first={fmt(v[0])} median={fmt(st.median(v))} max={fmt(max(v))} "
            f"last={fmt(v[-1])} slope/min={slope_per_min(pts):+.1f} "
            f"third1_med={fmt(st.median(v[:k]))} third3_med={fmt(st.median(v[-k:]))}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=300)
    ap.add_argument("--interval", type=float, default=15)
    ap.add_argument("--label", default="run")
    a = ap.parse_args()
    t0 = time.time()
    kl, sl = [], []
    while time.time() - t0 < a.seconds:
        t = time.time() - t0
        k, s = kafka_lag(), silver_lag_s()
        kl.append((t, k))
        sl.append((t, s))
        print(f"  t={t:5.0f}s kafka_lag={k if k is None else int(k)} silver_lag_s={s if s is None else round(s, 1)}",
              flush=True)
        time.sleep(max(0.0, a.interval - (time.time() - t0 - t)))
    print(f"LAG {a.label}: " + summary("kafka_lag", kl, lambda x: f"{x:.0f}") + " | "
          + summary("silver_lag_s", sl, lambda x: f"{x:.1f}"))


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
