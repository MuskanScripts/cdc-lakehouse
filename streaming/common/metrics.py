"""Prometheus metrics for the streaming driver (ADR-031).

A StreamingQueryListener keeps the latest progress of each query (bronze, silver,
orders_per_minute); a stdlib HTTP server thread serves it as Prometheus text on :9109/metrics.
No extra jar, no Spark UI (it stays disabled to save memory), no Pushgateway container.

Anything that goes wrong in here is logged and swallowed: metrics must never stop the stream.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("metrics")
PORT = int(os.environ.get("METRICS_PORT", "9109"))


class ProgressListener(StreamingQueryListener):
    def __init__(self):
        self.lock = threading.Lock()
        self.latest: dict = {}     # query name -> last progress numbers
        self.batches: dict = {}    # query name -> batches completed since the driver started
        self.rows: dict = {}       # query name -> input rows since the driver started

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        try:
            p = event.progress
            name = p.name or str(p.id)
            dur = (p.durationMs or {}).get("triggerExecution", 0) / 1000.0
            with self.lock:
                self.latest[name] = {
                    "batch_duration_seconds": dur,
                    "input_rows": float(p.numInputRows or 0),
                    "input_rows_per_second": float(p.inputRowsPerSecond or 0.0),
                    "processed_rows_per_second": float(p.processedRowsPerSecond or 0.0),
                    "batch_id": float(p.batchId),
                    "last_progress_timestamp_seconds": time.time(),
                }
                self.batches[name] = self.batches.get(name, 0) + 1
                self.rows[name] = self.rows.get(name, 0) + int(p.numInputRows or 0)
        except Exception:  # noqa: BLE001
            log.exception("metrics listener failed on a progress event (ignored)")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def render(self) -> str:
        with self.lock:
            latest = {k: dict(v) for k, v in self.latest.items()}
            batches, rows = dict(self.batches), dict(self.rows)
        return render(latest, batches, rows)


HELP = {
    "batch_duration_seconds": "Wall time of the last micro-batch (triggerExecution)",
    "input_rows": "Rows in the last micro-batch",
    "input_rows_per_second": "Input rate of the last micro-batch",
    "processed_rows_per_second": "Processing rate of the last micro-batch",
    "batch_id": "Id of the last completed micro-batch",
    "last_progress_timestamp_seconds": "Unix time of the last progress event",
}


def render(latest: dict, batches: dict, rows: dict) -> str:
    """Prometheus text format (pure, unit-tested)."""
    out = []
    for metric, help_text in HELP.items():
        out += [f"# HELP spark_streaming_{metric} {help_text}", f"# TYPE spark_streaming_{metric} gauge"]
        out += [f'spark_streaming_{metric}{{query="{q}"}} {v[metric]}' for q, v in sorted(latest.items())]
    out += ["# HELP spark_streaming_batches_total Micro-batches completed since the driver started",
            "# TYPE spark_streaming_batches_total counter"]
    out += [f'spark_streaming_batches_total{{query="{q}"}} {n}' for q, n in sorted(batches.items())]
    out += ["# HELP spark_streaming_input_rows_total Input rows since the driver started",
            "# TYPE spark_streaming_input_rows_total counter"]
    out += [f'spark_streaming_input_rows_total{{query="{q}"}} {n}' for q, n in sorted(rows.items())]
    return "\n".join(out) + "\n"


def start_metrics(spark) -> ProgressListener | None:
    try:
        listener = ProgressListener()
        spark.streams.addListener(listener)

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = listener.render().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # no access log in the driver output
                pass

        server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
        threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True).start()
        log.warning("metrics on :%s/metrics", PORT)
        return listener
    except Exception:  # noqa: BLE001
        log.exception("could not start the metrics endpoint (streaming continues without it)")
        return None
