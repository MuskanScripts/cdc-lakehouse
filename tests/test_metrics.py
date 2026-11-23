"""Prometheus output of the streaming listener and the pipeline exporter (pure functions)."""
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "monitoring"))

from streaming.common import metrics  # noqa: E402
import exporter  # noqa: E402


def test_streaming_render_has_one_series_per_query_and_counters():
    latest = {"silver": {"batch_duration_seconds": 12.5, "input_rows": 300.0, "input_rows_per_second": 30.0,
                         "processed_rows_per_second": 24.0, "batch_id": 7.0, "last_progress_timestamp_seconds": 1.0},
              "bronze": {"batch_duration_seconds": 2.0, "input_rows": 50.0, "input_rows_per_second": 5.0,
                         "processed_rows_per_second": 25.0, "batch_id": 9.0, "last_progress_timestamp_seconds": 1.0}}
    text = metrics.render(latest, {"silver": 3, "bronze": 9}, {"silver": 900, "bronze": 450})
    assert 'spark_streaming_batch_duration_seconds{query="silver"} 12.5' in text
    assert 'spark_streaming_batches_total{query="bronze"} 9' in text
    assert 'spark_streaming_input_rows_total{query="silver"} 900' in text
    assert text.count("# TYPE spark_streaming_batch_duration_seconds gauge") == 1
    assert text.endswith("\n")


def test_exporter_gold_metrics_blocked_cycle():
    now = dt.datetime(2026, 11, 23, 12, 0, 0, tzinfo=dt.timezone.utc)
    status = {"status": "BLOCKED", "finished_at": "2026-11-23T11:59:30Z", "duration_s": 21.0,
              "last_published_at": "2026-11-23T11:55:00Z",
              "dq": {"status": "FAIL", "failed": ["orders_total_non_negative"], "warned": ["orders_volume_vs_trailing_hours"],
                     "checks": [{"name": "orders_total_non_negative", "value": 5, "blocking": True},
                                {"name": "orders_volume_vs_trailing_hours", "value": 3.5, "blocking": False},
                                {"name": "orders_not_empty", "value": 100, "blocking": True}]}}
    o = exporter.Out()
    exporter.gold_metrics(o, status, now)
    text = "\n".join(o.lines)
    assert "lakehouse_gold_last_cycle_status 1.0" in text                  # blocked
    assert "lakehouse_gold_published_age_seconds 300.0" in text
    assert "lakehouse_gold_last_cycle_age_seconds 30.0" in text
    assert "lakehouse_dq_gate_status 2.0" in text
    assert 'lakehouse_dq_check_state{check="orders_total_non_negative",blocking="true"} 2.0' in text
    assert 'lakehouse_dq_check_state{check="orders_volume_vs_trailing_hours",blocking="false"} 1.0' in text
    assert 'lakehouse_dq_check_state{check="orders_not_empty",blocking="true"} 0.0' in text
    assert 'lakehouse_dq_check_value{check="orders_total_non_negative"} 5.0' in text


def test_exporter_without_status_file():
    o = exporter.Out()
    exporter.gold_metrics(o, None, dt.datetime.now(dt.timezone.utc))
    assert o.lines[-1] == "lakehouse_gold_status_up 0.0"
