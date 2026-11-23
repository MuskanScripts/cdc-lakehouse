"""Kafka lag maths for the dashboard (pure functions, no Kafka or MinIO needed)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

import health  # noqa: E402

# a real Spark offset-log file: version line, batch metadata, then one JSON per source
OFFSETS = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1795418772616,"conf":{}}
{"ecom.public.orders":{"2":120,"1":95,"0":101},"ecom.public.users":{"0":7}}"""


def test_parse_offsets_reads_every_topic_partition():
    assert health.parse_offsets(OFFSETS) == {
        ("ecom.public.orders", 0): 101, ("ecom.public.orders", 1): 95,
        ("ecom.public.orders", 2): 120, ("ecom.public.users", 0): 7}


def test_latest_batch_ignores_non_numeric_keys():
    keys = ["_checkpoints/bronze/commits/9", "_checkpoints/bronze/commits/10",
            "_checkpoints/bronze/commits/.10.crc", "_checkpoints/bronze/commits/"]
    assert health.latest_batch(keys) == 10
    assert health.latest_batch([]) is None


def test_gold_alerts_healthy_failed_stale_and_missing():
    ok = {"status": "OK", "finished_at": "t", "summary": "Done. PASS=29"}
    assert health.gold_alerts(ok, 25) == []
    failed = health.gold_alerts({"status": "FAILED", "finished_at": "t", "summary": "ERROR=9"}, 25)
    assert len(failed) == 1 and "FAILED" in failed[0] and "ERROR=9" in failed[0]
    stale = health.gold_alerts(ok, 121)
    assert len(stale) == 1 and "121s old" in stale[0]
    assert len(health.gold_alerts({"status": "FAILED"}, 600)) == 2      # both problems shown
    assert len(health.gold_alerts(None, None)) == 2                     # loop never ran, no gold


def test_lag_counts_unread_messages_and_new_partitions():
    committed = health.parse_offsets(OFFSETS)
    end = {("ecom.public.orders", 0): 101, ("ecom.public.orders", 1): 100,
           ("ecom.public.orders", 2): 120, ("ecom.public.users", 0): 7,
           ("ecom.public.payments", 0): 3}          # a topic bronze has never committed
    assert health.kafka_lag(committed, end) == 5 + 3
