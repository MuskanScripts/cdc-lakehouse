"""Pipeline health numbers for the dashboard: Kafka lag and quarantine size.

Kafka lag = (latest offset in each Kafka partition) - (offset bronze has committed for it).
Spark does not commit offsets to a Kafka consumer group, so the committed position comes from
the bronze streaming checkpoint: the highest batch id in `commits/` and its file in `offsets/`
(line 1 "v1", line 2 batch metadata, then one JSON per source: {"topic": {"partition": offset}}).
The offset stored there is the NEXT one to read, which is directly comparable with the high
watermark.

Kept free of heavy imports at module level so the parsing is unit-testable anywhere.
"""
from __future__ import annotations

import json
import os

BUCKET = "lakehouse"
CHECKPOINT = "_checkpoints/bronze"
QUARANTINE_TABLES = ["users", "products", "orders", "order_items", "payments"]


def parse_offsets(text: str) -> dict:
    """{(topic, partition): next_offset} from one Spark offset-log file."""
    out = {}
    for line in text.splitlines()[2:]:
        line = line.strip()
        if not line.startswith("{"):
            continue
        for topic, parts in json.loads(line).items():
            for p, off in parts.items():
                out[(topic, int(p))] = int(off)
    return out


def latest_batch(keys) -> int | None:
    ids = [int(k.rsplit("/", 1)[-1]) for k in keys if k.rsplit("/", 1)[-1].isdigit()]
    return max(ids) if ids else None


def kafka_lag(committed: dict, end_offsets: dict) -> int:
    """Messages in Kafka that bronze has not committed yet, over all partitions."""
    return sum(max(0, end - committed.get(tp, 0)) for tp, end in end_offsets.items())


# ------------------------------------------------------------ live readers (need boto3 / kafka)

def _s3():
    import boto3
    return boto3.client(
        "s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345"),
        region_name="us-east-1")


def bronze_committed() -> dict:
    s3 = _s3()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{CHECKPOINT}/commits/"):
        keys += [o["Key"] for o in page.get("Contents", [])]
    b = latest_batch(keys)
    if b is None:
        return {}
    body = s3.get_object(Bucket=BUCKET, Key=f"{CHECKPOINT}/offsets/{b}")["Body"].read().decode()
    return parse_offsets(body)


def kafka_end_offsets(prefix: str = "ecom.public.") -> dict:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"),
                  "group.id": "dashboard-lag-probe", "enable.auto.commit": False})
    try:
        md = c.list_topics(timeout=5)
        out = {}
        for t, tm in md.topics.items():
            if not t.startswith(prefix):
                continue
            for p in tm.partitions:
                _, high = c.get_watermark_offsets(TopicPartition(t, p), timeout=5)
                out[(t, p)] = high
        return out
    finally:
        c.close()


def quarantine_rows() -> dict:
    """Rows per quarantine table, from the Delta log's per-file row counts (no data read)."""
    from deltalake import DeltaTable
    out = {}
    for t in QUARANTINE_TABLES:
        try:
            acts = DeltaTable(f"s3://{BUCKET}/quarantine/{t}").get_add_actions(flatten=True).to_pydict()
            out[t] = int(sum(x or 0 for x in acts.get("num_records", [])))
        except Exception:  # no quarantine table for this source yet
            out[t] = 0
    return out
