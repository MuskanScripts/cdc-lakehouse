"""Week 3: schema drift, event time / late events, idempotent writes.

Same approach as test_silver.py: real Confluent-framed Avro through decode_table(). Two writer
schemas are used for `orders`: v1 (sid 7) and v2 (sid 8) which adds `coupon_code` in the MIDDLE
of the record, so decoding v2 bytes with the v1 schema would misparse them. v3 (sid 9) changes
the type of `total`, which is a breaking change.
"""
import json
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import (DecimalType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from streaming.common.late_events import (EVENTS_SCHEMA, LATE_SCHEMA, WatermarkTracker,
                                          process_order_events, split_late)
from streaming.common.transforms import (decode_table, incompatible_columns,
                                         merge_into_silver, schema_id_col, to_silver_rows,
                                         write_quarantine)

DEC = {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}


def avro_schemas(version):
    fields = [{"name": "order_id", "type": ["null", "long"], "default": None},
              {"name": "user_id", "type": "long"},
              {"name": "status", "type": "string"},
              {"name": "total", "type": "string" if version == 3 else DEC},
              {"name": "created_at", "type": "string"}]
    if version == 2:
        fields.append({"name": "coupon_code", "type": ["null", "string"], "default": None})
    fields.append({"name": "updated_at", "type": "string"})
    value = {"type": "record", "name": "Value", "fields": fields}
    return json.dumps({"type": "record", "name": "Envelope", "fields": [
        {"name": "before", "type": ["null", value], "default": None},
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "source", "type": {"type": "record", "name": "Source", "fields": [
            {"name": "lsn", "type": ["null", "long"], "default": None},
            {"name": "ts_ms", "type": "long"}]}},
        {"name": "op", "type": "string"}]})


SIDS = {1: 7, 2: 8, 3: 9}
SCHEMAS = {SIDS[v]: avro_schemas(v) for v in SIDS}


def row_type(version):
    f = [StructField("order_id", LongType()), StructField("user_id", LongType()),
         StructField("status", StringType()),
         StructField("total", StringType() if version == 3 else DecimalType(12, 2)),
         StructField("created_at", StringType())]
    if version == 2:
        f.append(StructField("coupon_code", StringType()))
    f.append(StructField("updated_at", StringType()))
    return StructType(f)


def make_row(version, order_id, status, total, created, coupon):
    base = [order_id, 1, status, "10.00" if version == 3 else Decimal(total), created]
    if version == 2:
        base.append(coupon)
    return tuple(base + ["2026-01-01T00:00:00.000000Z"])


def bronze(spark, events, version=1):
    """events: dicts with op, lsn, offset, order_id, status, total, created, coupon."""
    rt = row_type(version)
    env_t = StructType([StructField("before", rt), StructField("after", rt),
                        StructField("source", StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])),
                        StructField("op", StringType())])
    data = []
    for e in events:
        r = make_row(version, e["order_id"], e.get("status", "created"), e.get("total", "10.00"),
                     e.get("created", "2026-01-01T00:00:00.000000Z"), e.get("coupon"))
        data.append(((r if e["op"] == "d" else None, None if e["op"] == "d" else r,
                      (e["lsn"], 1_700_000_000_000 + e["lsn"]), e["op"]), e["offset"]))
    df = spark.createDataFrame(data, StructType([StructField("env", env_t), StructField("offset", LongType())]))
    sid = SIDS[version]
    framed = F.concat(F.unhex(F.lit("00%08x" % sid)), to_avro(F.col("env"), SCHEMAS[sid]))
    return (df.select(F.lit("ecom.public.orders").alias("topic"), F.lit(0).alias("partition"), "offset",
                      F.lit(None).cast("binary").alias("key"), framed.alias("value"))
            .withColumn("schema_id", schema_id_col())
            .withColumn("ingest_ts", F.current_timestamp()))


def schema_for(sid):
    return SCHEMAS[sid]


def apply(spark, path, *batches):
    """batches: (events, version) pairs that arrive in ONE micro-batch."""
    df = None
    for events, version in batches:
        b = bronze(spark, events, version)
        df = b if df is None else df.unionByName(b)
    target = (lambda: spark.read.format("delta").load(path).schema) if _exists(spark, path) else None
    good, bad = decode_table(df, "orders", schema_for, target_schema_for=target)
    if good is not None:
        merge_into_silver(to_silver_rows(good, "orders"), path, ["order_id"])
    return good, bad


def _exists(spark, path):
    try:
        spark.read.format("delta").load(path)
        return True
    except Exception:
        return False


def silver(spark, path):
    return {r.order_id: r for r in spark.read.format("delta").load(path).collect()}


# ------------------------------------------------------------------ schema drift

def test_new_column_reaches_silver_and_old_rows_stay_null(spark, tmp_path):
    p = str(tmp_path / "orders")
    apply(spark, p, ([dict(op="c", lsn=10, offset=1, order_id=1), dict(op="c", lsn=11, offset=2, order_id=2)], 1))
    assert "coupon_code" not in spark.read.format("delta").load(p).columns

    # the first event after the ALTER carries the new schema; v1 and v2 events share one batch
    apply(spark, p, ([dict(op="u", lsn=20, offset=3, order_id=1, status="paid", coupon="WELCOME10"),
                      dict(op="c", lsn=21, offset=4, order_id=3, coupon="FEST20")], 2),
          ([dict(op="u", lsn=22, offset=5, order_id=2, status="paid")], 1))
    s = silver(spark, p)
    assert "coupon_code" in spark.read.format("delta").load(p).columns
    assert (s[1].status, s[1].coupon_code, s[1].total) == ("paid", "WELCOME10", Decimal("10.00"))
    assert (s[2].status, s[2].coupon_code) == ("paid", None)   # old schema, no coupon
    assert (s[3].status, s[3].coupon_code) == ("created", "FEST20")


def test_old_schema_batch_after_evolution_does_not_fail_or_erase_the_column(spark, tmp_path):
    p = str(tmp_path / "orders")
    apply(spark, p, ([dict(op="c", lsn=10, offset=1, order_id=1, coupon="KEEPME")], 2))
    # a replayed / late batch written with the v1 schema (no coupon_code) and a higher lsn
    apply(spark, p, ([dict(op="u", lsn=30, offset=2, order_id=1, status="shipped")], 1))
    r = silver(spark, p)[1]
    assert (r.status, r.coupon_code, r._lsn) == ("shipped", "KEEPME", 30)


def test_breaking_type_change_is_quarantined_as_schema_incompatible(spark, tmp_path):
    p = str(tmp_path / "orders")
    apply(spark, p, ([dict(op="c", lsn=10, offset=1, order_id=1)], 1))
    good, bad = apply(spark, p, ([dict(op="u", lsn=20, offset=2, order_id=1, status="paid")], 1),
                      ([dict(op="u", lsn=21, offset=3, order_id=1, status="BROKEN")], 3))
    assert {(r.offset, r.reason) for r in bad.collect()} == {(3, "schema_incompatible")}
    r = silver(spark, p)[1]
    assert (r.status, r._lsn) == ("paid", 20)  # the good rows were merged, the breaking one was not


def test_incompatible_columns_rules():
    tgt = StructType([StructField("a", LongType()), StructField("b", DecimalType(12, 2)),
                      StructField("c", StringType()), StructField("created_at", StringType())])
    # int -> long widens, a decimal that fits widens, long -> string is breaking, a NEW column is fine
    src = StructType([StructField("a", IntegerType()),
                      StructField("b", DecimalType(10, 2)), StructField("c", LongType()),
                      StructField("brand_new", StringType())])
    assert incompatible_columns(src, tgt, []) == ["c"]
    # a wider decimal than the silver column (could overflow) and a changed scale are breaking
    assert incompatible_columns(StructType([StructField("b", DecimalType(14, 2))]), tgt, []) == ["b"]
    assert incompatible_columns(StructType([StructField("b", DecimalType(12, 4))]), tgt, []) == ["b"]
    # a Debezium timestamp string is fine for a timestamp silver column
    ts_tgt = StructType([StructField("created_at", TimestampType())])
    assert incompatible_columns(StructType([StructField("created_at", StringType())]), ts_tgt, ["created_at"]) == []


# ------------------------------------------------------------------ event time / late events

def make_event_tables(spark, root):
    ev, late = f"{root}/events", f"{root}/late"
    spark.createDataFrame([], EVENTS_SCHEMA).write.format("delta").save(ev)
    spark.createDataFrame([], LATE_SCHEMA).write.format("delta").save(late)
    return ev, late


def good_orders(spark, events):
    good, _ = decode_table(bronze(spark, events, 1), "orders", schema_for)
    return good


def run_events(spark, ev, late, batch_id, events, tracker):
    return process_order_events(good_orders(spark, events), batch_id, tracker, ev, late)


def created(order_id, ts, lsn, offset=None, op="c"):
    return dict(op=op, lsn=lsn, offset=offset or lsn, order_id=order_id, created=ts)


def test_split_late_uses_the_watermark(spark):
    df = spark.sql("SELECT 1 AS order_id, timestamp'2026-01-01 12:00:00' AS event_time UNION ALL "
                   "SELECT 2, timestamp'2026-01-01 11:00:00' UNION ALL SELECT 3, timestamp'2026-01-01 11:50:00'")
    on_time, late = split_late(df, None)
    assert late is None and on_time.count() == 3   # no watermark yet, nothing is late
    wm = spark.sql("SELECT unix_micros(timestamp'2026-01-01 11:50:00')").first()[0]
    on_time, late = split_late(df, wm)
    assert {r.order_id for r in on_time.collect()} == {1, 3}   # exactly at the watermark is on time
    assert {r.order_id for r in late.collect()} == {2}


def test_late_events_are_routed_and_replays_do_not_duplicate(spark, tmp_path):
    ev, late = make_event_tables(spark, str(tmp_path))
    t = WatermarkTracker(ev, 600)

    # batch 0: first events, no watermark yet -> both on time
    assert run_events(spark, ev, late, 0, [created(1, "2026-01-01T12:00:00Z", 10),
                                           created(2, "2026-01-01T12:01:00Z", 11)], t) == (2, 0)
    # batch 1: watermark = 12:01 - 10 min = 11:51. 12:30 moves it on; 11:00 is 51 min late; 11:55 is on time
    b1 = [created(3, "2026-01-01T12:30:00Z", 12), created(4, "2026-01-01T11:00:00Z", 13),
          created(5, "2026-01-01T11:55:00Z", 14), dict(op="u", lsn=15, offset=15, order_id=1)]
    assert run_events(spark, ev, late, 1, b1, t) == (2, 1)   # only `c` events count: the update is ignored
    assert {r.order_id for r in spark.read.format("delta").load(late).collect()} == {4}
    assert {r.order_id for r in spark.read.format("delta").load(ev).collect()} == {1, 2, 3, 5}

    # crash + replay of batch 1 in a NEW process: the tracker is rebuilt from the table with
    # batch_id < 1, so it sees the same watermark and nothing is written twice
    t2 = WatermarkTracker(ev, 600)
    assert run_events(spark, ev, late, 1, b1, t2)[1] == 1
    assert spark.read.format("delta").load(ev).count() == 4
    assert spark.read.format("delta").load(late).count() == 1

    # Debezium re-delivers order 1's insert after a Connect crash: same event, higher offset
    assert run_events(spark, ev, late, 2, [created(1, "2026-01-01T12:00:00Z", 10, offset=99)], t2) == (0, 0)
    assert spark.read.format("delta").load(ev).count() == 4
    # ... and the same for a re-delivery of an event that was itself late (order 4)
    assert run_events(spark, ev, late, 3, [created(4, "2026-01-01T11:00:00Z", 13, offset=100)], t2) == (0, 0)
    assert spark.read.format("delta").load(late).count() == 1


def test_snapshot_reads_are_not_events(spark, tmp_path):
    ev, late = make_event_tables(spark, str(tmp_path))
    assert run_events(spark, ev, late, 0, [created(1, "2026-01-01T12:00:00Z", 10, op="r")],
                      WatermarkTracker(ev, 600)) == (0, 0)


# ------------------------------------------------------------------ idempotent writes

def test_bronze_batch_replayed_after_a_partial_commit_is_not_duplicated(spark, tmp_path, monkeypatch):
    """The failure the idempotent bronze write fixes: the driver dies after topic A's commit but
    before topic B's. Spark replays the SAME batch id; A must be skipped and B written."""
    from streaming import bronze_ingest

    monkeypatch.setattr(bronze_ingest, "BRONZE", str(tmp_path / "bronze"))
    batch = spark.createDataFrame(
        [("ecom.public.orders", 0, 1, b"a"), ("ecom.public.orders", 0, 2, b"b"), ("ecom.public.users", 0, 1, b"c")],
        "topic string, partition int, offset long, value binary")

    bronze_ingest._write_topic(batch, "ecom.public.orders", 7)          # first attempt: only A committed
    bronze_ingest._write_topic(batch, "ecom.public.orders", 7)          # replay of batch 7: both topics
    bronze_ingest._write_topic(batch, "ecom.public.users", 7)
    assert spark.read.format("delta").load(str(tmp_path / "bronze/orders")).count() == 2   # not 4
    assert spark.read.format("delta").load(str(tmp_path / "bronze/users")).count() == 1

    bronze_ingest._write_topic(batch, "ecom.public.orders", 8)          # a different batch id is real new data
    assert spark.read.format("delta").load(str(tmp_path / "bronze/orders")).count() == 4

    # control: the Week-2 write (plain append) really does duplicate on replay
    plain = str(tmp_path / "plain")
    for _ in range(2):
        batch.write.format("delta").mode("append").save(plain)
    assert spark.read.format("delta").load(plain).count() == 6


def test_quarantine_write_is_idempotent_per_batch_id(spark, tmp_path):
    bad = (spark.createDataFrame([("t", 0, 1, None, bytearray(b"x"), "null_pk")],
                                 "topic string, partition int, offset long, key binary, value binary, reason string"))
    p = str(tmp_path / "q")
    write_quarantine(bad, p, 5)
    write_quarantine(bad, p, 5)    # the replayed batch
    assert spark.read.format("delta").load(p).count() == 1
    write_quarantine(bad, p, 6)    # a genuinely new batch is written
    assert spark.read.format("delta").load(p).count() == 2
