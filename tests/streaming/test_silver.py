"""Silver correctness: dedup, soft-delete MERGE, replay safety, quarantine.

Events are real Confluent-framed Avro (0x00 + schema id + payload) built with to_avro, so
decode_table() is exercised end to end exactly as it runs in production.
"""
import json
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import (DecimalType, LongType, StringType, StructField,
                               StructType)

from streaming.common.transforms import (decode_table, dedup_latest,
                                         merge_into_silver, schema_id_col,
                                         to_silver_rows)

VALUE = {
    "type": "record", "name": "Value", "fields": [
        {"name": "order_id", "type": ["null", "long"], "default": None},
        {"name": "user_id", "type": "long"},
        {"name": "status", "type": "string"},
        {"name": "total", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
        {"name": "updated_at", "type": "string"},
    ],
}
ENVELOPE = json.dumps({
    "type": "record", "name": "Envelope", "fields": [
        {"name": "before", "type": ["null", VALUE], "default": None},
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "source", "type": {"type": "record", "name": "Source", "fields": [
            {"name": "lsn", "type": ["null", "long"], "default": None},
            {"name": "ts_ms", "type": "long"}]}},
        {"name": "op", "type": "string"},
    ],
})
SID = 7

ROW_T = StructType([
    StructField("order_id", LongType()), StructField("user_id", LongType()),
    StructField("status", StringType()), StructField("total", DecimalType(12, 2)),
    StructField("updated_at", StringType()),
])
ENV_T = StructType([
    StructField("before", ROW_T), StructField("after", ROW_T),
    StructField("source", StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])),
    StructField("op", StringType()),
])


def row(order_id, status, total="10.00"):
    return (order_id, 1, status, Decimal(total), "2026-01-01T00:00:00.000000Z")


def ev(op, lsn, offset, order_id=1, status="created", total="10.00"):
    """One Debezium event; deletes carry the row in `before`, others in `after`."""
    r = row(order_id, status, total)
    return dict(op=op, lsn=lsn, offset=offset, before=r if op == "d" else None, after=None if op == "d" else r)


def bronze(spark, events):
    data = [((e["before"], e["after"], (e["lsn"], 1_700_000_000_000 + e["lsn"]), e["op"]), e["offset"]) for e in events]
    schema = StructType([StructField("env", ENV_T), StructField("offset", LongType())])
    df = spark.createDataFrame(data, schema)
    framed = F.concat(F.unhex(F.lit("00%08x" % SID)), to_avro(F.col("env"), ENVELOPE))
    return (df.select(F.lit("ecom.public.orders").alias("topic"), F.lit(0).alias("partition"), "offset",
                      F.lit(None).cast("binary").alias("key"), framed.alias("value"))
            .withColumn("schema_id", schema_id_col()))


def schema_for(sid):
    assert sid == SID
    return ENVELOPE


def apply_batch(spark, events, path):
    good, bad = decode_table(bronze(spark, events), "orders", schema_for)
    if good is not None:
        merge_into_silver(to_silver_rows(good, "orders"), path, ["order_id"])
    return bad


def silver(spark, path):
    return {r.order_id: r for r in spark.read.format("delta").load(path).collect()}


def test_schema_id_read_from_bytes_2_to_5(spark):
    df = spark.createDataFrame([(bytearray(b"\x00\x00\x00\x01\x02payload"),), (bytearray(b"\x01\x00\x00\x00\x05x"),),
                                (bytearray(b"\x00\x00"),)], ["value"])
    ids = [r.schema_id for r in df.withColumn("schema_id", schema_id_col()).collect()]
    assert ids == [258, None, None]  # 0x0102; wrong magic byte; too short


def test_dedup_keeps_highest_lsn_then_highest_offset(spark):
    df = spark.createDataFrame(
        [(1, 10, 5, "old"), (1, 30, 1, "newest-lsn"), (1, 20, 9, "mid"), (2, 5, 1, "a"), (2, 5, 2, "same-lsn-higher-offset")],
        ["order_id", "_lsn", "_offset", "tag"])
    out = {r.order_id: r.tag for r in dedup_latest(df, ["order_id"]).collect()}
    assert out == {1: "newest-lsn", 2: "same-lsn-higher-offset"}


def test_merge_insert_update_delete_reinsert(spark, tmp_path):
    p = str(tmp_path / "orders")

    apply_batch(spark, [ev("c", 10, 1, status="created")], p)
    r = silver(spark, p)[1]
    assert (r.status, r._is_deleted, r._lsn, r._op) == ("created", False, 10, "c")

    apply_batch(spark, [ev("u", 20, 2, status="paid", total="12.50")], p)
    r = silver(spark, p)[1]
    assert (r.status, r.total, r._is_deleted, r._lsn) == ("paid", Decimal("12.50"), False, 20)

    apply_batch(spark, [ev("d", 30, 3, status="paid", total="12.50")], p)
    r = silver(spark, p)[1]
    assert (r._is_deleted, r._lsn, r._op) == (True, 30, "d")  # tombstone kept, not removed
    assert spark.read.format("delta").load(p).count() == 1

    apply_batch(spark, [ev("c", 40, 4, status="created")], p)  # same key comes back
    r = silver(spark, p)[1]
    assert (r.status, r._is_deleted, r._lsn) == ("created", False, 40)
    assert spark.read.format("delta").load(p).count() == 1


def test_replaying_older_batch_changes_nothing(spark, tmp_path):
    p = str(tmp_path / "orders")
    apply_batch(spark, [ev("c", 10, 1), ev("u", 20, 2, status="paid"), ev("d", 30, 3, status="paid")], p)
    before = {k: v.asDict() for k, v in silver(spark, p).items()}
    assert before[1]["_is_deleted"] is True

    # replay: the old insert/update would resurrect the row if deletes were hard
    apply_batch(spark, [ev("c", 10, 1), ev("u", 20, 2, status="paid")], p)
    after = {k: v.asDict() for k, v in silver(spark, p).items()}
    keep = lambda d: {k: v for k, v in d.items() if k != "_silver_ts"}  # noqa: E731
    assert {k: keep(v) for k, v in after.items()} == {k: keep(v) for k, v in before.items()}

    apply_batch(spark, [ev("d", 30, 3, status="paid")], p)  # replaying the delete itself is a no-op too
    assert silver(spark, p)[1]._is_deleted is True


def test_one_batch_with_many_versions_lands_on_the_latest(spark, tmp_path):
    p = str(tmp_path / "orders")
    apply_batch(spark, [ev("u", 25, 3, status="shipped"), ev("c", 10, 1), ev("u", 20, 2, status="paid")], p)
    r = silver(spark, p)[1]
    assert (r.status, r._lsn) == ("shipped", 25)


def test_null_pk_goes_to_quarantine_not_silver(spark, tmp_path):
    p = str(tmp_path / "orders")
    bad = apply_batch(spark, [ev("c", 10, 1, order_id=1), ev("c", 11, 2, order_id=None)], p)
    assert set(silver(spark, p)) == {1}
    reasons = [(r.reason, r.offset) for r in bad.collect()]
    assert reasons == [("null_pk", 2)]


def test_undecodable_and_unframed_values_are_quarantined(spark, tmp_path):
    good = bronze(spark, [ev("c", 10, 1)])
    junk = spark.createDataFrame(
        [("t", 0, 50, None, bytearray(b"\x00\x00\x00\x00\x07\xff\xff\xff\xff"), ),
         ("t", 0, 51, None, bytearray(b"not avro at all"))],
        "topic string, partition int, offset long, key binary, value binary",
    ).withColumn("schema_id", schema_id_col())
    g, bad = decode_table(good.unionByName(junk), "orders", schema_for)
    merge_into_silver(to_silver_rows(g, "orders"), str(tmp_path / "orders"), ["order_id"])
    reasons = {r.offset: r.reason for r in bad.collect()}
    assert reasons == {50: "avro_decode_failed", 51: "bad_wire_format"}
    assert set(silver(spark, str(tmp_path / "orders"))) == {1}


def test_unknown_schema_id_is_quarantined(spark):
    from streaming.common.schema_registry import SchemaNotFound

    def missing(_sid):
        raise SchemaNotFound(_sid)

    g, bad = decode_table(bronze(spark, [ev("c", 10, 1)]), "orders", missing)
    assert g is None
    assert [r.reason for r in bad.collect()] == ["schema_not_found"]


def test_process_batch_end_to_end_merges_and_quarantines(spark, tmp_path):
    """The real foreachBatch body: groups by table/schema id, merges in threads, writes quarantine."""
    from streaming.common.transforms import process_batch

    batch = bronze(spark, [ev("c", 10, 1), ev("u", 20, 2, status="paid"), ev("c", 11, 3, order_id=None)])
    batch = batch.withColumn("_table", F.lit("orders"))
    silver_root, q_root = str(tmp_path / "silver"), str(tmp_path / "quarantine")

    process_batch(batch, 5, schema_for, silver_root, q_root)

    s = silver(spark, f"{silver_root}/orders")
    assert set(s) == {1} and s[1].status == "paid" and s[1]._lsn == 20
    q = spark.read.format("delta").load(f"{q_root}/orders").collect()
    assert [(r.reason, r.offset, r.batch_id) for r in q] == [("null_pk", 3, 5)]
