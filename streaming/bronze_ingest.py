"""Bronze: Kafka -> Delta, bytes unchanged.

One streaming query reads every `ecom.public.*` topic and appends to one Delta table per
source table (s3a://lakehouse/bronze/<table>). Nothing is decoded here, so bronze keeps
working through any schema change and can always be replayed into silver.
"""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F
from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from streaming.common.spark_session import KAFKA_BOOTSTRAP, get_spark
from streaming.common.tables import BRONZE, CHECKPOINTS, TABLES
from streaming.common.transforms import schema_id_col

BRONZE_SCHEMA = StructType([
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("schema_id", IntegerType()),
    StructField("ingest_ts", TimestampType()),
])

log = logging.getLogger("bronze")
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")


def ensure_bronze_tables(spark) -> None:
    """Create empty bronze tables so silver can start streaming from them immediately."""
    for table in TABLES:
        spark.createDataFrame([], BRONZE_SCHEMA).write.format("delta").mode("ignore").save(f"{BRONZE}/{table}")


def _write_topic(batch_df, topic: str) -> str:
    table = topic.rsplit(".", 1)[-1]
    (batch_df.filter(F.col("topic") == topic).coalesce(1)
     .write.format("delta").mode("append").save(f"{BRONZE}/{table}"))
    return table


def _write_batch(batch_df, batch_id: int) -> None:
    t0 = time.time()
    batch_df.persist()
    try:
        topics = [r[0] for r in batch_df.select("topic").distinct().collect()]
        with ThreadPoolExecutor(max_workers=len(topics) or 1) as pool:  # independent tables, write in parallel
            list(pool.map(lambda t: _write_topic(batch_df, t), topics))
        log.warning("bronze batch=%s took %.1fs, %s topics", batch_id, time.time() - t0, len(topics))
    finally:
        batch_df.unpersist()


def start_bronze(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", r"ecom\.public\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )
    df = raw.select(
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        "key", "value",
        schema_id_col().alias("schema_id"),
        F.current_timestamp().alias("ingest_ts"),
    )
    return (
        df.writeStream.foreachBatch(_write_batch)
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
        .trigger(processingTime=TRIGGER)
        .queryName("bronze")
        .start()
    )


if __name__ == "__main__":
    spark = get_spark("bronze")
    ensure_bronze_tables(spark)
    start_bronze(spark).awaitTermination()
