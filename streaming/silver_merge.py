"""Silver: bronze Delta -> decoded, deduplicated, MERGEd Delta tables.

One streaming query reads all five bronze tables (unioned, tagged with `_table`) and
handles them together in foreachBatch: one micro-batch, one checkpoint, one JVM.

Current state of a table = rows where `_is_deleted = false`, e.g. in Spark SQL:
    SELECT * FROM delta.`s3a://lakehouse/silver/orders` WHERE NOT _is_deleted
"""
import os
from functools import reduce

from pyspark.sql import functions as F

from streaming.common.schema_registry import SchemaRegistry
from streaming.common.spark_session import SCHEMA_REGISTRY_URL, get_spark
from streaming.common.tables import BRONZE, CHECKPOINTS, QUARANTINE, SILVER, TABLES
from streaming.common.transforms import process_batch

MAX_FILES = int(os.environ.get("MAX_FILES_PER_TRIGGER", "20"))
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")


def start_silver(spark):
    registry = SchemaRegistry(SCHEMA_REGISTRY_URL)
    sources = [
        spark.readStream.format("delta").option("maxFilesPerTrigger", MAX_FILES)
        .load(f"{BRONZE}/{t}").withColumn("_table", F.lit(t))
        for t in TABLES
    ]
    stream = reduce(lambda a, b: a.unionByName(b), sources)
    return (
        stream.writeStream
        .foreachBatch(lambda df, bid: process_batch(df, bid, registry.writer_schema, SILVER, QUARANTINE))
        .option("checkpointLocation", f"{CHECKPOINTS}/silver")
        .trigger(processingTime=TRIGGER)
        .queryName("silver")
        .start()
    )


if __name__ == "__main__":
    spark = get_spark("silver")
    start_silver(spark).awaitTermination()
