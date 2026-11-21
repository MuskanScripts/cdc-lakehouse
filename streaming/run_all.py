"""Run bronze and silver in ONE driver JVM (see ADR-013): WSL has ~7.5 GB, so two Spark
JVMs next to Kafka, Connect and Postgres don't fit. If either query dies, the whole process
exits and `restart: unless-stopped` brings it back from the checkpoints."""
import logging

from streaming.bronze_ingest import ensure_bronze_tables, start_bronze
from streaming.common.spark_session import get_spark
from streaming.silver_merge import start_silver

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    spark = get_spark("cdc-streaming")
    spark.sparkContext.setLogLevel("WARN")
    ensure_bronze_tables(spark)
    start_bronze(spark)
    start_silver(spark)
    spark.streams.awaitAnyTermination()  # re-raises the failing query's exception
