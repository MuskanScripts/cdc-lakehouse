"""Run bronze, silver and the orders-per-minute aggregate in ONE driver JVM (see ADR-013): WSL
has ~10 GB, so separate Spark JVMs next to Kafka, Connect and Postgres don't fit. If any query
dies, the whole process exits and `restart: unless-stopped` brings it back from the checkpoints."""
import logging

from streaming.bronze_ingest import ensure_bronze_tables, start_bronze
from streaming.common.late_events import ensure_event_tables, start_orders_per_minute
from streaming.common.metrics import start_metrics
from streaming.common.spark_session import get_spark
from streaming.silver_merge import start_silver

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    spark = get_spark("cdc-streaming")
    spark.sparkContext.setLogLevel("WARN")
    start_metrics(spark)   # Prometheus :9109/metrics, batch duration + rows/s per query (ADR-031)
    ensure_bronze_tables(spark)
    ensure_event_tables(spark)
    start_bronze(spark)
    start_silver(spark)
    start_orders_per_minute(spark)
    spark.streams.awaitAnyTermination()  # re-raises the failing query's exception
