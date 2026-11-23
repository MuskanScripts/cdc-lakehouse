"""One place for Spark + Delta + S3A (MinIO) configuration.

Jobs are launched with `spark-submit --packages ...` (see docker-compose.yml), so the
session itself only sets configs. Unit tests pass `with_packages=True` so a plain
`python -m pytest` run resolves the jars itself.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "org.apache.spark:spark-avro_2.12:3.5.3",
    "io.delta:delta-spark_2.12:3.2.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")


def get_spark(app_name: str, *, master: str | None = None, s3: bool = True,
              with_packages: bool = False) -> SparkSession:
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        # default 200 shuffle partitions is absurd for a 2-core driver and tiny batches
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Delta replays its log with 50 tasks by default; for tables this small that's pure overhead
        .config("spark.databricks.delta.snapshotPartitions", "2")
        # schema drift: a new source column is added to silver by the MERGE itself (ADR-018)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if master:
        b = b.master(master)
    if with_packages:
        b = b.config("spark.jars.packages", PACKAGES).config("spark.jars.ivy", "/root/.ivy2")
    if s3:
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        )
    return b.getOrCreate()
