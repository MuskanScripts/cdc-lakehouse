"""Smoke test: Spark can write and read a Delta table on MinIO (s3a://lakehouse).

Run with `make spark-smoke`. If this passes, Week 2's bronze job has a working
storage layer.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("delta-minio-smoke")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .getOrCreate()
)

path = "s3a://lakehouse/_smoke/delta_check"
df = spark.range(0, 1000).withColumnRenamed("id", "n")
df.write.format("delta").mode("overwrite").save(path)

read_back = spark.read.format("delta").load(path)
count = read_back.count()
history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "operation").collect()

print(f"\nDELTA SMOKE TEST: wrote and read {count} rows at {path}")
print(f"Delta log versions: {[(r.version, r.operation) for r in history]}")
assert count == 1000, "row count mismatch"
print("PASS\n")
spark.stop()
