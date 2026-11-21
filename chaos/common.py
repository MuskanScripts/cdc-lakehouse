"""Shared helpers: read Postgres and silver (Delta on MinIO, via delta-rs, no JVM)."""
import os
from datetime import datetime, timezone

import psycopg
from deltalake import DeltaTable

# table -> (primary key, compared business columns)
TABLES = {
    "users": (["user_id"], ["name", "email", "city", "tier", "created_at", "updated_at"]),
    "products": (["product_id"], ["name", "category", "price", "updated_at"]),
    "orders": (["order_id"], ["user_id", "status", "total", "created_at", "updated_at"]),
    "order_items": (["order_id", "product_id"], ["qty", "price"]),
    "payments": (["payment_id"], ["order_id", "method", "status", "amount", "updated_at"]),
}

STORAGE = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345"),
    "AWS_ENDPOINT_URL": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_REGION": "us-east-1",
}


def norm(v):
    """Make Postgres and Delta values comparable: timestamps -> naive UTC."""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    return v


def pg_rows(table: str) -> list[dict]:
    with psycopg.connect(autocommit=True) as conn, conn.cursor() as cur:  # PG* env vars
        cur.execute(f"SELECT * FROM {table}")
        cols = [d.name for d in cur.description]
        return [{c: norm(v) for c, v in zip(cols, row)} for row in cur.fetchall()]


def silver_rows(table: str) -> list[dict]:
    """ALL silver rows including soft-deleted tombstones."""
    dt = DeltaTable(f"s3://lakehouse/silver/{table}", storage_options=STORAGE)
    return [{k: norm(v) for k, v in r.items()} for r in dt.to_pyarrow_table().to_pylist()]
