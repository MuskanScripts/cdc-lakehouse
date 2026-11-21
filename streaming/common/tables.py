"""What silver needs to know about each source table.

pk       primary key columns (dedup + MERGE key)
ts_cols  Debezium ZonedTimestamp strings that become real timestamps in silver
"""

TABLES = {
    "users": {"pk": ["user_id"], "ts_cols": ["created_at", "updated_at"]},
    "products": {"pk": ["product_id"], "ts_cols": ["updated_at"]},
    "orders": {"pk": ["order_id"], "ts_cols": ["created_at", "updated_at"]},
    "order_items": {"pk": ["order_id", "product_id"], "ts_cols": []},
    "payments": {"pk": ["payment_id"], "ts_cols": ["updated_at"]},
}

BUCKET = "s3a://lakehouse"
BRONZE = f"{BUCKET}/bronze"
SILVER = f"{BUCKET}/silver"
QUARANTINE = f"{BUCKET}/quarantine"
CHECKPOINTS = f"{BUCKET}/_checkpoints"

# Columns silver adds on top of the source columns.
META_COLS = ["_lsn", "_op", "_source_ts", "_silver_ts", "_is_deleted"]
