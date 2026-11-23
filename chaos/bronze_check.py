"""Is bronze exactly-once per Kafka offset?

Each Kafka record (topic, partition, offset) must be in bronze at most once. Before the
idempotent write (ADR-019) a batch replayed after a crash appended the same offsets again.
Debezium re-delivering an event after a Connect crash is different: that is a NEW Kafka offset
carrying the same change, which silver's LSN guard absorbs, so it is not counted here.
Exit 1 if any offset appears twice.
"""
import os
import sys
from collections import Counter

from deltalake import DeltaTable

from common import STORAGE, TABLES


def main() -> int:
    bad = 0
    print(f"{'table':<12}{'rows':>10}{'duplicate offsets':>20}")
    for t in TABLES:
        cols = DeltaTable(f"s3://lakehouse/bronze/{t}", storage_options=STORAGE).to_pyarrow_table(
            columns=["topic", "partition", "offset"])
        keys = Counter(zip(cols["topic"].to_pylist(), cols["partition"].to_pylist(), cols["offset"].to_pylist()))
        dupes = sum(c - 1 for c in keys.values() if c > 1)
        bad += dupes
        print(f"{t:<12}{cols.num_rows:>10}{dupes:>20}")
    print("BRONZE OK: every Kafka offset exactly once" if not bad else "BRONZE HAS DUPLICATE OFFSETS")
    return 1 if bad else 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)
