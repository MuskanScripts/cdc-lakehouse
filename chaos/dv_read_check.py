"""Can our delta-rs readers (reconcile, dbt's delta plugin) read a table with deletion vectors?

Reads the two probe tables from spark/dv_probe.py exactly like reconcile does
(DeltaTable(...).to_pyarrow_table()) and compares row counts and a checksum with what Spark wrote.
A reader that ignored deletion vectors would NOT fail: it would silently return the old versions
of updated rows as well, so the count check matters more than "no exception".
"""
import os
import sys

from deltalake import DeltaTable, __version__

from common import STORAGE

print(f"deltalake {__version__}")
for name in ("plain", "dv"):
    path = f"s3://lakehouse/_probe/orders_{name}"
    try:
        dt = DeltaTable(path, storage_options=STORAGE)
        prot = dt.protocol()
        t = dt.to_pyarrow_table(columns=["order_id"])
        ids = t.column("order_id").to_pylist()
        print(f"{name:<6} OK   protocol reader={prot.min_reader_version} writer={prot.min_writer_version} "
              f"reader_features={getattr(prot, 'reader_features', None)} rows={len(ids)} distinct_ids={len(set(ids))}")
    except Exception as e:
        print(f"{name:<6} FAIL {type(e).__name__}: {str(e)[:300]}")
sys.stdout.flush()
os._exit(0)
