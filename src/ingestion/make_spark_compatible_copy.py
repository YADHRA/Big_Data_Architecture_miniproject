"""Stage 6 fix: write a copy of the raw Parquet with timestamp[us] so Spark can read it.
The original file is NEVER modified. The cast is safe: it raises if any value would lose precision.
"""
import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "raw" / "application_data_consumption_breakdown.parquet"
OUT_DIR = ROOT / "data" / "raw" / "compat_us"
OUT = OUT_DIR / "application_data_consumption_breakdown_us.parquet"
CHECKSUM_FILE = ROOT / "docs" / "dataset_checksum.txt"

# 1. Confirm we are starting from the exact original file
expected = CHECKSUM_FILE.read_text().split()[0]
actual = hashlib.sha256(SRC.read_bytes()).hexdigest()

print("expected sha256:", expected)
print("actual   sha256:", actual)

if expected != actual:
    raise SystemExit("Source file does not match the recorded checksum. Stopping.")

# 2. Read, then cast ONLY the timestamp column ns -> us
table = pq.read_table(SRC)

print("\nORIGINAL schema:")
print(table.schema)

idx = table.schema.get_field_index("timestamp")
new_ts = table.column("timestamp").cast(
    pa.timestamp("us"),
    safe=True
)

new_table = table.set_column(
    idx,
    pa.field("timestamp", pa.timestamp("us")),
    new_ts
)

# 3. Prove conversion is lossless
roundtrip_ok = new_table.cast(table.schema).equals(table)

print("\nrows original / copy:", table.num_rows, "/", new_table.num_rows)
print(
    "lossless round-trip (all columns, all rows):",
    roundtrip_ok
)

if not roundtrip_ok or table.num_rows != new_table.num_rows:
    raise SystemExit("Copy is NOT identical to the original. Not writing.")

# 4. Write the Spark-compatible copy
OUT_DIR.mkdir(parents=True, exist_ok=True)

pq.write_table(
    new_table,
    OUT,
    compression="snappy"
)

check = pq.ParquetFile(OUT)

print("\nCOPY schema:")
print(check.schema_arrow)

print("copy rows:", check.metadata.num_rows)
print(f"copy size: {OUT.stat().st_size / (1024 * 1024):.2f} MB")
print("Saved to:", OUT)
