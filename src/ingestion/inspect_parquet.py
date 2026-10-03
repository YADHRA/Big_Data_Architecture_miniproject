"""Stage 2: inspect the Parquet file WITHOUT loading it fully into memory."""
from pathlib import Path
import pyarrow.parquet as pq

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

# The file may sit in a subfolder if the repo stores it that way.
candidates = list(RAW_DIR.rglob("application_data_consumption_breakdown.parquet"))
if not candidates:
    raise SystemExit("Parquet file not found under data/raw/. Run download_dataset.py first.")
path = candidates[0]

pf = pq.ParquetFile(path)
meta = pf.metadata

print("=== FILE ===")
print("Path           :", path)
print(f"Size on disk   : {path.stat().st_size / (1024 * 1024):.2f} MB")

print("\n=== METADATA (read from the footer, no data scanned) ===")
print("Rows           :", meta.num_rows)
print("Columns        :", meta.num_columns)
print("Row groups     :", meta.num_row_groups)
print("Created by     :", meta.created_by)

print("\n=== SCHEMA ===")
print(pf.schema_arrow)

print("\n=== COLUMN NAMES ===")
print(pf.schema_arrow.names)

print("\n=== SAMPLE (first 5 rows only) ===")
first_batch = next(pf.iter_batches(batch_size=5))
import pandas as pd
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
print(first_batch.to_pandas())
