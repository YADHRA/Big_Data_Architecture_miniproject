"""Stage 8: write validated data to HDFS as Parquet, then verify it by reading it back."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark, RAW_COMPAT_PATH, CLEANED_PATH
from src.preprocessing.cleaning import EXPECTED_SCHEMA, standardize, add_validation

CLEANED_USAGE_PATH = f"{CLEANED_PATH}/usage_cleaned"
REJECTED_PATH = f"{CLEANED_PATH}/rejected_rows"
COLS = list(EXPECTED_SCHEMA.keys())

spark = get_spark("Stage8-WriteCleaned")

# Read the same Spark-compatible source used in Stage 7.
raw = spark.read.parquet(RAW_COMPAT_PATH)

# Reuse Stage 7 cleaning and validation logic.
flagged = add_validation(standardize(raw).dropDuplicates())

# Keep only the original 10 columns for the cleaned dataset.
valid = flagged.filter(F.col("is_valid")).select(*COLS)

# ---- 1. Write valid rows as one Snappy-compressed Parquet file ----
print("\n===== WRITING CLEANED DATA =====")
print("target:", CLEANED_USAGE_PATH)

(
    valid.coalesce(1)
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(CLEANED_USAGE_PATH)
)

# ---- 2. Write rejected rows only if any exist ----
rejected = flagged.filter(~F.col("is_valid")).select(*COLS, "reject_reason")
n_rejected = rejected.count()

print("rejected rows:", n_rejected)

if n_rejected > 0:
    (
        rejected.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(REJECTED_PATH)
    )
    print("rejected rows written to:", REJECTED_PATH)
else:
    print("no rejected rows, nothing written to", REJECTED_PATH)

# ---- 3. Read the written Parquet back and verify ----
print("\n===== VERIFYING (reading the written Parquet back) =====")

back = spark.read.parquet(CLEANED_USAGE_PATH)

print("columns:", len(back.columns), "|", back.columns)

actual_types = dict(back.dtypes)

type_diff = {
    c: (t, actual_types.get(c))
    for c, t in EXPECTED_SCHEMA.items()
    if actual_types.get(c) != t
}

print(
    "type differences vs expected schema:",
    type_diff or "none"
)

n_back = back.count()
n_valid = valid.count()

print("rows read back      :", n_back)
print("valid rows (source) :", n_valid)
print("row counts equal    :", n_back == n_valid)

missing_in_back = valid.exceptAll(back).count()
extra_in_back = back.exceptAll(valid).count()

print(
    "rows in source but not in written data:",
    missing_in_back
)

print(
    "rows in written data but not in source:",
    extra_in_back
)

print(
    "distinct session_id :",
    back.select("session_id").distinct().count()
)

back.agg(
    F.min("data_consumed_mb").alias("min_mb"),
    F.max("data_consumed_mb").alias("max_mb"),
    F.min("session_duration_minutes").alias("min_dur"),
    F.max("session_duration_minutes").alias("max_dur"),
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts"),
).show(truncate=False)

ok = (
    not type_diff
    and n_back == n_valid
    and missing_in_back == 0
    and extra_in_back == 0
)

print(
    "STAGE 8 VERIFICATION:",
    "PASSED" if ok else "FAILED"
)

print(
    "partitions when reading back:",
    back.rdd.getNumPartitions()
)

spark.stop()

if not ok:
    raise SystemExit(1)
