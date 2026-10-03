"""Stage 7: run the cleaning rules and write a validation report. Writes NOTHING to HDFS."""
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark, RAW_COMPAT_PATH
from src.preprocessing.cleaning import (
    EXPECTED_SCHEMA,
    standardize,
    add_validation,
)

REPORT = ROOT / "docs" / "cleaning_report.txt"

out = io.StringIO()


def p(*args):
    print(*args)
    print(*args, file=out)


spark = get_spark("Stage7-Cleaning")

raw = spark.read.parquet(RAW_COMPAT_PATH)


# ---- 1. Schema check ----

actual = dict(raw.dtypes)

mismatch = {
    c: (t, actual.get(c))
    for c, t in EXPECTED_SCHEMA.items()
    if actual.get(c) != t
}

extra = sorted(set(actual) - set(EXPECTED_SCHEMA))

p("=== 1. SCHEMA CHECK ===")
p("type mismatches (expected, actual):", mismatch or "none")
p("unexpected extra columns          :", extra or "none")

if mismatch or extra:
    spark.stop()
    raise SystemExit("Schema differs from the audit. Stopping.")


# ---- 2. Standardize, remove exact duplicates, validate ----

std = standardize(raw)

rows_in = std.count()

flagged = add_validation(
    std.dropDuplicates()
)

ok_cols = [
    c for c in flagged.columns
    if c.startswith("ok_")
]


# ---- 3. One aggregation for all validation counts ----

aggs = [
    F.count("*").alias("rows_after_exact_dedupe"),

    F.countDistinct("session_id").alias(
        "distinct_session_ids"
    ),

    F.sum(
        F.col("is_valid").cast("int")
    ).alias("valid_rows"),

    F.min(
        F.when(
            F.col("is_valid"),
            F.col("data_consumed_mb")
        )
    ).alias("valid_min_mb"),

    F.max(
        F.when(
            F.col("is_valid"),
            F.col("data_consumed_mb")
        )
    ).alias("valid_max_mb"),

    F.min(
        F.when(
            F.col("is_valid"),
            F.col("session_duration_minutes")
        )
    ).alias("valid_min_dur"),

    F.max(
        F.when(
            F.col("is_valid"),
            F.col("session_duration_minutes")
        )
    ).alias("valid_max_dur"),

    F.min(
        F.when(
            F.col("is_valid"),
            F.col("timestamp")
        )
    ).alias("valid_min_ts"),

    F.max(
        F.when(
            F.col("is_valid"),
            F.col("timestamp")
        )
    ).alias("valid_max_ts"),
] + [
    F.sum(
        (~F.col(c)).cast("int")
    ).alias(
        "fail_" + c[3:]
    )
    for c in ok_cols
]


# Collect ONLY the single summary row.
res = flagged.agg(*aggs).collect()[0].asDict()


# ---- 4. Row counts ----

p("\n=== 2. ROW COUNTS ===")

p("rows read                         :", rows_in)

p(
    "rows after exact-duplicate removal:",
    res["rows_after_exact_dedupe"],
)

p(
    "exact duplicate rows removed      :",
    rows_in - res["rows_after_exact_dedupe"],
)

p(
    "distinct session_ids              :",
    res["distinct_session_ids"],
)

p(
    "duplicate session_ids (rows - distinct):",
    res["rows_after_exact_dedupe"]
    - res["distinct_session_ids"],
)


# ---- 5. Rule failure counts ----

p(
    "\n=== 3. ROWS FAILING EACH RULE "
    "(a row can fail more than one) ==="
)

for k, v in res.items():
    if k.startswith("fail_"):
        p(f"{k[5:]:28s}: {v}")


# ---- 6. Final result ----

n_valid = res["valid_rows"] or 0

n_after = res["rows_after_exact_dedupe"]

p("\n=== 4. RESULT ===")

p("valid rows   :", n_valid)

p(
    "rejected rows:",
    n_after - n_valid
)


# ---- 7. Confirm extremes were preserved ----

p(
    "\n=== 5. EXTREMES ARE PRESERVED "
    "(ranges among VALID rows) ==="
)

p(
    "data_consumed_mb          :",
    res["valid_min_mb"],
    "to",
    res["valid_max_mb"],
)

p(
    "session_duration_minutes  :",
    res["valid_min_dur"],
    "to",
    res["valid_max_dur"],
)

p(
    "timestamp                 :",
    res["valid_min_ts"],
    "to",
    res["valid_max_ts"],
)


# ---- 8. Show rejected samples only if needed ----

if n_after - n_valid > 0:

    p(
        "\n=== 6. SAMPLE OF REJECTED ROWS "
        "(max 10) ==="
    )

    rej = (
        flagged
        .filter(~F.col("is_valid"))
        .select(
            *EXPECTED_SCHEMA.keys(),
            "reject_reason",
        )
        .limit(10)
    )

    for r in rej.collect():
        p(r.asDict())


# ---- 9. Save local report ----

REPORT.parent.mkdir(exist_ok=True)

REPORT.write_text(
    out.getvalue()
)

print(
    "\nReport saved to:",
    REPORT
)

spark.stop()
