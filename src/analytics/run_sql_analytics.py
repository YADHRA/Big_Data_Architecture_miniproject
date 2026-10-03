"""Stage 9: run Spark SQL analytics queries and save results to HDFS."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark, CLEANED_PATH, RESULTS_PATH


SQL_FILE = ROOT / "sql" / "spark_sql" / "analytics_queries.sql"
CLEANED_USAGE_PATH = f"{CLEANED_PATH}/usage_cleaned"
SQL_RESULTS = f"{RESULTS_PATH}/sql"


def load_queries(path: Path) -> dict:
    """Load SQL queries separated by '-- name:' markers."""
    queries = {}
    name = None
    buffer = []

    for line in path.read_text().splitlines():

        if line.startswith("-- name:"):

            if name is not None:
                queries[name] = "\n".join(buffer).strip().rstrip(";")

            name = line.split(":", 1)[1].strip()
            buffer = []

        elif name is not None:
            buffer.append(line)

    if name is not None:
        queries[name] = "\n".join(buffer).strip().rstrip(";")

    return queries


spark = get_spark("Stage9-SparkSQL")

usage = spark.read.parquet(CLEANED_USAGE_PATH)

usage.createOrReplaceTempView("usage_cleaned")

n_rows = usage.count()

print()
print("===== SOURCE =====")
print("view 'usage_cleaned' rows:", n_rows)

queries = load_queries(SQL_FILE)

print("queries found:", list(queries))

kpi = None
checks = []

for name, sql in queries.items():

    print()
    print(f"===== {name} =====")

    output_path = f"{SQL_RESULTS}/{name}"

    result = spark.sql(sql)

    (
        result
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    saved = spark.read.parquet(output_path)

    saved.show(50, truncate=False)

    if name == "overall_kpis":

        kpi = saved.collect()[0].asDict()

    elif name.startswith("usage_by_"):

        totals = (
            saved
            .agg(
                F.sum("sessions").alias("session_sum"),
                F.sum("total_data_mb").alias("data_sum"),
                F.count("*").alias("group_count"),
            )
            .collect()[0]
        )

        checks.append(
            (
                name,
                totals["session_sum"],
                totals["data_sum"],
                totals["group_count"],
            )
        )


print()
print("===== CONSISTENCY CHECKS =====")

all_ok = True

if kpi is not None:

    sessions_ok = kpi["total_sessions"] == n_rows
    all_ok = all_ok and sessions_ok

    print(
        "overall_kpis total_sessions == view row count:",
        sessions_ok,
        f"({kpi['total_sessions']} vs {n_rows})"
    )

    for name, session_sum, data_sum, group_count in checks:

        tolerance = 0.005 * (group_count + 1) + 0.01

        session_ok = session_sum == n_rows

        data_ok = abs(data_sum - kpi["total_data_mb"]) <= tolerance

        all_ok = all_ok and session_ok and data_ok

        print(
            f"{name:20s} "
            f"groups={group_count:3d} "
            f"sessions_sum_ok={session_ok} "
            f"data_sum_ok={data_ok}"
        )

print(
    "STAGE 9 CHECKS:",
    "PASSED" if all_ok else "FAILED"
)

spark.stop()
