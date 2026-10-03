"""Stage 10: create the Hive database/tables over existing Parquet and verify them."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark, RESULTS_PATH

SQL_FILE = ROOT / "sql" / "hive" / "create_tables.sql"


def statements(path: Path) -> list:
    lines = [
        line
        for line in path.read_text().splitlines()
        if not line.strip().startswith("--")
    ]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


spark = get_spark("Stage10-Hive", hive=True)

print("\n===== HIVE SETTINGS =====")
print("catalog implementation:", spark.conf.get("spark.sql.catalogImplementation"))
print("warehouse dir         :", spark.conf.get("spark.sql.warehouse.dir"))

print("\n===== CREATING DATABASE AND TABLES =====")
for stmt in statements(SQL_FILE):
    print(">>", " ".join(stmt.split())[:110])
    spark.sql(stmt)

print("\n===== DATABASES =====")
spark.sql("SHOW DATABASES").show(truncate=False)

print("===== TABLES IN internet_usage =====")
spark.sql("SHOW TABLES IN internet_usage").show(truncate=False)

for table_name in [
    "internet_usage.raw_usage",
    "internet_usage.cleaned_usage",
]:
    print(f"\n===== {table_name}: metadata =====")
    (
        spark.sql(f"DESCRIBE EXTENDED {table_name}")
        .filter(
            F.col("col_name").isin(
                "Catalog",
                "Database",
                "Table",
                "Type",
                "Provider",
                "Location",
            )
        )
        .show(truncate=False)
    )

    spark.table(table_name).printSchema()

print("\n===== VERIFICATION =====")

cleaned = spark.table("internet_usage.cleaned_usage")
raw = spark.table("internet_usage.raw_usage")

n_cleaned = cleaned.count()
n_raw = raw.count()

sum_mb = cleaned.agg(
    F.round(F.sum("data_consumed_mb"), 2)
).collect()[0][0]

print("raw_usage rows              :", n_raw)
print("cleaned_usage rows          :", n_cleaned)
print("cleaned_usage total_data_mb:", sum_mb)

kpi = (
    spark.read
    .parquet(f"{RESULTS_PATH}/sql/overall_kpis")
    .collect()[0]
    .asDict()
)

ok_counts = n_raw == n_cleaned
ok_kpi_rows = n_cleaned == kpi["total_sessions"]
ok_kpi_mb = abs(sum_mb - kpi["total_data_mb"]) <= 0.01

print(
    "raw rows == cleaned rows         :",
    ok_counts,
)

print(
    "rows == Stage 9 total_sessions   :",
    ok_kpi_rows,
    f"({n_cleaned} vs {kpi['total_sessions']})",
)

print(
    "data MB == Stage 9 total_data_mb :",
    ok_kpi_mb,
    f"({sum_mb} vs {kpi['total_data_mb']})",
)

via_hive = spark.sql("""
    SELECT
        app_category,
        COUNT(*) AS sessions,
        ROUND(SUM(data_consumed_mb), 2) AS total_data_mb
    FROM internet_usage.cleaned_usage
    GROUP BY app_category
""")

saved = (
    spark.read
    .parquet(f"{RESULTS_PATH}/sql/usage_by_category")
    .select("app_category", "sessions", "total_data_mb")
)

via_hive_rows = {
    row["app_category"]: (row["sessions"], row["total_data_mb"])
    for row in via_hive.collect()
}

saved_rows = {
    row["app_category"]: (row["sessions"], row["total_data_mb"])
    for row in saved.collect()
}

diff = sum(
    via_hive_rows.get(category) != saved_rows.get(category)
    for category in set(via_hive_rows) | set(saved_rows)
)

print(
    "category result differences vs Stage 9 (3 rows):",
    diff,
)

via_hive.orderBy("app_category").show(truncate=False)

ok = ok_counts and ok_kpi_rows and ok_kpi_mb and diff == 0

print(
    "STAGE 10 CHECKS:",
    "PASSED" if ok else "FAILED",
)

spark.stop()
