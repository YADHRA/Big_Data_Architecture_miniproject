"""
Stage 18A - Compact fact table ("cube") for Power BI

Grain: usage_date x app_name x app_category x operator x network_type x time_of_day
Columns hold SUMS (sessions, data, duration, anomalies, anomaly data) so Power BI can
re-aggregate them correctly with DAX. Averages must be computed as total / sessions.

Synthetic dataset. Usage anomalies, not fraud.

Run from the project root with the venv active:
    python -m src.ml.powerbi_cube_export
"""

import csv
import json
import math
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark

ANOMALY_PATH = "hdfs://localhost:9000/internet_usage/results/usage_anomalies"
OUT_DIR = "powerbi_data"
OUT_FILE = os.path.join(OUT_DIR, "usage_cube.csv")
REPORT_PATH = "docs/stage18_cube_report.json"
MAX_CUBE_ROWS = 200000  # guard: the cube must stay small

NEEDED = ["timestamp", "app_name", "app_category", "operator", "network_type",
          "time_of_day", "data_consumed_mb", "session_duration_minutes", "anomaly_flag"]
GROUP_COLS = ["usage_date", "app_name", "app_category", "operator",
              "network_type", "time_of_day"]


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs("docs", exist_ok=True)
    spark = get_spark()
    checks = []

    def check(name, ok, detail):
        checks.append({"check": name, "passed": bool(ok), "detail": detail})
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    a = spark.read.parquet(ANOMALY_PATH)
    missing = [c for c in NEEDED if c not in a.columns]
    if missing:
        print(f"STOP: usage_anomalies is missing columns {missing}")
        spark.stop()
        sys.exit(1)

    a = a.withColumn("usage_date", F.date_format(F.to_timestamp("timestamp"), "yyyy-MM-dd"))

    src = a.agg(
        F.count("*").alias("sessions"),
        F.sum("data_consumed_mb").alias("data_mb"),
        F.sum("session_duration_minutes").alias("duration_min"),
        F.sum("anomaly_flag").alias("anomalies"),
        F.sum(F.when(F.col("usage_date").isNull(), 1).otherwise(0)).alias("null_dates"),
    ).first()

    cube = (
        a.groupBy(*GROUP_COLS)
        .agg(
            F.count("*").alias("sessions"),
            F.sum("data_consumed_mb").alias("total_data_mb"),
            F.sum("session_duration_minutes").alias("total_duration_min"),
            F.sum("anomaly_flag").alias("anomalies"),
            F.sum(F.when(F.col("anomaly_flag") == 1, F.col("data_consumed_mb"))
                  .otherwise(F.lit(0.0))).alias("anomaly_data_mb"),
        )
        .orderBy(*GROUP_COLS)
        .cache()  # tiny result, reused for count + collect
    )
    n_rows = cube.count()
    print(f"cube rows: {n_rows:,}")
    if n_rows > MAX_CUBE_ROWS:
        print(f"STOP: cube has {n_rows:,} rows (> {MAX_CUBE_ROWS:,}). Grain is unexpectedly large.")
        spark.stop()
        sys.exit(1)

    rows = [tuple(r) for r in cube.collect()]  # small, aggregated
    cols = cube.columns
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
    print(f"wrote {OUT_FILE} ({os.path.getsize(OUT_FILE):,} bytes)")

    # ------------------------------------------------------------ validation
    print("\nCHECKS")
    i = {c: cols.index(c) for c in cols}
    cube_sessions = sum(int(r[i["sessions"]]) for r in rows)
    cube_anoms = sum(int(r[i["anomalies"]]) for r in rows)
    cube_mb = sum(float(r[i["total_data_mb"]]) for r in rows)
    cube_dur = sum(float(r[i["total_duration_min"]]) for r in rows)

    check("sessions", cube_sessions == src["sessions"], f"{cube_sessions:,} vs {src['sessions']:,}")
    check("anomalies", cube_anoms == int(src["anomalies"]), f"{cube_anoms:,} vs {int(src['anomalies']):,}")
    check("total data", math.isclose(cube_mb, src["data_mb"], rel_tol=1e-9),
          f"{cube_mb:.4f} vs {src['data_mb']:.4f}")
    check("total duration", math.isclose(cube_dur, src["duration_min"], rel_tol=1e-9),
          f"{cube_dur:.4f} vs {src['duration_min']:.4f}")
    check("no null dates", src["null_dates"] == 0, f"{src['null_dates']} null usage_date values")

    # Dim_App needs each app in exactly one category
    n_apps = a.select("app_name").distinct().count()
    n_pairs = a.select("app_name", "app_category").distinct().count()
    check("each app maps to one category", n_apps == n_pairs,
          f"{n_apps} apps, {n_pairs} app/category pairs")

    report = {
        "stage": "18A",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": ANOMALY_PATH,
        "output": OUT_FILE,
        "grain": GROUP_COLS,
        "cube_rows": n_rows,
        "source_totals": {k: (float(v) if v is not None else None) for k, v in src.asDict().items()},
        "checks": checks,
    }
    with open(REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"report: {REPORT_PATH}")

    cube.unpersist()
    spark.stop()
    if any(not c["passed"] for c in checks):
        print("STAGE 18A NOT COMPLETE: fix failed checks first.")
        sys.exit(1)
    print("STAGE 18A COMPLETE.")


if __name__ == "__main__":
    main()