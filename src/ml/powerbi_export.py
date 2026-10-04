"""
Stage 17 - Power BI Data Preparation and Export

Reads validated Stage 16 anomaly results plus Stage 13/15 clustering results,
creates small Power BI-ready CSV tables, validates reconciliation totals,
and writes a Stage 17 JSON report.

Important:
- Dataset is synthetic.
- Anomalies are usage anomalies, NOT fraud labels.
- No ground-truth anomaly labels exist.
- Stage 18 will build the Power BI dashboard.

Run from project root:
    python -m src.ml.powerbi_export
"""

import csv
import json
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark


# ============================================================================
# CONFIGURATION
# ============================================================================

BASE = "hdfs://localhost:9000/internet_usage/results"

ANOMALY_PATH = f"{BASE}/usage_anomalies"
SESSION_CLUSTER_PATH = f"{BASE}/session_clusters"
CUSTOMER_PROFILE_PATH = f"{BASE}/customer_cluster_profile"

OUT_DIR = "powerbi_data"
REPORT_PATH = "docs/stage17_powerbi_export_report.json"

VALUE = "data_consumed_mb"
DURATION = "session_duration_minutes"

DATA_NOTES = [
    (
        "dataset",
        "Synthetic data",
        "The dataset is a synthetic telecom usage dataset designed to simulate "
        "application-level internet consumption patterns. It is not real customer data."
    ),
    (
        "architecture",
        "Architecture",
        "Local/pseudo-distributed Big Data implementation using HDFS, Spark, "
        "Hive and Spark MLlib."
    ),
    (
        "anomaly_scope",
        "Anomaly detection scope",
        "Usage anomaly detection only, not fraud detection. There are no "
        "ground-truth anomaly labels, so accuracy, precision, recall, F1 and "
        "ROC-AUC are not reported."
    ),
    (
        "anomaly_method",
        "Anomaly method",
        "Category-specific IQR thresholds were calculated during Stage 16. "
        "A session is flagged when its data consumption exceeds the threshold "
        "for its app category."
    ),
    (
        "customer_clusters",
        "Customer segments caveat",
        "Customer clusters are descriptive aggregate usage segments for repeat "
        "customers. Shuffled-session controls produced similar silhouette values, "
        "so these clusters are not strong evidence of persistent individual "
        "customer behavior."
    ),
    (
        "session_clusters",
        "Session clusters caveat",
        "Session-level clusters should be interpreted descriptively. Clusters 0 "
        "and 2 had similar average data consumption and differed mainly in "
        "session duration."
    ),
]


# ============================================================================
# HELPERS
# ============================================================================

def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def fmt(value):
    if value is None:
        return ""

    if isinstance(value, float):
        return f"{value:.4f}"

    return value


def write_csv(filename, columns, rows):
    """
    Write rows to powerbi_data/<filename>.
    Returns number of data rows written.
    """

    path = os.path.join(OUT_DIR, filename)

    count = 0

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)

        writer.writerow(columns)

        for row in rows:
            writer.writerow([fmt(value) for value in row])
            count += 1

    return count


def record(manifest, filename, columns, row_count):
    path = os.path.join(OUT_DIR, filename)

    size = os.path.getsize(path)

    manifest.append(
        {
            "file": filename,
            "rows": row_count,
            "columns": columns,
            "bytes": size,
        }
    )

    print(
        f"  wrote {filename:<35} "
        f"rows={row_count:<9,} bytes={size:,}"
    )


def export_small_df(df, filename, manifest):
    """
    Collect only a small aggregated DataFrame.
    Never use this on the 800K-row source.
    """

    columns = df.columns

    rows = [tuple(row) for row in df.collect()]

    count = write_csv(filename, columns, rows)

    record(manifest, filename, columns, count)

    return rows


def dim_summary(df, group_columns, total_sessions, total_mb):
    """
    Create a Power BI summary for a dimension.
    """

    return (
        df.groupBy(*group_columns)
        .agg(
            F.count("*").alias("sessions"),
            F.sum(VALUE).alias("total_data_mb"),
            F.avg(VALUE).alias("avg_data_mb"),
            F.avg(DURATION).alias("avg_duration_min"),
            F.sum("anomaly_flag").alias("anomalies"),
        )
        .withColumn(
            "anomaly_pct",
            F.col("anomalies") / F.col("sessions") * 100.0,
        )
        .withColumn(
            "session_share_pct",
            F.col("sessions") / F.lit(total_sessions) * 100.0,
        )
        .withColumn(
            "data_share_pct",
            F.col("total_data_mb") / F.lit(total_mb) * 100.0,
        )
        .orderBy(*group_columns)
    )


# ============================================================================
# SESSION CLUSTER EXPORT
# ============================================================================

def export_session_clusters(
    spark,
    anomalies,
    total_sessions,
    manifest,
    warnings,
):
    section("SESSION CLUSTER EXPORT — STAGE 13")

    try:
        clusters = spark.read.parquet(SESSION_CLUSTER_PATH)

    except Exception as exc:
        message = (
            "session_clusters could not be read: "
            f"{str(exc)[:200]}"
        )

        warnings.append(message)

        print("WARNING:", message)

        return

    required = {"session_id", "cluster"}

    missing = required - set(clusters.columns)

    if missing:
        message = (
            f"session_clusters is missing columns: {sorted(missing)}"
        )

        warnings.append(message)

        print("WARNING:", message)

        return

    cluster_rows = clusters.count()

    print(f"Stage 13 cluster rows: {cluster_rows:,}")

    joined = (
        anomalies
        .join(
            clusters.select(
                "session_id",
                F.col("cluster")
                .cast("int")
                .alias("session_cluster"),
            ),
            on="session_id",
            how="inner",
        )
    )

    joined_rows = joined.count()

    print(f"Cluster join rows: {joined_rows:,}")

    if joined_rows != total_sessions:

        message = (
            f"Session cluster join matched {joined_rows:,} of "
            f"{total_sessions:,} sessions."
        )

        warnings.append(message)

        print("WARNING:", message)

    summary = (
        joined.groupBy("session_cluster")
        .agg(
            F.count("*").alias("sessions"),
            F.sum(VALUE).alias("total_data_mb"),
            F.avg(VALUE).alias("avg_data_mb"),
            F.avg(DURATION).alias("avg_duration_min"),
            F.sum("anomaly_flag").alias("anomalies"),
        )
        .withColumn(
            "anomaly_pct",
            F.col("anomalies") / F.col("sessions") * 100.0,
        )
        .withColumn(
            "session_share_pct",
            F.col("sessions") / F.lit(joined_rows) * 100.0,
        )
        .orderBy("session_cluster")
    )

    export_small_df(
        summary,
        "session_cluster_summary.csv",
        manifest,
    )

    by_app = (
        joined.groupBy(
            "session_cluster",
            "app_name",
        )
        .agg(
            F.count("*").alias("sessions"),
            F.sum(VALUE).alias("total_data_mb"),
            F.avg(VALUE).alias("avg_data_mb"),
        )
        .orderBy(
            "session_cluster",
            "app_name",
        )
    )

    export_small_df(
        by_app,
        "session_cluster_by_app.csv",
        manifest,
    )


# ============================================================================
# CUSTOMER CLUSTER PROFILE EXPORT
# ============================================================================

def export_customer_profile(
    spark,
    manifest,
    warnings,
):
    section("CUSTOMER CLUSTER PROFILE — STAGE 15")

    try:
        profile = spark.read.parquet(
            CUSTOMER_PROFILE_PATH
        )

    except Exception as exc:
        message = (
            "customer_cluster_profile could not be read: "
            f"{str(exc)[:200]}"
        )

        warnings.append(message)

        print("WARNING:", message)

        return

    row_count = profile.count()

    print(f"Customer profile rows: {row_count:,}")

    if row_count > 1000:

        message = (
            f"Unexpectedly large customer profile: "
            f"{row_count:,} rows."
        )

        warnings.append(message)

        print("WARNING:", message)

        return

    export_small_df(
        profile.orderBy("cluster"),
        "customer_cluster_profile.csv",
        manifest,
    )


# ============================================================================
# MAIN
# ============================================================================

def main():

    os.makedirs(OUT_DIR, exist_ok=True)

    os.makedirs(
        os.path.dirname(REPORT_PATH),
        exist_ok=True,
    )

    spark = get_spark(
        app_name="PowerBIExport",
        hive=False,
    )

    manifest = []
    checks = []
    warnings = []

    def check(name, passed, detail):

        checks.append(
            {
                "check": name,
                "passed": bool(passed),
                "detail": detail,
            }
        )

        status = "PASS" if passed else "FAIL"

        print(
            f"  [{status}] {name}: {detail}"
        )

    # ========================================================================
    # 1. LOAD STAGE 16
    # ========================================================================

    section("1. LOAD STAGE 16 ANOMALY DATA")

    try:
        anomalies = spark.read.parquet(
            ANOMALY_PATH
        )

    except Exception as exc:

        print(
            "STOP: Could not read Stage 16 usage_anomalies."
        )

        print(str(exc))

        spark.stop()

        sys.exit(1)

    required_columns = [
        "app_category",
        "session_id",
        "timestamp",
        "customer_id",
        "operator",
        "app_name",
        "data_consumed_mb",
        "session_duration_minutes",
        "network_type",
        "time_of_day",
        "anomaly_threshold_mb",
        "anomaly_flag",
        "anomaly_reason",
    ]

    missing = [
        column
        for column in required_columns
        if column not in anomalies.columns
    ]

    if missing:

        print(
            "STOP: Stage 16 is missing columns:"
        )

        print(missing)

        spark.stop()

        sys.exit(1)

    anomalies = (
        anomalies
        .withColumn(
            "_ts",
            F.col("timestamp").cast("timestamp"),
        )
        .cache()
    )

    total_sessions = anomalies.count()

    print(
        f"Stage 16 sessions loaded: "
        f"{total_sessions:,}"
    )

    # ========================================================================
    # 2. GLOBAL METRICS
    # ========================================================================

    section("2. CALCULATE GLOBAL KPIs")

    metrics = (
        anomalies
        .agg(
            F.countDistinct("customer_id")
            .alias("unique_customers"),

            F.sum(VALUE)
            .alias("total_data_mb"),

            F.avg(VALUE)
            .alias("avg_data_mb"),

            F.avg(DURATION)
            .alias("avg_duration_min"),

            F.sum("anomaly_flag")
            .alias("total_anomalies"),

            F.min("_ts")
            .alias("first_timestamp"),

            F.max("_ts")
            .alias("last_timestamp"),
        )
        .first()
    )

    unique_customers = int(
        metrics["unique_customers"]
    )

    total_mb = float(
        metrics["total_data_mb"]
    )

    avg_mb = float(
        metrics["avg_data_mb"]
    )

    avg_duration = float(
        metrics["avg_duration_min"]
    )

    total_anomalies = int(
        metrics["total_anomalies"]
    )

    anomaly_pct = (
        total_anomalies
        / total_sessions
        * 100.0
    )

    print(
        f"Total sessions    : {total_sessions:,}"
    )

    print(
        f"Unique customers  : {unique_customers:,}"
    )

    print(
        f"Total data (MB)   : {total_mb:,.2f}"
    )

    print(
        f"Average data (MB) : {avg_mb:.4f}"
    )

    print(
        f"Average duration  : {avg_duration:.4f}"
    )

    print(
        f"Total anomalies    : {total_anomalies:,}"
    )

    print(
        f"Anomaly rate       : {anomaly_pct:.4f}%"
    )

    # ========================================================================
    # 3. KPI TABLE
    # ========================================================================

    section("3. EXPORT KPI TABLE")

    kpi_columns = [
        "total_sessions",
        "unique_customers",
        "total_data_mb",
        "avg_data_mb",
        "avg_duration_min",
        "total_anomalies",
        "anomaly_pct",
        "first_timestamp",
        "last_timestamp",
    ]

    kpi_row = [
        total_sessions,
        unique_customers,
        total_mb,
        avg_mb,
        avg_duration,
        total_anomalies,
        anomaly_pct,
        str(metrics["first_timestamp"]),
        str(metrics["last_timestamp"]),
    ]

    count = write_csv(
        "kpi_summary.csv",
        kpi_columns,
        [kpi_row],
    )

    record(
        manifest,
        "kpi_summary.csv",
        kpi_columns,
        count,
    )

    # ========================================================================
    # 4. DIMENSION TABLES
    # ========================================================================

    section("4. EXPORT DIMENSION TABLES")

    dimension_definitions = [
        (
            "usage_by_app.csv",
            ["app_name", "app_category"],
        ),
        (
            "usage_by_category.csv",
            ["app_category"],
        ),
        (
            "usage_by_operator.csv",
            ["operator"],
        ),
        (
            "usage_by_network.csv",
            ["network_type"],
        ),
        (
            "usage_by_time_of_day.csv",
            ["time_of_day"],
        ),
    ]

    dimension_rows = {}

    for filename, group_columns in dimension_definitions:

        summary = dim_summary(
            anomalies,
            group_columns,
            total_sessions,
            total_mb,
        )

        rows = export_small_df(
            summary,
            filename,
            manifest,
        )

        dimension_rows[filename] = rows

    # ========================================================================
    # 5. DAILY TABLE
    # ========================================================================

    daily = (
        anomalies
        .withColumn(
            "usage_date",
            F.date_format(
                "_ts",
                "yyyy-MM-dd",
            ),
        )
    )

    daily_summary = dim_summary(
        daily,
        ["usage_date"],
        total_sessions,
        total_mb,
    )

    daily_rows = export_small_df(
        daily_summary,
        "usage_daily.csv",
        manifest,
    )

    dimension_rows["usage_daily.csv"] = daily_rows

    # ========================================================================
    # 6. HOURLY TABLE
    # ========================================================================

    hourly = (
        anomalies
        .withColumn(
            "hour_of_day",
            F.hour("_ts"),
        )
    )

    hourly_summary = dim_summary(
        hourly,
        ["hour_of_day"],
        total_sessions,
        total_mb,
    )

    hourly_rows = export_small_df(
        hourly_summary,
        "usage_hourly.csv",
        manifest,
    )

    dimension_rows["usage_hourly.csv"] = hourly_rows

    # ========================================================================
    # 7. ANOMALY THRESHOLDS
    # ========================================================================

    section("5. EXPORT ANOMALY THRESHOLDS")

    threshold_summary = (
        anomalies
        .select(
            "app_category",
            "anomaly_threshold_mb",
        )
        .distinct()
        .orderBy("app_category")
    )

    threshold_rows = export_small_df(
        threshold_summary,
        "anomaly_thresholds.csv",
        manifest,
    )

    category_count = (
        anomalies
        .select("app_category")
        .distinct()
        .count()
    )

    # ========================================================================
    # 8. FLAGGED ANOMALY SESSIONS
    # ========================================================================

    section("6. EXPORT FLAGGED ANOMALY SESSIONS")

    flagged = (
        anomalies
        .filter(
            F.col("anomaly_flag") == 1
        )
        .withColumn(
            "ratio_to_threshold",
            F.col(VALUE)
            / F.col("anomaly_threshold_mb"),
        )
        .select(
            "session_id",
            "timestamp",
            "customer_id",
            "operator",
            "app_name",
            "app_category",
            "data_consumed_mb",
            "session_duration_minutes",
            "network_type",
            "time_of_day",
            "anomaly_threshold_mb",
            "anomaly_flag",
            "anomaly_reason",
            "ratio_to_threshold",
        )
    )

    flagged_columns = flagged.columns

    # Important:
    # toLocalIterator() streams rows instead of collecting
    # all 11,804+ anomaly rows into one Python list.
    flagged_count = write_csv(
        "anomaly_sessions.csv",
        flagged_columns,
        (
            tuple(row)
            for row in flagged.toLocalIterator()
        ),
    )

    record(
        manifest,
        "anomaly_sessions.csv",
        flagged_columns,
        flagged_count,
    )

    # ========================================================================
    # 9. SESSION CLUSTERS
    # ========================================================================

    export_session_clusters(
        spark,
        anomalies,
        total_sessions,
        manifest,
        warnings,
    )

    # ========================================================================
    # 10. CUSTOMER CLUSTERS
    # ========================================================================

    export_customer_profile(
        spark,
        manifest,
        warnings,
    )

    # ========================================================================
    # 11. DATA NOTES
    # ========================================================================

    section("7. EXPORT DATA NOTES")

    notes_count = write_csv(
        "data_notes.csv",
        [
            "note_id",
            "topic",
            "text",
        ],
        DATA_NOTES,
    )

    record(
        manifest,
        "data_notes.csv",
        [
            "note_id",
            "topic",
            "text",
        ],
        notes_count,
    )

    # ========================================================================
    # 12. RECONCILIATION
    # ========================================================================

    section("8. RECONCILIATION CHECKS")

    for filename, group_columns in dimension_definitions:

        rows = dimension_rows[filename]

        # group columns come first
        # sessions immediately follows them
        sessions_index = len(group_columns)

        # Structure:
        # group columns
        # sessions
        # total_data_mb
        # avg_data_mb
        # avg_duration_min
        # anomalies
        # anomaly_pct
        # session_share_pct
        # data_share_pct

        anomaly_index = len(group_columns) + 4

        session_sum = sum(
            int(row[sessions_index])
            for row in rows
        )

        anomaly_sum = sum(
            int(row[anomaly_index])
            for row in rows
        )

        check(
            f"{filename} session reconciliation",
            session_sum == total_sessions,
            f"{session_sum:,} vs {total_sessions:,}",
        )

        check(
            f"{filename} anomaly reconciliation",
            anomaly_sum == total_anomalies,
            f"{anomaly_sum:,} vs {total_anomalies:,}",
        )

    # Daily
    daily_session_sum = sum(
        int(row[1])
        for row in daily_rows
    )

    daily_anomaly_sum = sum(
        int(row[5])
        for row in daily_rows
    )

    check(
        "usage_daily session reconciliation",
        daily_session_sum == total_sessions,
        f"{daily_session_sum:,} vs {total_sessions:,}",
    )

    check(
        "usage_daily anomaly reconciliation",
        daily_anomaly_sum == total_anomalies,
        f"{daily_anomaly_sum:,} vs {total_anomalies:,}",
    )

    # Hourly
    hourly_session_sum = sum(
        int(row[1])
        for row in hourly_rows
    )

    hourly_anomaly_sum = sum(
        int(row[5])
        for row in hourly_rows
    )

    check(
        "usage_hourly session reconciliation",
        hourly_session_sum == total_sessions,
        f"{hourly_session_sum:,} vs {total_sessions:,}",
    )

    check(
        "usage_hourly anomaly reconciliation",
        hourly_anomaly_sum == total_anomalies,
        f"{hourly_anomaly_sum:,} vs {total_anomalies:,}",
    )

    # Flagged anomaly file
    check(
        "anomaly_sessions row reconciliation",
        flagged_count == total_anomalies,
        f"{flagged_count:,} vs {total_anomalies:,}",
    )

    # Threshold count
    check(
        "one threshold per category",
        len(threshold_rows) == category_count,
        f"{len(threshold_rows)} vs {category_count}",
    )

    # Binary anomaly flag
    invalid_flags = (
        anomalies
        .filter(
            ~F.col("anomaly_flag").isin(0, 1)
        )
        .count()
    )

    check(
        "anomaly_flag contains only 0/1",
        invalid_flags == 0,
        f"{invalid_flags} invalid values",
    )

    # Timestamp
    invalid_timestamps = (
        anomalies
        .filter(
            F.col("_ts").isNull()
        )
        .count()
    )

    check(
        "all timestamps parsed",
        invalid_timestamps == 0,
        f"{invalid_timestamps} invalid timestamps",
    )

    # Thresholds
    null_thresholds = (
        anomalies
        .filter(
            F.col("anomaly_threshold_mb").isNull()
        )
        .count()
    )

    check(
        "no null anomaly thresholds",
        null_thresholds == 0,
        f"{null_thresholds} null thresholds",
    )

    # ========================================================================
    # 13. REPORT
    # ========================================================================

    report = {
        "stage": "17",
        "title": "Power BI Data Preparation and Export",
        "generated_at": datetime.now().isoformat(
            timespec="seconds"
        ),
        "source": ANOMALY_PATH,
        "output_directory": OUT_DIR,
        "total_sessions": total_sessions,
        "unique_customers": unique_customers,
        "total_anomalies": total_anomalies,
        "anomaly_rate_pct": anomaly_pct,
        "files": manifest,
        "checks": checks,
        "warnings": warnings,
        "caveats": [
            note[2]
            for note in DATA_NOTES
        ],
    }

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as fh:

        json.dump(
            report,
            fh,
            indent=2,
            default=str,
        )

    print(
        f"\nReport written: {REPORT_PATH}"
    )

    # ========================================================================
    # 14. FINAL RESULT
    # ========================================================================

    failed = [
        check
        for check in checks
        if not check["passed"]
    ]

    section("STAGE 17 SUMMARY")

    print(
        f"Files written : {len(manifest)}"
    )

    print(
        f"Checks passed : "
        f"{len(checks) - len(failed)}/{len(checks)}"
    )

    print(
        f"Warnings      : {len(warnings)}"
    )

    for warning in warnings:
        print(
            "WARNING:",
            warning,
        )

    anomalies.unpersist()

    spark.stop()

    if failed:

        print(
            "\nSTAGE 17 NOT COMPLETE."
        )

        print(
            "Fix the failed checks before continuing."
        )

        sys.exit(1)

    print(
        "\nSTAGE 17 EXPORT COMPLETE."
    )


if __name__ == "__main__":
    main()
