"""
Stage 16 — Internet Usage Anomaly Detection

Purpose:
    Detect unusually high data-consumption sessions using
    category-specific IQR thresholds.

Important:
    These are usage anomalies, NOT fraud labels.
    The dataset has no ground-truth anomaly labels.
"""

import json
from pathlib import Path

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = (
    "hdfs://localhost:9000/"
    "internet_usage/cleaned/usage_cleaned"
)

OUTPUT_PATH = (
    "hdfs://localhost:9000/"
    "internet_usage/results/usage_anomalies"
)

DOCS_DIR = Path("docs")
REPORT_PATH = DOCS_DIR / "stage16_anomaly_report.json"

HIVE_TABLE = "internet_usage.usage_anomalies"


# ============================================================
# CONFIGURATION
# ============================================================

IQR_MULTIPLIER = 1.5
PERCENTILE_ACCURACY = 100000

EXPECTED_ROWS = 800000


# ============================================================
# MAIN
# ============================================================

def main():

    spark = get_spark(
        app_name="Stage16AnomalyDetection",
        hive=True,
    )

    print("\n" + "=" * 75)
    print("STAGE 16 — INTERNET USAGE ANOMALY DETECTION")
    print("=" * 75)

    # --------------------------------------------------------
    # 1. READ CLEANED DATA
    # --------------------------------------------------------

    print("\n[1/8] Reading cleaned HDFS dataset...")

    df = spark.read.parquet(INPUT_PATH)

    input_count = df.count()

    print(f"Input rows: {input_count:,}")

    if input_count != EXPECTED_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_ROWS:,} rows, "
            f"but found {input_count:,}."
        )

    # --------------------------------------------------------
    # 2. CALCULATE CATEGORY-SPECIFIC THRESHOLDS
    # --------------------------------------------------------

    print("\n[2/8] Calculating category-specific IQR thresholds...")

    thresholds = (
        df.groupBy("app_category")
        .agg(
            F.count("*").alias("sessions"),

            F.percentile_approx(
                "data_consumed_mb",
                0.25,
                PERCENTILE_ACCURACY,
            ).alias("q1_mb"),

            F.percentile_approx(
                "data_consumed_mb",
                0.50,
                PERCENTILE_ACCURACY,
            ).alias("median_mb"),

            F.avg(
                "data_consumed_mb"
            ).alias("mean_mb"),

            F.percentile_approx(
                "data_consumed_mb",
                0.75,
                PERCENTILE_ACCURACY,
            ).alias("q3_mb"),

            F.percentile_approx(
                "data_consumed_mb",
                0.99,
                PERCENTILE_ACCURACY,
            ).alias("p99_mb"),

            F.max(
                "data_consumed_mb"
            ).alias("max_mb"),
        )
        .withColumn(
            "iqr_mb",
            F.col("q3_mb") - F.col("q1_mb"),
        )
        .withColumn(
            "upper_bound_mb",
            F.col("q3_mb")
            + F.lit(IQR_MULTIPLIER) * F.col("iqr_mb"),
        )
        .orderBy("app_category")
    )

    print("\nCategory thresholds:")
    thresholds.select(
        "app_category",
        "sessions",
        F.round("q1_mb", 2).alias("q1_mb"),
        F.round("median_mb", 2).alias("median_mb"),
        F.round("mean_mb", 2).alias("mean_mb"),
        F.round("q3_mb", 2).alias("q3_mb"),
        F.round("iqr_mb", 2).alias("iqr_mb"),
        F.round("upper_bound_mb", 2).alias(
            "upper_bound_mb"
        ),
        F.round("p99_mb", 2).alias("p99_mb"),
        F.round("max_mb", 2).alias("max_mb"),
    ).show(truncate=False)

    # --------------------------------------------------------
    # 3. JOIN THRESHOLDS TO EVERY SESSION
    # --------------------------------------------------------

    print("\n[3/8] Applying thresholds to sessions...")

    threshold_lookup = thresholds.select(
        "app_category",
        F.col("upper_bound_mb").alias(
            "anomaly_threshold_mb"
        ),
    )

    flagged = (
        df.join(
            threshold_lookup,
            on="app_category",
            how="left",
        )
        .withColumn(
            "anomaly_flag",
            (
                F.col("data_consumed_mb")
                > F.col("anomaly_threshold_mb")
            ).cast("integer"),
        )
        .withColumn(
            "anomaly_reason",
            F.when(
                F.col("anomaly_flag") == 1,
                F.lit(
                    "Data consumption exceeds "
                    "category-specific IQR upper bound"
                ),
            ).otherwise(F.lit(None)),
        )
    )

    # --------------------------------------------------------
    # 4. VALIDATE ROW COUNT
    # --------------------------------------------------------

    print("\n[4/8] Validating output row count...")

    output_count = flagged.count()

    print(f"Output rows: {output_count:,}")

    if output_count != input_count:
        raise RuntimeError(
            "Row count changed during anomaly detection."
        )

    # --------------------------------------------------------
    # 5. ANOMALY SUMMARY
    # --------------------------------------------------------

    print("\n[5/8] Calculating anomaly summary...")

    summary = (
        flagged.groupBy("app_category")
        .agg(
            F.count("*").alias("sessions"),
            F.sum("anomaly_flag").alias(
                "anomaly_sessions"
            ),
            F.avg("anomaly_flag").alias(
                "anomaly_rate"
            ),
        )
        .withColumn(
            "anomaly_rate_percent",
            F.col("anomaly_rate") * 100,
        )
        .orderBy("app_category")
    )

    print("\nAnomaly summary:")
    summary.select(
        "app_category",
        "sessions",
        "anomaly_sessions",
        F.round(
            "anomaly_rate_percent",
            4,
        ).alias("anomaly_rate_percent"),
    ).show(truncate=False)

    # --------------------------------------------------------
    # 6. GLOBAL ANOMALY SUMMARY
    # --------------------------------------------------------

    global_summary = (
        flagged.agg(
            F.count("*").alias("total_sessions"),
            F.sum("anomaly_flag").alias(
                "total_anomalies"
            ),
            F.avg("anomaly_flag").alias(
                "overall_anomaly_rate"
            ),
        )
        .collect()[0]
    )

    total_anomalies = int(
        global_summary["total_anomalies"]
    )

    overall_rate = float(
        global_summary["overall_anomaly_rate"]
    )

    print(
        f"\nTotal anomalies: {total_anomalies:,}"
    )

    print(
        f"Overall anomaly rate: "
        f"{overall_rate * 100:.4f}%"
    )

    # --------------------------------------------------------
    # 7. WRITE RESULTS TO HDFS
    # --------------------------------------------------------

    print("\n[6/8] Writing anomaly results to HDFS...")

    (
        flagged
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(f"Saved: {OUTPUT_PATH}")

    # --------------------------------------------------------
    # 8. HIVE + REPORT
    # --------------------------------------------------------

    print("\n[7/8] Registering Hive table...")

    (
        flagged.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(HIVE_TABLE)
    )

    print(f"Hive table: {HIVE_TABLE}")

    # --------------------------------------------------------
    # FINAL VALIDATION
    # --------------------------------------------------------

    print("\n[8/8] Running final validations...")

    anomaly_values = {
        row["anomaly_flag"]
        for row in (
            flagged
            .select("anomaly_flag")
            .distinct()
            .collect()
        )
    }

    if not anomaly_values.issubset({0, 1}):
        raise RuntimeError(
            f"Invalid anomaly_flag values: "
            f"{anomaly_values}"
        )

    threshold_nulls = flagged.filter(
        F.col("anomaly_threshold_mb").isNull()
    ).count()

    if threshold_nulls != 0:
        raise RuntimeError(
            "Some sessions do not have an anomaly threshold."
        )

    negative_values = flagged.filter(
        F.col("data_consumed_mb") < 0
    ).count()

    if negative_values != 0:
        raise RuntimeError(
            "Negative data-consumption values detected."
        )

    # Verify anomaly logic
    logic_errors = flagged.filter(
        (
            (F.col("anomaly_flag") == 1)
            & ~(
                F.col("data_consumed_mb")
                > F.col("anomaly_threshold_mb")
            )
        )
        |
        (
            (F.col("anomaly_flag") == 0)
            & (
                F.col("data_consumed_mb")
                > F.col("anomaly_threshold_mb")
            )
        )
    ).count()

    if logic_errors != 0:
        raise RuntimeError(
            f"Found {logic_errors} anomaly logic errors."
        )

    # --------------------------------------------------------
    # REPORT
    # --------------------------------------------------------

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    threshold_rows = [
        {
            "app_category": row["app_category"],
            "sessions": int(row["sessions"]),
            "q1_mb": float(row["q1_mb"]),
            "median_mb": float(row["median_mb"]),
            "mean_mb": float(row["mean_mb"]),
            "q3_mb": float(row["q3_mb"]),
            "iqr_mb": float(row["iqr_mb"]),
            "upper_bound_mb": float(
                row["upper_bound_mb"]
            ),
            "p99_mb": float(row["p99_mb"]),
            "max_mb": float(row["max_mb"]),
        }
        for row in thresholds.collect()
    ]

    summary_rows = [
        {
            "app_category": row["app_category"],
            "sessions": int(row["sessions"]),
            "anomaly_sessions": int(
                row["anomaly_sessions"]
            ),
            "anomaly_rate_percent": float(
                row["anomaly_rate_percent"]
            ),
        }
        for row in summary.collect()
    ]

    report = {
        "stage": "16",
        "name": "Internet Usage Anomaly Detection",
        "method": "Category-specific IQR",
        "input_path": INPUT_PATH,
        "output_path": OUTPUT_PATH,
        "hive_table": HIVE_TABLE,
        "input_rows": input_count,
        "output_rows": output_count,
        "iqr_multiplier": IQR_MULTIPLIER,
        "percentile_accuracy": PERCENTILE_ACCURACY,
        "thresholds": threshold_rows,
        "anomaly_summary": summary_rows,
        "total_anomalies": total_anomalies,
        "overall_anomaly_rate_percent": overall_rate * 100,
        "ground_truth_available": False,
        "validation": {
            "row_count_preserved": (
                output_count == input_count
            ),
            "threshold_nulls_zero": (
                threshold_nulls == 0
            ),
            "negative_values_zero": (
                negative_values == 0
            ),
            "anomaly_logic_valid": (
                logic_errors == 0
            ),
            "anomaly_flag_binary": (
                anomaly_values.issubset({0, 1})
            ),
        },
        "interpretation_note": (
            "Anomalies represent unusually high "
            "internet usage within an application "
            "category. They are not fraud labels."
        ),
    }

    REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
        )
    )

    print(f"\nReport saved: {REPORT_PATH}")

    print("\n" + "=" * 75)
    print("STAGE 16 COMPLETE")
    print("=" * 75)

    print(
        f"Total sessions : {input_count:,}"
    )

    print(
        f"Anomalies       : {total_anomalies:,}"
    )

    print(
        f"Anomaly rate    : "
        f"{overall_rate * 100:.4f}%"
    )

    print(
        f"HDFS output     : {OUTPUT_PATH}"
    )

    print(
        f"Hive table      : {HIVE_TABLE}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
