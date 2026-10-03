"""Stage 11: build, validate, and persist session-level time features."""

from pathlib import Path
import sys

from pyspark.sql import functions as F

# Allow imports from the project root when running this file directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.features.time_features import TIME_FEATURES, add_time_features
from src.utils.spark_session import get_spark


INPUT_TABLE = "internet_usage.cleaned_usage"
OUTPUT_PATH = "hdfs://localhost:9000/internet_usage/features/session_features"
OUTPUT_TABLE = "internet_usage.session_features"

ORIGINAL_COLUMNS = [
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
]


def main():
    spark = get_spark("Stage11_TimeFeatures", hive=True)

    try:
        print("=== STAGE 11: TIME FEATURE ENGINEERING ===")

        # ------------------------------------------------------------
        # 1. Read the persistent Hive table from Stage 10
        # ------------------------------------------------------------
        source = spark.table(INPUT_TABLE)

        source_count = source.count()
        source_distinct_sessions = source.select("session_id").distinct().count()

        print(f"Input table: {INPUT_TABLE}")
        print(f"Input rows: {source_count}")
        print(f"Input distinct session_id: {source_distinct_sessions}")

        # ------------------------------------------------------------
        # 2. Add the seven time features
        # ------------------------------------------------------------
        featured = add_time_features(source)

        expected_columns = ORIGINAL_COLUMNS + TIME_FEATURES

        if featured.columns != expected_columns:
            raise AssertionError(
                f"Unexpected column order/schema.\n"
                f"Expected: {expected_columns}\n"
                f"Actual:   {featured.columns}"
            )

        # ------------------------------------------------------------
        # 3. Row-count and session-id validation
        # ------------------------------------------------------------
        output_count = featured.count()
        output_distinct_sessions = (
            featured.select("session_id").distinct().count()
        )

        if output_count != source_count:
            raise AssertionError(
                f"Row count changed: {source_count} -> {output_count}"
            )

        if output_distinct_sessions != source_distinct_sessions:
            raise AssertionError(
                "Distinct session_id count changed: "
                f"{source_distinct_sessions} -> {output_distinct_sessions}"
            )

        # ------------------------------------------------------------
        # 4. Null validation for new columns
        # ------------------------------------------------------------
        null_exprs = [
            F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
            for c in TIME_FEATURES
        ]

        null_row = featured.agg(*null_exprs).first().asDict()

        if any(null_row[f"null_{c}"] != 0 for c in TIME_FEATURES):
            raise AssertionError(f"Nulls found in time features: {null_row}")

        # ------------------------------------------------------------
        # 5. Range validation
        # ------------------------------------------------------------
        range_row = featured.agg(
            F.min("hour").alias("min_hour"),
            F.max("hour").alias("max_hour"),
            F.min("day_of_week").alias("min_day_of_week"),
            F.max("day_of_week").alias("max_day_of_week"),
            F.min("day_of_month").alias("min_day_of_month"),
            F.max("day_of_month").alias("max_day_of_month"),
            F.min("month").alias("min_month"),
            F.max("month").alias("max_month"),
            F.min("is_weekend").alias("min_is_weekend"),
            F.max("is_weekend").alias("max_is_weekend"),
            F.min("is_night").alias("min_is_night"),
            F.max("is_night").alias("max_is_night"),
        ).first().asDict()

        ranges_ok = (
            range_row["min_hour"] == 0
            and range_row["max_hour"] == 23
            and range_row["min_day_of_week"] == 1
            and range_row["max_day_of_week"] == 7
            and 1 <= range_row["min_day_of_month"] <= 31
            and 1 <= range_row["max_day_of_month"] <= 31
            and 1 <= range_row["min_month"] <= 12
            and 1 <= range_row["max_month"] <= 12
            and range_row["min_is_weekend"] in (0, 1)
            and range_row["max_is_weekend"] in (0, 1)
            and range_row["min_is_night"] in (0, 1)
            and range_row["max_is_night"] in (0, 1)
        )

        if not ranges_ok:
            raise AssertionError(f"Invalid feature ranges: {range_row}")

        # ------------------------------------------------------------
        # 6. Validate is_night against the dataset's existing label
        # ------------------------------------------------------------
        expected_night = (
            (F.col("time_of_day") == "night").cast("int")
        )

        night_mismatches = featured.filter(
            F.col("is_night") != expected_night
        ).count()

        if night_mismatches != 0:
            raise AssertionError(
                f"is_night mismatches: {night_mismatches}"
            )

        # ------------------------------------------------------------
        # 7. Independent date-feature validation
        # ------------------------------------------------------------
        expected_dates = (
            source.select(F.to_date("timestamp").alias("usage_date"))
            .distinct()
        )

        actual_dates = featured.select("usage_date").distinct()

        missing_dates = expected_dates.subtract(actual_dates).count()
        extra_dates = actual_dates.subtract(expected_dates).count()

        if missing_dates != 0 or extra_dates != 0:
            raise AssertionError(
                f"Date mismatch: missing={missing_dates}, extra={extra_dates}"
            )

        # ------------------------------------------------------------
        # 8. Distribution evidence
        # ------------------------------------------------------------
        print("\n--- TIME FEATURE DISTRIBUTIONS ---")

        for column in [
            "hour",
            "day_of_week",
            "day_of_month",
            "month",
            "is_weekend",
            "is_night",
        ]:
            print(f"\n{column}:")
            (
                featured.groupBy(column)
                .count()
                .orderBy(column)
                .show(40, truncate=False)
            )

        print("\nusage_date:")
        (
            featured.groupBy("usage_date")
            .count()
            .orderBy("usage_date")
            .show(40, truncate=False)
        )

        # ------------------------------------------------------------
        # 9. Persist as Parquet
        # ------------------------------------------------------------
        (
            featured.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(OUTPUT_PATH)
        )

        # ------------------------------------------------------------
        # 10. Register persistent Hive table
        # ------------------------------------------------------------
        spark.sql(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")

        spark.sql(
            f"""
            CREATE TABLE {OUTPUT_TABLE}
            USING PARQUET
            LOCATION '{OUTPUT_PATH}'
            """
        )

        # ------------------------------------------------------------
        # 11. Re-read the persisted table
        # ------------------------------------------------------------
        persisted = spark.table(OUTPUT_TABLE)

        persisted_count = persisted.count()
        persisted_distinct_sessions = (
            persisted.select("session_id").distinct().count()
        )

        if persisted_count != source_count:
            raise AssertionError(
                f"Persisted row count mismatch: "
                f"{source_count} -> {persisted_count}"
            )

        if persisted_distinct_sessions != source_distinct_sessions:
            raise AssertionError(
                "Persisted distinct session_id mismatch: "
                f"{source_distinct_sessions} -> "
                f"{persisted_distinct_sessions}"
            )

        # ------------------------------------------------------------
        # 12. Final summary
        # ------------------------------------------------------------
        print("\n=== VALIDATION SUMMARY ===")
        print(f"Rows preserved: {source_count == output_count}")
        print(
            "Distinct session_id preserved: "
            f"{source_distinct_sessions == output_distinct_sessions}"
        )
        print("New-column null check: PASSED")
        print("Feature range check: PASSED")
        print(f"is_night mismatches: {night_mismatches}")
        print(
            "Date feature validation: "
            f"PASSED (missing={missing_dates}, extra={extra_dates})"
        )
        print(f"Persisted rows: {persisted_count}")
        print(f"Output path: {OUTPUT_PATH}")
        print(f"Hive table: {OUTPUT_TABLE}")
        print("STAGE 11 CHECKS: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
