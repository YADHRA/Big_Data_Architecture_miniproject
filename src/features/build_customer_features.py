"""Stage 14: build and validate repeat-customer features."""

from __future__ import annotations

from pathlib import Path

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as F

from src.features.customer_features import (
    FEATURE_COLS,
    MIN_SESSIONS,
    build_customer_features,
    repeat_customer_sessions,
)
from src.utils.spark_session import get_spark


# ----------------------------------------------------------------------
# Expected values established during the Stage 3 dataset audit.
# These act as regression checks so later stages cannot silently
# operate on a different population.
# ----------------------------------------------------------------------

EXPECTED_TOTAL_CUSTOMERS = 765245
EXPECTED_SINGLE_SESSION_CUSTOMERS = 731479
EXPECTED_REPEAT_CUSTOMERS = 33766
EXPECTED_REPEAT_SESSIONS = 68521

EXPECTED_SESSION_DISTRIBUTION = {
    2: 32802,
    3: 940,
    4: 23,
    5: 1,
}


# ----------------------------------------------------------------------
# Stage 14 outputs
# ----------------------------------------------------------------------

HDFS_OUTPUT = (
    "hdfs://localhost:9000/internet_usage/features/customer_features"
)

HIVE_TABLE = "internet_usage.customer_features"


# ----------------------------------------------------------------------
# Stage 13 gate
# ----------------------------------------------------------------------

GATE_FILE = Path("docs/stage13_final_report.txt")


# ----------------------------------------------------------------------
# Numerical comparison tolerance
# ----------------------------------------------------------------------

TOL = 1e-6


def main() -> None:
    """Build, validate, persist, and audit repeat-customer features."""

    # ==================================================================
    # STAGE 13 GATE
    # ==================================================================

    if not GATE_FILE.exists():
        raise RuntimeError(
            f"Missing Stage 13 gate file: {GATE_FILE}"
        )

    gate_text = GATE_FILE.read_text()

    if "STAGE 13 FINAL CHECKS: PASSED" not in gate_text:
        raise RuntimeError(
            "Stage 13 gate failed. Expected "
            "'STAGE 13 FINAL CHECKS: PASSED'."
        )

    # ==================================================================
    # START SPARK
    # ==================================================================

    spark = get_spark(
        "Stage14_CustomerFeatures",
        hive=True,
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=" * 72)
        print("STAGE 14 — REPEAT-CUSTOMER FEATURES")
        print("=" * 72)

        # ==============================================================
        # 1. LOAD SESSION FEATURES
        # ==============================================================

        print("\n1. LOAD SESSION FEATURES")

        sessions = spark.table(
            "internet_usage.session_features"
        )

        session_count = sessions.count()

        customer_count = (
            sessions
            .select("customer_id")
            .distinct()
            .count()
        )

        print(
            f"Session rows: {session_count:,}"
        )

        print(
            f"Distinct customers: {customer_count:,}"
        )

        assert session_count == 800000

        assert (
            customer_count
            == EXPECTED_TOTAL_CUSTOMERS
        )

        print("Population check: PASSED")

        # ==============================================================
        # 2. CUSTOMER SESSION DISTRIBUTION
        # ==============================================================

        print(
            "\n2. CUSTOMER SESSION DISTRIBUTION"
        )

        customer_counts = (
            sessions
            .groupBy("customer_id")
            .count()
        )

        # IMPORTANT:
        # Explicitly name the second aggregation column.
        # This avoids Spark-version-dependent names such as
        # "count(1)".
        dist_rows = (
            customer_counts
            .groupBy("count")
            .agg(
                F.count("*").alias(
                    "customer_count"
                )
            )
            .orderBy("count")
            .collect()
        )

        observed_dist = {
            int(row["count"]): int(
                row["customer_count"]
            )
            for row in dist_rows
        }

        print("Observed:")

        for count in sorted(observed_dist):
            print(
                f"  {count} session(s): "
                f"{observed_dist[count]:,} customers"
            )

        # One-session population check.
        assert (
            observed_dist.get(1, 0)
            == EXPECTED_SINGLE_SESSION_CUSTOMERS
        )

        # Repeat-session distribution checks.
        for (
            session_count_value,
            expected,
        ) in EXPECTED_SESSION_DISTRIBUTION.items():

            assert (
                observed_dist.get(
                    session_count_value,
                    0,
                )
                == expected
            )

        # Count repeat customers.
        repeat_customer_count = (
            customer_counts
            .filter(
                F.col("count")
                >= MIN_SESSIONS
            )
            .count()
        )

        assert (
            repeat_customer_count
            == EXPECTED_REPEAT_CUSTOMERS
        )

        # Count all sessions belonging to repeat customers.
        repeat_session_count = (
            customer_counts
            .filter(
                F.col("count")
                >= MIN_SESSIONS
            )
            .agg(
                F.sum("count").alias("n")
            )
            .first()["n"]
        )

        assert (
            repeat_session_count
            == EXPECTED_REPEAT_SESSIONS
        )

        print(
            f"Repeat customers: "
            f"{repeat_customer_count:,}"
        )

        print(
            f"Repeat sessions: "
            f"{repeat_session_count:,}"
        )

        print(
            "Population distribution: PASSED"
        )

        # ==============================================================
        # 3. BUILD CUSTOMER FEATURES
        # ==============================================================

        print(
            "\n3. BUILD CUSTOMER FEATURES"
        )

        feat = build_customer_features(
            sessions
        )

        rows = feat.count()

        distinct_ids = (
            feat
            .select("customer_id")
            .distinct()
            .count()
        )

        print(
            f"Feature rows: {rows:,}"
        )

        print(
            f"Distinct customer IDs: "
            f"{distinct_ids:,}"
        )

        assert (
            rows
            == EXPECTED_REPEAT_CUSTOMERS
        )

        assert (
            distinct_ids
            == EXPECTED_REPEAT_CUSTOMERS
        )

        print(
            "One row per repeat customer: PASSED"
        )

        # ==============================================================
        # 4. NULL VALIDATION
        # ==============================================================

        print("\n4. NULL VALIDATION")

        null_exprs = [
            F.sum(
                F.col(c)
                .isNull()
                .cast("int")
            ).alias(c)
            for c in (
                ["customer_id"]
                + FEATURE_COLS
            )
        ]

        null_row = (
            feat
            .select(*null_exprs)
            .first()
        )

        null_counts = {
            c: int(null_row[c])
            for c in (
                ["customer_id"]
                + FEATURE_COLS
            )
        }

        print(null_counts)

        assert (
            sum(null_counts.values())
            == 0
        )

        print("Null check: PASSED")

        # ==============================================================
        # 5. REPEAT-SESSION CONSISTENCY
        # ==============================================================

        print(
            "\n5. REPEAT-SESSION CONSISTENCY"
        )

        repeat_sessions = (
            repeat_customer_sessions(
                sessions
            )
        )

        repeat_session_count_2 = (
            repeat_sessions.count()
        )

        print(
            f"Semi-join repeat sessions: "
            f"{repeat_session_count_2:,}"
        )

        assert (
            repeat_session_count_2
            == EXPECTED_REPEAT_SESSIONS
        )

        feature_session_sum = (
            feat
            .agg(
                F.sum(
                    "total_sessions"
                ).alias("n")
            )
            .first()["n"]
        )

        print(
            f"Sum(total_sessions): "
            f"{int(feature_session_sum):,}"
        )

        assert (
            int(feature_session_sum)
            == EXPECTED_REPEAT_SESSIONS
        )

        print(
            "Session conservation: PASSED"
        )

        # ==============================================================
        # 6. FEATURE INVARIANTS
        # ==============================================================

        print(
            "\n6. FEATURE INVARIANTS"
        )

        invariant_row = (
            feat
            .select(
                F.min(
                    "total_sessions"
                ).alias(
                    "min_sessions"
                ),
                F.max(
                    "total_sessions"
                ).alias(
                    "max_sessions"
                ),
                F.min(
                    "total_data_mb"
                ).alias(
                    "min_total_data"
                ),
                F.min(
                    "avg_data_mb"
                ).alias(
                    "min_avg_data"
                ),
                F.min(
                    "max_session_data_mb"
                ).alias(
                    "min_max_data"
                ),
                F.min(
                    "unique_app_count"
                ).alias(
                    "min_apps"
                ),
                F.min(
                    "unique_network_count"
                ).alias(
                    "min_networks"
                ),
                F.min(
                    "weekend_sessions"
                ).alias(
                    "min_weekend"
                ),
                F.min(
                    "night_sessions"
                ).alias(
                    "min_night"
                ),
            )
            .first()
        )

        # Every feature row represents a repeat customer.
        assert (
            invariant_row["min_sessions"]
            >= 2
        )

        assert (
            invariant_row["min_total_data"]
            > 0
        )

        assert (
            invariant_row["min_avg_data"]
            > 0
        )

        assert (
            invariant_row["min_max_data"]
            > 0
        )

        assert (
            invariant_row["min_apps"]
            >= 1
        )

        assert (
            invariant_row["min_networks"]
            >= 1
        )

        assert (
            invariant_row["min_weekend"]
            >= 0
        )

        assert (
            invariant_row["min_night"]
            >= 0
        )

        print(
            "Feature invariants: PASSED"
        )

        # ==============================================================
        # 7. WRITE HDFS + HIVE
        # ==============================================================

        print(
            "\n7. WRITE HDFS + HIVE"
        )

        spark.sql(
            "CREATE DATABASE IF NOT EXISTS "
            "internet_usage"
        )

        # Remove an old Stage 14 table definition if one exists.
        spark.sql(
            f"DROP TABLE IF EXISTS "
            f"{HIVE_TABLE}"
        )

        # Write one Parquet part file for easy inspection.
        (
            feat
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                HDFS_OUTPUT
            )
        )

        # Register the Parquet output as a persistent Hive table.
        spark.sql(
            f"""
            CREATE TABLE {HIVE_TABLE}
            USING PARQUET
            LOCATION '{HDFS_OUTPUT}'
            """
        )

        back = spark.table(
            HIVE_TABLE
        )

        back_rows = back.count()

        back_ids = (
            back
            .select("customer_id")
            .distinct()
            .count()
        )

        print(
            f"Hive rows: {back_rows:,}"
        )

        print(
            f"Hive distinct IDs: "
            f"{back_ids:,}"
        )

        assert (
            back_rows
            == EXPECTED_REPEAT_CUSTOMERS
        )

        assert (
            back_ids
            == EXPECTED_REPEAT_CUSTOMERS
        )

        print(
            "Hive persistence: PASSED"
        )

        # ==============================================================
        # 8. PYTHON RECOMPUTATION CHECK
        # ==============================================================

        print(
            "\n8. RECOMPUTATION CHECK"
        )

        # Deterministic sample of 500 repeat customers.
        sample_ids = [
            row["customer_id"]
            for row in (
                feat
                .select("customer_id")
                .orderBy(
                    F.xxhash64(
                        "customer_id"
                    )
                )
                .limit(500)
                .collect()
            )
        ]

        # Retrieve all original sessions belonging to
        # the sampled customers.
        sample_sessions = (
            sessions
            .filter(
                F.col("customer_id").isin(
                    sample_ids
                )
            )
            .collect()
        )

        python_data = {}

        for row in sample_sessions:
            cid = row["customer_id"]

            if cid not in python_data:
                python_data[cid] = {
                    "total_sessions": 0,
                    "total_data_mb": 0.0,
                    "durations": [],
                    "data_values": [],
                    "apps": set(),
                    "networks": set(),
                    "weekend_sessions": 0,
                    "night_sessions": 0,
                }

            d = python_data[cid]

            d["total_sessions"] += 1

            d["total_data_mb"] += float(
                row["data_consumed_mb"]
            )

            d["durations"].append(
                float(
                    row[
                        "session_duration_minutes"
                    ]
                )
            )

            d["data_values"].append(
                float(
                    row[
                        "data_consumed_mb"
                    ]
                )
            )

            d["apps"].add(
                row["app_name"]
            )

            d["networks"].add(
                row["network_type"]
            )

            d["weekend_sessions"] += int(
                row["is_weekend"]
            )

            d["night_sessions"] += int(
                row["is_night"]
            )

        stored = {
            row["customer_id"]: row
            for row in (
                feat
                .filter(
                    F.col(
                        "customer_id"
                    ).isin(sample_ids)
                )
                .collect()
            )
        }

        max_diff = 0.0

        for cid, d in python_data.items():

            expected = {
                "total_sessions":
                    d["total_sessions"],

                "total_data_mb":
                    d["total_data_mb"],

                "avg_data_mb":
                    (
                        d["total_data_mb"]
                        / d["total_sessions"]
                    ),

                "avg_session_duration_min":
                    (
                        sum(d["durations"])
                        / d["total_sessions"]
                    ),

                "max_session_data_mb":
                    max(d["data_values"]),

                "unique_app_count":
                    len(d["apps"]),

                "unique_network_count":
                    len(d["networks"]),

                "weekend_sessions":
                    d["weekend_sessions"],

                "night_sessions":
                    d["night_sessions"],
            }

            actual = stored[cid]

            for c in FEATURE_COLS:

                diff = abs(
                    float(actual[c])
                    - float(expected[c])
                )

                max_diff = max(
                    max_diff,
                    diff,
                )

                assert diff <= TOL, (
                    cid,
                    c,
                    actual[c],
                    expected[c],
                    diff,
                )

        print(
            f"Checked customers: "
            f"{len(python_data):,}"
        )

        print(
            "Maximum recomputation difference: "
            f"{max_diff:.12g}"
        )

        print(
            "Python recomputation: PASSED"
        )

        # ==============================================================
        # 9. FEATURE SUMMARY
        # ==============================================================

        print(
            "\n9. FEATURE SUMMARY"
        )

        summary = (
            back
            .select(FEATURE_COLS)
            .summary(
                "mean",
                "stddev",
                "min",
                "50%",
                "max",
            )
            .collect()
        )

        print(
            "Feature                       "
            "mean | stddev | min | "
            "median | max"
        )

        for c in FEATURE_COLS:

            values = []

            for row in summary:
                values.append(
                    f"{float(row[c]):.3f}"
                )

            print(
                f"{c:30s}"
                + " | ".join(values)
            )

        # ==============================================================
        # 10. CORRELATION MATRIX
        # ==============================================================

        print(
            "\n10. CORRELATION MATRIX"
        )

        assembler = VectorAssembler(
            inputCols=FEATURE_COLS,
            outputCol="feature_vector",
        )

        vector_df = (
            assembler
            .transform(back)
            .select("feature_vector")
        )

        corr = (
            Correlation
            .corr(
                vector_df,
                "feature_vector",
            )
            .head()[0]
            .toArray()
        )

        print(
            "Correlation matrix:"
        )

        for row in corr:
            print(
                " ".join(
                    f"{float(x):8.4f}"
                    for x in row
                )
            )

        # ==============================================================
        # FINAL GATE
        # ==============================================================

        print(
            "\n" + "=" * 72
        )

        print(
            "STAGE 14 CHECKS: PASSED"
        )

        print(
            "=" * 72
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()