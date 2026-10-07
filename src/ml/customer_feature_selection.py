"""Stage 15A: select and scale repeat-customer ML features.

Feature selection is rule-driven and uses only evidence already available
from Stage 12 and Stage 14. customer_id is never an ML feature.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.features.customer_features import FEATURE_COLS
from src.utils.spark_session import get_spark


CUSTOMER_TABLE = "internet_usage.customer_features"

OUTPUT_PATH = (
    "hdfs://localhost:9000/internet_usage/features/customer_ml_features"
)

CONFIG_PATH = Path("configs/customer_feature_config.json")

REPORT_PATH = Path(
    "docs/stage15_customer_feature_selection_report.txt"
)

NEAR_CONSTANT_SHARE = 0.95


def load_stage12_config() -> dict:
    """Load the actual Stage 12 feature-selection configuration."""

    path = Path("configs/session_feature_config.json")

    with path.open() as f:
        return json.load(f)


def check_stage12_evidence(stage12: dict) -> dict:
    """Translate the actual Stage 12 config into customer-level evidence."""

    selected_numeric = set(
        stage12.get("numeric_features", [])
    )

    selected_categorical = set(
        stage12.get("categorical_features", [])
    )

    excluded = set(
        stage12.get("excluded_from_ml", [])
    )

    evidence = {
        "app_name_selected": "app_name" in selected_categorical,

        "network_type_selected":
            "network_type" in selected_categorical,

        "is_weekend_selected":
            "is_weekend" not in excluded
            and "is_weekend" in selected_numeric,

        "is_night_selected":
            "is_night" not in excluded
            and "is_night" in selected_numeric,

        "network_type_excluded":
            "network_type" in excluded,

        "is_weekend_excluded":
            "is_weekend" in excluded,

        "is_night_excluded":
            "is_night" in excluded,
    }

    return evidence


def zscore_values(df, columns):
    """Assemble selected features and standardize them."""

    assembler = VectorAssembler(
        inputCols=columns,
        outputCol="raw_features",
        handleInvalid="error",
    )

    assembled = assembler.transform(df)

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    scaler_model = scaler.fit(assembled)

    scaled = scaler_model.transform(assembled)

    return scaled, scaler_model


def main() -> None:
    """Run Stage 15A feature selection and validation."""

    spark = get_spark(
        "Stage15A_CustomerFeatureSelection",
        hive=True,
    )

    try:
        print("STAGE 15A — CUSTOMER FEATURE SELECTION")

        # ------------------------------------------------------------
        # Create / verify Hive database
        # ------------------------------------------------------------

        spark.sql(
            "CREATE DATABASE IF NOT EXISTS internet_usage"
        )

        # ------------------------------------------------------------
        # Load customer features
        # ------------------------------------------------------------

        cf = spark.table(CUSTOMER_TABLE)

        total_rows = cf.count()

        distinct_customers = (
            cf.select("customer_id")
            .distinct()
            .count()
        )

        print(f"Input rows: {total_rows}")
        print(
            f"Distinct customers: {distinct_customers}"
        )

        if total_rows != distinct_customers:
            raise RuntimeError(
                "Customer feature table is not "
                "one-row-per-customer."
            )

        if total_rows != 33766:
            raise RuntimeError(
                "Expected 33,766 repeat customers, "
                f"found {total_rows}"
            )

        # ------------------------------------------------------------
        # Stage 12 evidence
        # ------------------------------------------------------------

        stage12 = load_stage12_config()

        evidence = check_stage12_evidence(
            stage12
        )

        if not evidence["app_name_selected"]:
            raise RuntimeError(
                "Stage 12 did not select app_name; "
                "unique_app_count cannot be justified."
            )

        if not evidence["network_type_excluded"]:
            raise RuntimeError(
                "Expected network_type to be "
                "excluded in Stage 12."
            )

        if not evidence["is_weekend_excluded"]:
            raise RuntimeError(
                "Expected is_weekend to be "
                "excluded in Stage 12."
            )

        if not evidence["is_night_excluded"]:
            raise RuntimeError(
                "Expected is_night to be "
                "excluded in Stage 12."
            )

        # ------------------------------------------------------------
        # Candidate validation
        # ------------------------------------------------------------

        null_columns = (
            ["customer_id"] + FEATURE_COLS
        )

        null_counts = (
            cf.select(
                *[
                    F.sum(
                        F.col(c)
                        .isNull()
                        .cast("int")
                    ).alias(c)
                    for c in null_columns
                ]
            )
            .collect()[0]
            .asDict()
        )

        if any(
            int(value or 0) != 0
            for value in null_counts.values()
        ):
            raise RuntimeError(
                f"Null values found: {null_counts}"
            )

        # ------------------------------------------------------------
        # R1: Exact redundancy
        #
        # total_data_mb =
        # total_sessions * avg_data_mb
        # ------------------------------------------------------------

        redundancy_error = (
            cf
            .withColumn(
                "_expected_total_data",
                F.col("total_sessions")
                * F.col("avg_data_mb"),
            )
            .withColumn(
                "_error",
                F.abs(
                    F.col("total_data_mb")
                    - F.col("_expected_total_data")
                ),
            )
            .agg(
                F.max("_error")
                .alias("max_error")
            )
            .collect()[0]["max_error"]
        )

        redundancy_error = float(
            redundancy_error or 0.0
        )

        r1_drop = []

        if redundancy_error <= 1e-9:
            r1_drop.append(
                "total_data_mb"
            )

        # ------------------------------------------------------------
        # R2: Near-constant features
        # ------------------------------------------------------------

        r2_drop = []

        for col_name in FEATURE_COLS:

            top = (
                cf
                .groupBy(col_name)
                .count()
                .orderBy(
                    F.desc("count")
                )
                .limit(1)
                .collect()
            )

            if not top:
                continue

            dominant_share = (
                top[0]["count"]
                / total_rows
            )

            print(
                f"{col_name}: "
                f"dominant_share="
                f"{dominant_share:.6f}"
            )

            if (
                dominant_share
                >= NEAR_CONSTANT_SHARE
            ):
                r2_drop.append(
                    col_name
                )

        # ------------------------------------------------------------
        # R3: Stage 12 evidence
        #
        # Do not reintroduce session attributes that
        # Stage 12 explicitly excluded.
        # ------------------------------------------------------------

        r3_drop = []

        if evidence[
            "network_type_excluded"
        ]:
            r3_drop.append(
                "unique_network_count"
            )

        if evidence[
            "is_weekend_excluded"
        ]:
            r3_drop.append(
                "weekend_sessions"
            )

        if evidence[
            "is_night_excluded"
        ]:
            r3_drop.append(
                "night_sessions"
            )

        # ------------------------------------------------------------
        # R4: max_session_data_mb
        #
        # Keep it only if at least 10% of its variance
        # is not explained by avg_data_mb.
        # ------------------------------------------------------------

        correlation = (
            cf
            .select(
                F.corr(
                    "avg_data_mb",
                    "max_session_data_mb",
                ).alias("corr")
            )
            .collect()[0]["corr"]
        )

        correlation = float(
            correlation
        )

        unexplained_fraction = (
            1.0 - correlation ** 2
        )

        r4_keep = (
            unexplained_fraction >= 0.10
        )

        r4_drop = []

        if not r4_keep:
            r4_drop.append(
                "max_session_data_mb"
            )

        # ------------------------------------------------------------
        # Final feature selection
        # ------------------------------------------------------------

        all_drops = set(
            r1_drop
            + r2_drop
            + r3_drop
            + r4_drop
        )

        selected = [
            column
            for column in FEATURE_COLS
            if column not in all_drops
        ]

        if len(selected) < 2:
            raise RuntimeError(
                "Too few selected customer features: "
                f"{selected}"
            )

        print()
        print(
            "===== FEATURE DECISION ====="
        )

        print(
            f"R1 drop: {r1_drop}"
        )

        print(
            f"R2 drop: {r2_drop}"
        )

        print(
            f"R3 drop: {r3_drop}"
        )

        print(
            f"R4 correlation: "
            f"{correlation:.6f}"
        )

        print(
            f"R4 unexplained fraction: "
            f"{unexplained_fraction:.6f}"
        )

        print(
            "R4 keep max_session_data_mb: "
            f"{r4_keep}"
        )

        print(
            f"Final selected features: "
            f"{selected}"
        )

        # ------------------------------------------------------------
        # Build and scale ML features
        # ------------------------------------------------------------

        scaled, scaler_model = (
            zscore_values(
                cf,
                selected,
            )
        )

        ml_features = scaled.select(
            "customer_id",
            "features",
        )

        # ------------------------------------------------------------
        # Vector-length validation
        #
        # Spark ML VectorUDT is not a normal ARRAY.
        # Convert it using vector_to_array() before size().
        # ------------------------------------------------------------

        feature_length = (
            ml_features
            .select(
                vector_to_array(
                    "features"
                ).alias(
                    "features_array"
                )
            )
            .select(
                F.size(
                    "features_array"
                ).alias("n")
            )
            .agg(
                F.min("n").alias(
                    "min_n"
                ),
                F.max("n").alias(
                    "max_n"
                ),
            )
            .collect()[0]
        )

        min_vector_length = (
            feature_length["min_n"]
        )

        max_vector_length = (
            feature_length["max_n"]
        )

        if (
            min_vector_length
            != len(selected)
            or
            max_vector_length
            != len(selected)
        ):
            raise RuntimeError(
                "Feature vector length "
                "validation failed: "
                f"min={min_vector_length}, "
                f"max={max_vector_length}, "
                f"expected={len(selected)}"
            )

        # ------------------------------------------------------------
        # Row preservation
        # ------------------------------------------------------------

        output_rows = ml_features.count()

        if output_rows != total_rows:
            raise RuntimeError(
                "Rows changed during "
                "feature construction."
            )

        output_distinct_customers = (
            ml_features
            .select("customer_id")
            .distinct()
            .count()
        )

        if (
            output_distinct_customers
            != distinct_customers
        ):
            raise RuntimeError(
                "Customer IDs changed during "
                "feature construction."
            )

        # ------------------------------------------------------------
        # Independent scaling validation
        # ------------------------------------------------------------

        scaler_means = [
            float(value)
            for value
            in scaler_model.mean.toArray()
        ]

        scaler_stds = [
            float(value)
            for value
            in scaler_model.std.toArray()
        ]

        stats = (
            cf
            .select(
                *[
                    F.avg(column).alias(
                        f"{column}_mean"
                    )
                    for column in selected
                ],
                *[
                    F.stddev_samp(column).alias(
                        f"{column}_std"
                    )
                    for column in selected
                ],
            )
            .collect()[0]
            .asDict()
        )

        maximum_scaling_error = 0.0

        for index, column in enumerate(
            selected
        ):

            observed_mean = float(
                stats[
                    f"{column}_mean"
                ]
            )

            observed_std = float(
                stats[
                    f"{column}_std"
                ]
            )

            maximum_scaling_error = max(
                maximum_scaling_error,
                abs(
                    observed_mean
                    - scaler_means[index]
                ),
                abs(
                    observed_std
                    - scaler_stds[index]
                ),
            )

        if maximum_scaling_error > 1e-9:
            raise RuntimeError(
                "Independent scaling "
                "validation failed: "
                f"{maximum_scaling_error}"
            )

        # ------------------------------------------------------------
        # Write ML features to HDFS
        # ------------------------------------------------------------

        (
            ml_features
            .write
            .mode("overwrite")
            .parquet(
                OUTPUT_PATH
            )
        )

        # ------------------------------------------------------------
        # HDFS readback validation
        # ------------------------------------------------------------

        readback = (
            spark.read
            .parquet(
                OUTPUT_PATH
            )
        )

        readback_rows = (
            readback.count()
        )

        if readback_rows != total_rows:
            raise RuntimeError(
                "HDFS readback row count "
                "validation failed."
            )

        readback_distinct = (
            readback
            .select("customer_id")
            .distinct()
            .count()
        )

        if (
            readback_distinct
            != distinct_customers
        ):
            raise RuntimeError(
                "HDFS readback distinct "
                "customer validation failed."
            )

        # ------------------------------------------------------------
        # Save Stage 15A configuration
        # ------------------------------------------------------------

        CONFIG_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        config = {
            "stage": 15,
            "part": "15A",
            "purpose": (
                "Leak-free repeat-customer "
                "K-Means input"
            ),
            "input_table": CUSTOMER_TABLE,
            "output_path": OUTPUT_PATH,
            "row_count": total_rows,
            "distinct_customer_ids":
                distinct_customers,
            "candidate_features":
                FEATURE_COLS,
            "selected_features":
                selected,
            "excluded_features":
                sorted(all_drops),
            "stage12_evidence":
                evidence,
            "rules": {
                "R1_exact_redundancy": {
                    "dropped": r1_drop,
                    "max_error":
                        redundancy_error,
                },
                "R2_near_constant": {
                    "threshold":
                        NEAR_CONSTANT_SHARE,
                    "dropped": r2_drop,
                },
                "R3_stage12_evidence": {
                    "dropped": r3_drop,
                },
                "R4_max_vs_average": {
                    "correlation":
                        correlation,
                    "unexplained_fraction":
                        unexplained_fraction,
                    "threshold": 0.10,
                    "kept": r4_keep,
                },
            },
            "scaler": {
                "withMean": True,
                "withStd": True,
                "mean": scaler_means,
                "std": scaler_stds,
            },
            "validation": {
                "rows_preserved": True,
                "distinct_ids_preserved": True,
                "vector_length": True,
                "no_nulls": True,
                "independent_scaling": True,
                "readback": True,
            },
        }

        with CONFIG_PATH.open("w") as f:
            json.dump(
                config,
                f,
                indent=2,
            )

        # ------------------------------------------------------------
        # Save Stage 15A report
        # ------------------------------------------------------------

        REPORT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with REPORT_PATH.open("w") as f:

            f.write(
                "STAGE 15A — "
                "CUSTOMER FEATURE SELECTION\n"
            )

            f.write(
                "=" * 60 + "\n\n"
            )

            f.write(
                f"Input rows: "
                f"{total_rows}\n"
            )

            f.write(
                "Distinct customers: "
                f"{distinct_customers}\n\n"
            )

            f.write(
                "Candidate features:\n"
            )

            for column in FEATURE_COLS:
                f.write(
                    f"  - {column}\n"
                )

            f.write(
                "\nFeature decisions:\n"
            )

            f.write(
                f"R1 dropped: "
                f"{r1_drop}\n"
            )

            f.write(
                f"R2 dropped: "
                f"{r2_drop}\n"
            )

            f.write(
                f"R3 dropped: "
                f"{r3_drop}\n"
            )

            f.write(
                f"R4 correlation: "
                f"{correlation:.6f}\n"
            )

            f.write(
                "R4 unexplained fraction: "
                f"{unexplained_fraction:.6f}\n"
            )

            f.write(
                f"R4 kept: "
                f"{r4_keep}\n\n"
            )

            f.write(
                "Selected features:\n"
            )

            for column in selected:
                f.write(
                    f"  - {column}\n"
                )

            f.write(
                "\nStage 12 evidence:\n"
            )

            for key, value in evidence.items():
                f.write(
                    f"  {key}: {value}\n"
                )

            f.write(
                "\nScaler statistics:\n"
            )

            for index, column in enumerate(
                selected
            ):
                f.write(
                    f"  {column}: "
                    f"mean={scaler_means[index]:.12f}, "
                    f"std={scaler_stds[index]:.12f}\n"
                )

            f.write(
                "\nValidation:\n"
            )

            f.write(
                "  rows preserved: PASSED\n"
            )

            f.write(
                "  distinct IDs preserved: PASSED\n"
            )

            f.write(
                "  vector length: PASSED\n"
            )

            f.write(
                "  no nulls: PASSED\n"
            )

            f.write(
                "  independent scaling: PASSED\n"
            )

            f.write(
                "  HDFS readback: PASSED\n"
            )

            f.write(
                "\nSTAGE 15A CHECKS: PASSED\n"
            )

        print()
        print(
            "STAGE 15A CHECKS: PASSED"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()