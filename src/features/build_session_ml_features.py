"""Stage 12: build the leak-free session-level ML feature vector."""

import json
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.utils.spark_session import FEATURES_PATH, get_spark


# ----------------------------------------------------------------------
# Paths and configuration
# ----------------------------------------------------------------------

INPUT_TABLE = "internet_usage.session_features"

OUTPUT_PATH = f"{FEATURES_PATH}/session_ml_features"

CONFIG_PATH = Path("configs/session_feature_config.json")

REPORT_PATH = Path("docs/stage12_report.txt")


# ----------------------------------------------------------------------
# Approved ML features
# ----------------------------------------------------------------------

NUMERIC_COLS = [
    "data_consumed_mb",
    "session_duration_minutes",
]

CATEGORICAL_COLS = [
    "app_name",
]


# 2 standardized numeric features
# + 8 app_name one-hot features
# = 10 total features
EXPECTED_VECTOR_LENGTH = 10

TOL = 1e-6


def main():
    """Build, validate, persist, and document Stage 12 ML features."""

    spark = get_spark(
        "Stage12BuildSessionMLFeatures",
        hive=True,
    )

    try:
        print("=" * 80)
        print("STAGE 12 - BUILD SESSION ML FEATURES")
        print("=" * 80)

        print(f"\nInput Hive table: {INPUT_TABLE}")
        print(f"Output HDFS path: {OUTPUT_PATH}")

        # ------------------------------------------------------------------
        # 1. Read Stage 11 Hive table
        # ------------------------------------------------------------------

        df = spark.table(INPUT_TABLE)

        input_rows = df.count()

        input_ids = (
            df.select("session_id")
            .distinct()
            .count()
        )

        print(f"\nInput rows: {input_rows:,}")
        print(f"Input distinct session IDs: {input_ids:,}")

        # ------------------------------------------------------------------
        # 2. Select ONLY approved source columns
        #
        # IDs are retained only for validation/output identification.
        # They are NOT included in the ML feature vector.
        # ------------------------------------------------------------------

        source = df.select(
            "session_id",
            *NUMERIC_COLS,
            *CATEGORICAL_COLS,
        )

        # ------------------------------------------------------------------
        # 3. StringIndexer + OneHotEncoder
        #
        # app_name has 8 categories.
        #
        # dropLast=False is intentional because:
        # - this is K-Means, not linear regression
        # - we want all app categories represented explicitly
        # ------------------------------------------------------------------

        indexer = StringIndexer(
            inputCol="app_name",
            outputCol="app_name_index",
            handleInvalid="error",
        )

        encoder = OneHotEncoder(
            inputCol="app_name_index",
            outputCol="app_name_ohe",
            dropLast=False,
        )

        # ------------------------------------------------------------------
        # 4. Assemble numeric features
        # ------------------------------------------------------------------

        numeric_assembler = VectorAssembler(
            inputCols=NUMERIC_COLS,
            outputCol="numeric_raw",
        )

        # ------------------------------------------------------------------
        # 5. Standardize numeric features
        #
        # withMean=True:
        #   mean approximately 0
        #
        # withStd=True:
        #   sample standard deviation approximately 1
        # ------------------------------------------------------------------

        scaler = StandardScaler(
            inputCol="numeric_raw",
            outputCol="numeric_scaled",
            withMean=True,
            withStd=True,
        )

        # ------------------------------------------------------------------
        # 6. Final feature vector
        #
        # Vector layout:
        #
        # [0] data_consumed_mb standardized
        # [1] session_duration_minutes standardized
        # [2:] app_name one-hot encoding
        # ------------------------------------------------------------------

        final_assembler = VectorAssembler(
            inputCols=[
                "numeric_scaled",
                "app_name_ohe",
            ],
            outputCol="features",
        )

        # ------------------------------------------------------------------
        # 7. Build Spark ML preprocessing pipeline
        # ------------------------------------------------------------------

        pipeline = Pipeline(
            stages=[
                indexer,
                encoder,
                numeric_assembler,
                scaler,
                final_assembler,
            ]
        )

        print("\nFitting Spark ML preprocessing pipeline...")

        model = pipeline.fit(source)

        # ------------------------------------------------------------------
        # 8. Transform the source data
        #
        # Keep source columns temporarily so we can validate the generated
        # vector against the original data.
        # ------------------------------------------------------------------

        transformed = (
            model.transform(source)
            .select(
                "session_id",
                "data_consumed_mb",
                "session_duration_minutes",
                "app_name",
                "features",
            )
        )

        # ------------------------------------------------------------------
        # 9. Row and ID preservation
        # ------------------------------------------------------------------

        output_rows = transformed.count()

        output_ids = (
            transformed.select("session_id")
            .distinct()
            .count()
        )

        print(f"\nOutput rows: {output_rows:,}")
        print(f"Output distinct session IDs: {output_ids:,}")

        rows_ok = output_rows == input_rows

        ids_ok = output_ids == input_ids

        # ------------------------------------------------------------------
        # 10. Feature vector length validation
        # ------------------------------------------------------------------

        feature_array = vector_to_array("features")

        vector_lengths = (
            transformed
            .select(
                F.size(feature_array).alias("length")
            )
            .groupBy("length")
            .count()
            .orderBy("length")
        )

        print("\nFeature vector lengths:")

        vector_lengths.show(truncate=False)

        length_rows = vector_lengths.collect()

        vector_length_ok = (
            len(length_rows) == 1
            and length_rows[0]["length"] == EXPECTED_VECTOR_LENGTH
            and length_rows[0]["count"] == output_rows
        )

        # ------------------------------------------------------------------
        # 11. Null / NaN / infinity validation
        # ------------------------------------------------------------------

        null_counts = (
            transformed
            .agg(
                *[
                    F.sum(
                        F.col(column).isNull().cast("long")
                    ).alias(column)
                    for column in [
                        "session_id",
                        *NUMERIC_COLS,
                        "app_name",
                        "features",
                    ]
                ]
            )
            .collect()[0]
            .asDict()
        )

        null_ok = all(
            (value or 0) == 0
            for value in null_counts.values()
        )

        # Spark 4.1.1 does not expose F.isinf().
        #
        # Therefore infinity is checked explicitly using equality
        # against positive and negative infinity.

        bad_vector_values = (
            transformed
            .select(
                F.explode(feature_array).alias("value")
            )
            .where(
                F.col("value").isNull()
                | F.isnan("value")
                | (F.col("value") == float("inf"))
                | (F.col("value") == float("-inf"))
            )
            .count()
        )

        finite_ok = bad_vector_values == 0

        # ------------------------------------------------------------------
        # 12. Verify standardized numeric slots
        #
        # Slot 0 = standardized data_consumed_mb
        # Slot 1 = standardized session_duration_minutes
        # ------------------------------------------------------------------

        numeric_stats = (
            transformed
            .select(
                F.avg(feature_array[0]).alias("mean_data"),
                F.stddev_samp(feature_array[0]).alias("std_data"),
                F.avg(feature_array[1]).alias("mean_duration"),
                F.stddev_samp(feature_array[1]).alias("std_duration"),
            )
            .collect()[0]
        )

        scaling_ok = (
            abs(numeric_stats["mean_data"]) < TOL
            and abs(numeric_stats["mean_duration"]) < TOL
            and abs(numeric_stats["std_data"] - 1.0) < TOL
            and abs(numeric_stats["std_duration"] - 1.0) < TOL
        )

        print("\nScaled numeric statistics:")

        print(
            "  data_consumed_mb: "
            f"mean={numeric_stats['mean_data']:.12f}, "
            f"std={numeric_stats['std_data']:.12f}"
        )

        print(
            "  session_duration_minutes: "
            f"mean={numeric_stats['mean_duration']:.12f}, "
            f"std={numeric_stats['std_duration']:.12f}"
        )

        # ------------------------------------------------------------------
        # 13. Independent scaling validation
        #
        # Recalculate z-scores directly from the source data and compare
        # them with the values produced by Spark StandardScaler.
        # ------------------------------------------------------------------

        independent_stats = (
            source
            .select(
                *[
                    F.avg(column).alias(
                        f"mean_{column}"
                    )
                    for column in NUMERIC_COLS
                ],
                *[
                    F.stddev_samp(column).alias(
                        f"std_{column}"
                    )
                    for column in NUMERIC_COLS
                ],
            )
            .collect()[0]
            .asDict()
        )

        independent_df = source

        for column in NUMERIC_COLS:
            independent_df = independent_df.withColumn(
                f"z_{column}",
                (
                    F.col(column)
                    - F.lit(
                        independent_stats[
                            f"mean_{column}"
                        ]
                    )
                )
                / F.lit(
                    independent_stats[
                        f"std_{column}"
                    ]
                ),
            )

        independent_df = independent_df.select(
            "session_id",
            *[
                f"z_{column}"
                for column in NUMERIC_COLS
            ],
        )

        actual_numeric = (
            transformed
            .select(
                "session_id",
                feature_array[0].alias("actual_data"),
                feature_array[1].alias("actual_duration"),
            )
        )

        scaling_comparison = (
            independent_df
            .join(
                actual_numeric,
                on="session_id",
                how="inner",
            )
            .select(
                F.abs(
                    F.col("z_data_consumed_mb")
                    - F.col("actual_data")
                ).alias("err_data"),
                F.abs(
                    F.col("z_session_duration_minutes")
                    - F.col("actual_duration")
                ).alias("err_duration"),
            )
        )

        max_errors = (
            scaling_comparison
            .agg(
                F.max("err_data").alias(
                    "max_data_error"
                ),
                F.max("err_duration").alias(
                    "max_duration_error"
                ),
            )
            .collect()[0]
        )

        independent_scaling_ok = (
            max_errors["max_data_error"] < TOL
            and max_errors["max_duration_error"] < TOL
        )

        print("\nIndependent scaling validation:")

        print(
            "  max data error = "
            f"{max_errors['max_data_error']:.12e}"
        )

        print(
            "  max duration error = "
            f"{max_errors['max_duration_error']:.12e}"
        )

        # ------------------------------------------------------------------
        # 14. Validate app_name one-hot encoding
        #
        # IMPORTANT:
        # Do NOT collect 800K rows to Python.
        #
        # Spark performs this validation in a distributed manner.
        # ------------------------------------------------------------------

        index_model = model.stages[0]

        labels = list(index_model.labels)

        app_ohe_start = len(NUMERIC_COLS)

        expected_ohe = F.array(
            *[
                F.when(
                    F.col("app_name") == label,
                    F.lit(1.0),
                ).otherwise(
                    F.lit(0.0)
                )
                for label in labels
            ]
        )

        actual_ohe = F.slice(
            feature_array,
            app_ohe_start + 1,
            len(labels),
        )

        ohe_error = F.zip_with(
            actual_ohe,
            expected_ohe,
            lambda actual, expected:
                F.abs(actual - expected),
        )

        max_ohe_error = (
            transformed
            .select(
                F.array_max(ohe_error).alias(
                    "max_error"
                )
            )
            .collect()[0]["max_error"]
        )

        one_hot_ok = (
            max_ohe_error is not None
            and max_ohe_error <= TOL
        )

        print(f"\napp_name labels: {labels}")

        print(
            "Maximum one-hot error: "
            f"{max_ohe_error:.12e}"
        )

        print(
            "One-hot validation: "
            f"{'PASSED' if one_hot_ok else 'FAILED'}"
        )

        # ------------------------------------------------------------------
        # 15. Confirm expected feature-vector structure
        # ------------------------------------------------------------------

        vector_length_ok = (
            vector_length_ok
            and EXPECTED_VECTOR_LENGTH
            == (
                len(NUMERIC_COLS)
                + len(labels)
            )
        )

        # ------------------------------------------------------------------
        # 16. Overall in-memory validation
        # ------------------------------------------------------------------

        all_checks = [
            rows_ok,
            ids_ok,
            vector_length_ok,
            null_ok,
            finite_ok,
            scaling_ok,
            independent_scaling_ok,
            one_hot_ok,
        ]

        print("\n" + "=" * 80)
        print("STAGE 12 VALIDATION")
        print("=" * 80)

        checks = {
            "rows_preserved": rows_ok,
            "distinct_session_ids_preserved": ids_ok,
            "vector_length": vector_length_ok,
            "no_nulls": null_ok,
            "no_nan_or_inf": finite_ok,
            "standard_scaling": scaling_ok,
            "independent_scaling_match": independent_scaling_ok,
            "app_name_one_hot": one_hot_ok,
        }

        for name, passed in checks.items():
            print(
                f"  {name:<35} "
                f"{'PASSED' if passed else 'FAILED'}"
            )

        if not all(all_checks):
            raise RuntimeError(
                "Stage 12 in-memory validation FAILED."
            )

        # ------------------------------------------------------------------
        # 17. Write final ML feature dataset to HDFS
        #
        # Only session_id and features are written.
        # IDs are NOT inside the feature vector.
        # ------------------------------------------------------------------

        print(
            f"\nWriting final ML features to:\n"
            f"{OUTPUT_PATH}"
        )

        (
            transformed
            .select(
                "session_id",
                "features",
            )
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(OUTPUT_PATH)
        )

        # ------------------------------------------------------------------
        # 18. Read-back validation
        #
        # Verify:
        # - row count
        # - distinct IDs
        # - vector length
        # - actual vector values
        # ------------------------------------------------------------------

        print(
            "\nReading final Parquet output back "
            "from HDFS..."
        )

        saved = spark.read.parquet(
            OUTPUT_PATH
        )

        saved_rows = saved.count()

        saved_ids = (
            saved
            .select("session_id")
            .distinct()
            .count()
        )

        saved_lengths = (
            saved
            .select(
                F.size(
                    vector_to_array("features")
                ).alias("length")
            )
            .groupBy("length")
            .count()
            .collect()
        )

        # Prepare computed and saved vectors.
        computed_for_compare = (
            transformed
            .select(
                "session_id",
                vector_to_array(
                    "features"
                ).alias(
                    "computed_features"
                ),
            )
        )

        saved_for_compare = (
            saved
            .select(
                "session_id",
                vector_to_array(
                    "features"
                ).alias(
                    "saved_features"
                ),
            )
        )

        # Join by unique session_id and calculate the element-wise
        # absolute difference between the two vectors.
        joined_vectors = (
            computed_for_compare
            .join(
                saved_for_compare,
                on="session_id",
                how="inner",
            )
        )

        vector_error = F.zip_with(
            F.col("computed_features"),
            F.col("saved_features"),
            lambda computed, saved_value:
                F.abs(
                    computed - saved_value
                ),
        )

        comparison_result = (
            joined_vectors
            .select(
                F.array_max(
                    vector_error
                ).alias(
                    "max_vector_error"
                )
            )
            .agg(
                F.count("*").alias(
                    "compared_rows"
                ),
                F.max(
                    "max_vector_error"
                ).alias(
                    "global_max_vector_error"
                ),
            )
            .collect()[0]
        )

        compared_rows = (
            comparison_result["compared_rows"]
        )

        global_max_vector_error = (
            comparison_result[
                "global_max_vector_error"
            ]
        )

        readback_ok = (
            saved_rows == output_rows
            and saved_ids == output_ids
            and compared_rows == output_rows
            and len(saved_lengths) == 1
            and saved_lengths[0]["length"]
            == EXPECTED_VECTOR_LENGTH
            and saved_lengths[0]["count"]
            == output_rows
            and global_max_vector_error is not None
            and global_max_vector_error <= TOL
        )

        print(
            f"  rows: {saved_rows:,}"
        )

        print(
            f"  distinct session IDs: "
            f"{saved_ids:,}"
        )

        print(
            f"  compared rows: "
            f"{compared_rows:,}"
        )

        print(
            "  maximum feature-vector error: "
            f"{global_max_vector_error:.12e}"
        )

        print(
            "  read-back validation: "
            f"{'PASSED' if readback_ok else 'FAILED'}"
        )

        if not readback_ok:
            raise RuntimeError(
                "Read-back validation FAILED."
            )

        # ------------------------------------------------------------------
        # 19. Save reusable configuration for Stage 13
        # ------------------------------------------------------------------

        scaler_model = model.stages[3]

        config = {
            "stage": 12,
            "purpose": (
                "Leak-free session-level "
                "K-Means input"
            ),
            "input_table": INPUT_TABLE,
            "output_path": OUTPUT_PATH,
            "row_count": output_rows,
            "distinct_session_ids": output_ids,
            "feature_vector_length": (
                EXPECTED_VECTOR_LENGTH
            ),
            "feature_order": [
                "data_consumed_mb_standardized",
                "session_duration_minutes_standardized",
                "app_name_one_hot",
            ],
            "numeric_features": NUMERIC_COLS,
            "categorical_features": CATEGORICAL_COLS,
            "app_name_labels": labels,
            "app_name_one_hot_drop_last": False,
            "standard_scaler": {
                "withMean": True,
                "withStd": True,
                "mean": [
                    float(value)
                    for value
                    in scaler_model.mean.toArray()
                ],
                "std": [
                    float(value)
                    for value
                    in scaler_model.std.toArray()
                ],
            },
            "excluded_from_ml": [
                "session_id",
                "customer_id",
                "app_category",
                "hour",
                "time_of_day",
                "is_night",
                "day_of_week",
                "is_weekend",
                "operator",
                "network_type",
                "timestamp",
                "usage_date",
                "day_of_month",
                "month",
            ],
            "validation": {
                **checks,
                "read_back": readback_ok,
            },
        }

        CONFIG_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        CONFIG_PATH.write_text(
            json.dumps(
                config,
                indent=2,
            ),
            encoding="utf-8",
        )

        print(
            f"\nSaved configuration: "
            f"{CONFIG_PATH}"
        )

        # ------------------------------------------------------------------
        # 20. Save Stage 12 handoff report
        # ------------------------------------------------------------------

        REPORT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        report = f"""STAGE 12 REPORT
================

Project:
Internet Usage and Data Consumption Analytics using Apache Spark and Power BI

Purpose:
Build a leak-free session-level ML feature dataset for Stage 13 K-Means clustering.

Input:
Hive table: {INPUT_TABLE}

Output:
HDFS path: {OUTPUT_PATH}

Rows:
{output_rows:,}

Distinct session IDs:
{output_ids:,}

Selected ML features:
1. data_consumed_mb -> StandardScaler
2. session_duration_minutes -> StandardScaler
3. app_name -> StringIndexer + OneHotEncoder(dropLast=False)

Feature vector length:
{EXPECTED_VECTOR_LENGTH}

Feature vector order:
Slot 0:
  standardized data_consumed_mb

Slot 1:
  standardized session_duration_minutes

Slots 2-{EXPECTED_VECTOR_LENGTH - 1}:
  app_name one-hot encoding in StringIndexer label order:
  {labels}

Feature selection evidence:
app_name eta-squared for data consumption = 0.597739
app_name incremental eta-squared beyond app_category = 0.034102

app_category was excluded because app_name is the finer feature
and provided sufficient incremental information.

Time-based features were excluded from K-Means because their
eta-squared values were far below the 0.01 selection threshold.

Excluded from ML:
session_id
customer_id
app_category
hour
time_of_day
is_night
day_of_week
is_weekend
operator
network_type
timestamp
usage_date
day_of_month
month

Validation:
Rows preserved: {rows_ok}
Distinct session IDs preserved: {ids_ok}
Vector length valid: {vector_length_ok}
No null values: {null_ok}
No NaN/infinite feature values: {finite_ok}
Standard scaling valid: {scaling_ok}
Independent scaling comparison: {independent_scaling_ok}
App-name one-hot validation: {one_hot_ok}
HDFS read-back validation: {readback_ok}
Compared rows after read-back: {compared_rows:,}
Maximum read-back vector error: {global_max_vector_error:.12e}

Methodology notes:
- This is an unsupervised learning preparation stage.
- No train/test split is required for this feature-building stage.
- session_id and customer_id are identifiers, not ML features.
- Standardization is applied only to the two continuous numeric features.
- app_name is one-hot encoded without dropping a category.
- Stage 13 should evaluate K-Means for multiple K values.
- Stage 13 should evaluate cluster stability across random seeds.
- Cluster labels should be assigned only after inspecting cluster profiles.
- The dataset is synthetic, so findings describe the generated dataset
  and should not be presented as measured real-world telecom behavior.
"""

        REPORT_PATH.write_text(
            report,
            encoding="utf-8",
        )

        print(
            f"Saved Stage 12 report: "
            f"{REPORT_PATH}"
        )

        # ------------------------------------------------------------------
        # 21. Final success message
        # ------------------------------------------------------------------

        print("\n" + "=" * 80)
        print("STAGE 12 CHECKS: PASSED")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()