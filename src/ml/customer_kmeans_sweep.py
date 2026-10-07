"""
Stage 15B: Customer-level K-Means sweep and shuffled-session control.

This stage:
1. Sweeps K=2..8 across multiple seeds on the real repeat-customer
   feature dataset.
2. Evaluates silhouette, WSSSE, cluster-size validity, and seed stability.
3. Selects K using predeclared rules.
4. Builds shuffled-session null controls that preserve the customer
   session-count distribution.
5. Compares real clustering against the shuffled controls.

Stage 13 is intentionally left untouched.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from src.utils.spark_session import get_spark


# ============================================================================
# PROJECT PATHS
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = (
    PROJECT_ROOT
    / "configs"
    / "customer_feature_config.json"
)

JSON_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "stage15_customer_k_sweep.json"
)

REPORT_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "stage15_customer_kmeans_report.txt"
)


# ============================================================================
# HDFS INPUTS
# ============================================================================

# Stage 15A output
INPUT_PATH = (
    "hdfs://localhost:9000/"
    "internet_usage/features/customer_ml_features"
)

# Stage 11 validated session-level feature dataset.
#
# IMPORTANT:
# We intentionally read this Parquet path directly instead of relying on
# the Hive table internet_usage.session_features.
#
# Reason:
# The project uses an embedded Derby Hive metastore. A fresh Spark process
# may not always have the same registered Hive tables available. The HDFS
# Parquet dataset is the persistent source needed for this control.
SESSION_FEATURE_PATH = (
    "hdfs://localhost:9000/"
    "internet_usage/features/session_features"
)

CUSTOMER_FEATURE_TABLE = (
    "internet_usage.customer_features"
)


# ============================================================================
# EXPERIMENT SETTINGS
# ============================================================================

K_VALUES = list(range(2, 9))

SEEDS = [42, 7, 2024]

CONTROL_SEEDS = [11, 23]

MAX_ITER = 50

TOL = 1e-6

MIN_CLUSTER_SHARE = 0.01

ARI_THRESHOLD = 0.80

SILHOUETTE_TOLERANCE = 0.01

EXPECTED_CUSTOMERS = 33766

EXPECTED_REPEAT_SESSIONS = 68521


# ============================================================================
# CONFIG
# ============================================================================

def load_config() -> dict:
    """Load Stage 15A feature-selection configuration."""

    with open(
        CONFIG_PATH,
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


# ============================================================================
# ARI
# ============================================================================

def contingency_ari(
    df_a,
    df_b,
    id_column: str = "customer_id",
) -> float:
    """
    Calculate Adjusted Rand Index using a Spark contingency table.

    Both input DataFrames must contain:
        id_column
        prediction_a
    """

    a = (
        df_a
        .select(
            id_column,
            F.col("prediction_a").alias("cluster_a"),
        )
    )

    b = (
        df_b
        .select(
            id_column,
            F.col("prediction_a").alias("cluster_b"),
        )
    )

    contingency = (
        a
        .join(
            b,
            on=id_column,
            how="inner",
        )
        .groupBy("cluster_a")
        .pivot("cluster_b")
        .count()
        .fillna(0)
    )

    columns = [
        column
        for column in contingency.columns
        if column != "cluster_a"
    ]

    if not columns:
        raise RuntimeError(
            "Could not construct ARI contingency table."
        )

    matrix = np.array(
        contingency
        .select(*columns)
        .collect(),
        dtype=np.int64,
    )

    from src.ml.cluster_metrics import (
        adjusted_rand_from_contingency,
    )

    return float(
        adjusted_rand_from_contingency(matrix)
    )


def evaluate_stability(
    assignments: dict[int, object],
) -> float:
    """Calculate mean pairwise ARI across all seeds."""

    seed_values = sorted(assignments)

    scores = []

    for i in range(len(seed_values)):

        for j in range(i + 1, len(seed_values)):

            a = assignments[seed_values[i]]
            b = assignments[seed_values[j]]

            scores.append(
                contingency_ari(
                    a,
                    b,
                    id_column="customer_id",
                )
            )

    if not scores:
        raise RuntimeError(
            "No pairwise ARI scores were calculated."
        )

    return float(np.mean(scores))


# ============================================================================
# VECTOR VALIDATION
# ============================================================================

def validate_feature_dataset(
    spark,
    config: dict,
):
    """Load and validate Stage 15A customer ML features."""

    expected_rows = int(
        config["row_count"]
    )

    selected_features = config[
        "selected_features"
    ]

    expected_vector_length = len(
        selected_features
    )

    print(
        f"Input: {INPUT_PATH}"
    )

    print(
        f"Expected customers: "
        f"{expected_rows:,}"
    )

    print(
        f"Expected vector length: "
        f"{expected_vector_length}"
    )

    df = (
        spark.read
        .parquet(INPUT_PATH)
        .select(
            "customer_id",
            "features",
        )
    )

    actual_rows = df.count()

    if actual_rows != expected_rows:
        raise RuntimeError(
            "Customer row count mismatch: "
            f"expected {expected_rows}, "
            f"got {actual_rows}"
        )

    distinct_ids = (
        df
        .select("customer_id")
        .distinct()
        .count()
    )

    if distinct_ids != expected_rows:
        raise RuntimeError(
            "Distinct customer ID mismatch: "
            f"expected {expected_rows}, "
            f"got {distinct_ids}"
        )

    # Spark 4.x VectorUDT is represented as a struct.
    # Convert vector -> array before checking its length.
    vector_lengths = (
        df
        .select(
            F.size(
                vector_to_array("features")
            ).alias("length")
        )
        .groupBy("length")
        .count()
        .orderBy("length")
        .collect()
    )

    if len(vector_lengths) != 1:
        raise RuntimeError(
            "Unexpected vector lengths: "
            f"{vector_lengths}"
        )

    actual_vector_length = int(
        vector_lengths[0]["length"]
    )

    if (
        actual_vector_length
        != expected_vector_length
    ):
        raise RuntimeError(
            "Feature vector length mismatch: "
            f"expected {expected_vector_length}, "
            f"got {actual_vector_length}"
        )

    # Check for null feature vectors.
    null_features = (
        df
        .filter(F.col("features").isNull())
        .count()
    )

    if null_features != 0:
        raise RuntimeError(
            "Null feature-vector count: "
            f"{null_features}"
        )

    print(
        f"Verified customers: "
        f"{actual_rows:,}"
    )

    print(
        f"Verified distinct customer IDs: "
        f"{distinct_ids:,}"
    )

    print(
        f"Verified feature vector length: "
        f"{actual_vector_length}"
    )

    print(
        "Verified feature vectors are non-null"
    )

    return (
        df,
        actual_rows,
        distinct_ids,
        actual_vector_length,
    )


# ============================================================================
# REAL CUSTOMER K-MEANS SWEEP
# ============================================================================

def run_real_sweep(
    df,
    actual_rows: int,
):
    """Run K=2..8 across the three predefined seeds."""

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    results = []

    for k in K_VALUES:

        print()
        print("-" * 72)
        print(
            f"REAL CUSTOMER K = {k}"
        )
        print("-" * 72)

        assignments_for_k = {}

        for seed in SEEDS:

            print(
                f"Running K={k}, seed={seed} ..."
            )

            model = (
                KMeans(
                    k=k,
                    seed=seed,
                    maxIter=MAX_ITER,
                    tol=TOL,
                    featuresCol="features",
                    predictionCol="prediction",
                )
                .fit(df)
            )

            predictions = (
                model
                .transform(df)
                .select(
                    "customer_id",
                    "features",
                    "prediction",
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            prediction_count = (
                predictions.count()
            )

            if prediction_count != actual_rows:
                predictions.unpersist()

                raise RuntimeError(
                    "Prediction row count mismatch "
                    f"for K={k}, seed={seed}: "
                    f"{prediction_count}"
                )

            silhouette = float(
                evaluator.evaluate(
                    predictions
                )
            )

            cluster_counts_rows = (
                predictions
                .groupBy("prediction")
                .count()
                .orderBy("prediction")
                .collect()
            )

            counts = {
                int(row["prediction"]):
                int(row["count"])
                for row in cluster_counts_rows
            }

            if len(counts) != k:
                predictions.unpersist()

                raise RuntimeError(
                    f"Expected {k} clusters "
                    f"but got {len(counts)} "
                    f"for K={k}, seed={seed}"
                )

            min_share = (
                min(counts.values())
                / actual_rows
            )

            centers = model.clusterCenters()

            center_norm = float(
                max(
                    math.sqrt(
                        float(
                            np.dot(
                                center,
                                center,
                            )
                        )
                    )
                    for center in centers
                )
            )

            wssse = float(
                model.summary.trainingCost
            )

            iterations = int(
                model.summary.numIter
            )

            results.append(
                {
                    "k": k,
                    "seed": seed,
                    "silhouette": silhouette,
                    "wssse": wssse,
                    "iterations": iterations,
                    "min_cluster_share": float(
                        min_share
                    ),
                    "cluster_counts": {
                        str(key): value
                        for key, value in counts.items()
                    },
                    "max_center_norm": center_norm,
                }
            )

            assignments_for_k[seed] = (
                predictions
                .select(
                    "customer_id",
                    F.col(
                        "prediction"
                    ).alias(
                        "prediction_a"
                    ),
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            # Materialize the assignment DataFrame before the original
            # prediction DataFrame is released.
            assignments_for_k[seed].count()

            print(
                f"  silhouette="
                f"{silhouette:.6f} "
                f"wssse="
                f"{wssse:.4f} "
                f"iterations="
                f"{iterations} "
                f"min_share="
                f"{min_share:.4%}"
            )

            predictions.unpersist()

        mean_ari = evaluate_stability(
            assignments_for_k
        )

        for seed_df in (
            assignments_for_k.values()
        ):
            seed_df.unpersist()

        for result in results:

            if result["k"] == k:
                result[
                    "mean_pairwise_ari"
                ] = mean_ari

        print(
            f"  mean pairwise ARI="
            f"{mean_ari:.6f}"
        )

    return results


# ============================================================================
# AGGREGATE REAL RESULTS
# ============================================================================

def summarize_results(
    results,
):
    """Aggregate run-level results by K."""

    summary = []

    for k in K_VALUES:

        rows = [
            result
            for result in results
            if result["k"] == k
        ]

        if len(rows) != len(SEEDS):
            raise RuntimeError(
                f"Expected {len(SEEDS)} "
                f"results for K={k}, "
                f"got {len(rows)}"
            )

        mean_silhouette = float(
            np.mean(
                [
                    row["silhouette"]
                    for row in rows
                ]
            )
        )

        std_silhouette = float(
            np.std(
                [
                    row["silhouette"]
                    for row in rows
                ]
            )
        )

        mean_wssse = float(
            np.mean(
                [
                    row["wssse"]
                    for row in rows
                ]
            )
        )

        mean_ari = float(
            rows[0][
                "mean_pairwise_ari"
            ]
        )

        minimum_share = float(
            min(
                row["min_cluster_share"]
                for row in rows
            )
        )

        share_ok = all(
            row["min_cluster_share"]
            >= MIN_CLUSTER_SHARE
            for row in rows
        )

        stable = (
            mean_ari
            >= ARI_THRESHOLD
        )

        summary.append(
            {
                "k": k,
                "mean_silhouette":
                    mean_silhouette,
                "std_silhouette":
                    std_silhouette,
                "mean_wssse":
                    mean_wssse,
                "mean_pairwise_ari":
                    mean_ari,
                "min_cluster_share":
                    minimum_share,
                "all_seeds_above_share_threshold":
                    share_ok,
                "stable":
                    stable,
            }
        )

    return summary


# ============================================================================
# SELECT K
# ============================================================================

def select_k(summary):
    """Apply the predefined K-selection rules."""

    eligible = [
        row
        for row in summary
        if (
            row[
                "all_seeds_above_share_threshold"
            ]
            and row["stable"]
        )
    ]

    if not eligible:
        return (
            eligible,
            None,
            None,
        )

    best_mean_silhouette = max(
        row["mean_silhouette"]
        for row in eligible
    )

    near_best = [
        row
        for row in eligible
        if (
            row["mean_silhouette"]
            >= (
                best_mean_silhouette
                - SILHOUETTE_TOLERANCE
            )
        )
    ]

    selected_k = min(
        row["k"]
        for row in near_best
    )

    return (
        eligible,
        selected_k,
        best_mean_silhouette,
    )


# ============================================================================
# LOAD REPEAT-CUSTOMER SESSIONS
# ============================================================================

def load_repeat_customer_sessions(
    spark,
):
    """
    Load the 68,521 sessions belonging to repeat customers.

    IMPORTANT:
    Read Stage 11 Parquet directly from HDFS rather than relying on
    internet_usage.session_features being registered in the current
    embedded Hive metastore.
    """

    print()
    print("=" * 72)
    print(
        "LOADING REPEAT-CUSTOMER SESSIONS"
    )
    print("=" * 72)

    print(
        f"Session feature source: "
        f"{SESSION_FEATURE_PATH}"
    )

    sessions = (
        spark.read
        .parquet(SESSION_FEATURE_PATH)
        .select(
            "session_id",
            "customer_id",
            "data_consumed_mb",
            "session_duration_minutes",
            "app_name",
            "network_type",
            "is_weekend",
            "is_night",
        )
    )

    # ------------------------------------------------------------------------
    # Validate source row count.
    # ------------------------------------------------------------------------

    total_session_rows = sessions.count()

    if total_session_rows != 800000:
        raise RuntimeError(
            "Stage 11 session feature row count "
            "unexpected: "
            f"expected 800000, "
            f"got {total_session_rows}"
        )

    # ------------------------------------------------------------------------
    # Validate session IDs.
    # ------------------------------------------------------------------------

    distinct_session_ids = (
        sessions
        .select("session_id")
        .distinct()
        .count()
    )

    if distinct_session_ids != total_session_rows:
        raise RuntimeError(
            "Stage 11 session IDs are not unique: "
            f"rows={total_session_rows}, "
            f"distinct={distinct_session_ids}"
        )

    # ------------------------------------------------------------------------
    # Find repeat customers.
    # ------------------------------------------------------------------------

    customer_counts = (
        sessions
        .groupBy("customer_id")
        .count()
        .filter(
            F.col("count") >= 2
        )
        .select(
            "customer_id",
            F.col("count").alias(
                "total_sessions"
            ),
        )
    )

    repeat_sessions = (
        sessions
        .join(
            customer_counts.select(
                "customer_id"
            ),
            on="customer_id",
            how="left_semi",
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

    actual_session_rows = (
        repeat_sessions.count()
    )

    expected_session_rows = (
        customer_counts
        .agg(
            F.sum(
                "total_sessions"
            ).alias("n")
        )
        .collect()[0]["n"]
    )

    expected_session_rows = int(
        expected_session_rows
    )

    if (
        actual_session_rows
        != expected_session_rows
    ):
        repeat_sessions.unpersist()

        raise RuntimeError(
            "Repeat-session conservation "
            "check failed: "
            f"rows={actual_session_rows}, "
            f"expected={expected_session_rows}"
        )

    if (
        actual_session_rows
        != EXPECTED_REPEAT_SESSIONS
    ):
        repeat_sessions.unpersist()

        raise RuntimeError(
            "Repeat-session count mismatch: "
            f"expected {EXPECTED_REPEAT_SESSIONS}, "
            f"got {actual_session_rows}"
        )

    customer_count = (
        customer_counts.count()
    )

    if customer_count != EXPECTED_CUSTOMERS:
        repeat_sessions.unpersist()

        raise RuntimeError(
            "Repeat customer count mismatch: "
            f"expected {EXPECTED_CUSTOMERS}, "
            f"got {customer_count}"
        )

    # ------------------------------------------------------------------------
    # Validate customer count distribution.
    # ------------------------------------------------------------------------

    distribution = {
        int(row["total_sessions"]):
        int(row["n"])
        for row in (
            customer_counts
            .groupBy("total_sessions")
            .count()
            .withColumnRenamed(
                "count",
                "n",
            )
            .collect()
        )
    }

    expected_distribution = {
        2: 32802,
        3: 940,
        4: 23,
        5: 1,
    }

    if distribution != expected_distribution:
        repeat_sessions.unpersist()

        raise RuntimeError(
            "Repeat-customer session-count "
            "distribution mismatch.\n"
            f"Expected: {expected_distribution}\n"
            f"Actual:   {distribution}"
        )

    print(
        f"Verified source sessions: "
        f"{total_session_rows:,}"
    )

    print(
        f"Verified unique session IDs: "
        f"{distinct_session_ids:,}"
    )

    print(
        f"Repeat customers: "
        f"{customer_count:,}"
    )

    print(
        f"Repeat sessions: "
        f"{actual_session_rows:,}"
    )

    print(
        f"Session-count distribution: "
        f"{distribution}"
    )

    print(
        "Repeat-customer input checks: PASSED"
    )

    return (
        repeat_sessions,
        customer_counts,
        actual_session_rows,
    )


# ============================================================================
# SHUFFLED SESSION CONTROL
# ============================================================================

def build_shuffled_sessions(
    repeat_sessions,
    customer_counts,
    seed: int,
):
    """
    Randomly reassign sessions to customer IDs while preserving
    the exact customer session-count distribution.

    Method:
      1. Randomly order the real sessions.
      2. Expand each customer's session count into that many
         target customer slots.
      3. Randomly order the target slots.
      4. Match source sessions to target customer slots by
         random position.

    This preserves:
      - number of sessions
      - total data
      - total duration
      - exact multiset of customer session counts

    It destroys the original relationship between a customer's
    sessions and is therefore a null/control construction.
    """

    print(
        f"Building shuffled control "
        f"with seed={seed} ..."
    )

    # ------------------------------------------------------------------------
    # Randomly order source sessions.
    # ------------------------------------------------------------------------

    source = (
        repeat_sessions
        .withColumn(
            "_source_order",
            F.row_number().over(
                Window.orderBy(
                    F.rand(seed)
                )
            ),
        )
    )

    # ------------------------------------------------------------------------
    # Expand each customer into target slots.
    # ------------------------------------------------------------------------

    target_slots = (
        customer_counts
        .select(
            "customer_id",
            F.explode(
                F.sequence(
                    F.lit(1),
                    F.col(
                        "total_sessions"
                    ),
                )
            ).alias("_slot"),
        )
        .withColumn(
            "_target_order",
            F.row_number().over(
                Window.orderBy(
                    F.rand(seed + 1000)
                )
            ),
        )
        .select(
            F.col(
                "_target_order"
            ).alias("_source_order"),
            F.col(
                "customer_id"
            ).alias(
                "_target_customer_id"
            ),
        )
    )

    # ------------------------------------------------------------------------
    # Reassign each source session.
    # ------------------------------------------------------------------------

    shuffled = (
        source
        .join(
            target_slots,
            on="_source_order",
            how="inner",
        )
        .select(
            "session_id",
            F.col(
                "_target_customer_id"
            ).alias("customer_id"),
            "data_consumed_mb",
            "session_duration_minutes",
            "app_name",
            "network_type",
            "is_weekend",
            "is_night",
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

    # ------------------------------------------------------------------------
    # Validate row count.
    # ------------------------------------------------------------------------

    shuffled_count = (
        shuffled.count()
    )

    original_count = (
        repeat_sessions.count()
    )

    if shuffled_count != original_count:
        shuffled.unpersist()

        raise RuntimeError(
            "Shuffled session count changed: "
            f"original={original_count}, "
            f"shuffled={shuffled_count}"
        )

    # ------------------------------------------------------------------------
    # Validate every session ID remains exactly once.
    # ------------------------------------------------------------------------

    distinct_sessions = (
        shuffled
        .select("session_id")
        .distinct()
        .count()
    )

    if distinct_sessions != original_count:
        shuffled.unpersist()

        raise RuntimeError(
            "Shuffled session IDs are not unique: "
            f"expected={original_count}, "
            f"got={distinct_sessions}"
        )

    # ------------------------------------------------------------------------
    # Validate total data.
    # ------------------------------------------------------------------------

    original_data = (
        repeat_sessions
        .agg(
            F.sum(
                "data_consumed_mb"
            ).alias("x")
        )
        .collect()[0]["x"]
    )

    shuffled_data = (
        shuffled
        .agg(
            F.sum(
                "data_consumed_mb"
            ).alias("x")
        )
        .collect()[0]["x"]
    )

    if not np.isclose(
        float(original_data),
        float(shuffled_data),
        rtol=0,
        atol=1e-6,
    ):
        shuffled.unpersist()

        raise RuntimeError(
            "Total data changed after shuffle: "
            f"original={original_data}, "
            f"shuffled={shuffled_data}"
        )

    # ------------------------------------------------------------------------
    # Validate total duration.
    # ------------------------------------------------------------------------

    original_duration = (
        repeat_sessions
        .agg(
            F.sum(
                "session_duration_minutes"
            ).alias("x")
        )
        .collect()[0]["x"]
    )

    shuffled_duration = (
        shuffled
        .agg(
            F.sum(
                "session_duration_minutes"
            ).alias("x")
        )
        .collect()[0]["x"]
    )

    if not np.isclose(
        float(original_duration),
        float(shuffled_duration),
        rtol=0,
        atol=1e-6,
    ):
        shuffled.unpersist()

        raise RuntimeError(
            "Total duration changed after shuffle: "
            f"original={original_duration}, "
            f"shuffled={shuffled_duration}"
        )

    # ------------------------------------------------------------------------
    # Validate customer session-count distribution.
    # ------------------------------------------------------------------------

    original_distribution = {
        int(row["total_sessions"]):
        int(row["n"])
        for row in (
            customer_counts
            .groupBy("total_sessions")
            .count()
            .withColumnRenamed(
                "count",
                "n",
            )
            .collect()
        )
    }

    shuffled_distribution = {
        int(row["total_sessions"]):
        int(row["n"])
        for row in (
            shuffled
            .groupBy("customer_id")
            .count()
            .withColumnRenamed(
                "count",
                "total_sessions",
            )
            .groupBy("total_sessions")
            .count()
            .withColumnRenamed(
                "count",
                "n",
            )
            .collect()
        )
    }

    if (
        original_distribution
        != shuffled_distribution
    ):
        shuffled.unpersist()

        raise RuntimeError(
            "Customer session-count "
            "distribution changed."
        )

    # ------------------------------------------------------------------------
    # Measure original-customer retention.
    # ------------------------------------------------------------------------

    same_customer = (
        shuffled.alias("s")
        .join(
            repeat_sessions
            .select(
                "session_id",
                F.col(
                    "customer_id"
                ).alias(
                    "original_customer_id"
                ),
            )
            .alias("o"),
            on="session_id",
            how="inner",
        )
        .filter(
            F.col("s.customer_id")
            ==
            F.col(
                "o.original_customer_id"
            )
        )
        .count()
    )

    same_customer_rate = (
        same_customer
        / original_count
    )

    print(
        f"  same-customer assignments="
        f"{same_customer:,} "
        f"({same_customer_rate:.4%})"
    )

    if same_customer_rate >= 0.01:
        shuffled.unpersist()

        raise RuntimeError(
            "Too many sessions retained their "
            "original customer assignment: "
            f"{same_customer_rate:.4%}"
        )

    print(
        "  session conservation: PASSED"
    )

    print(
        "  unique session IDs: PASSED"
    )

    print(
        "  total data conservation: PASSED"
    )

    print(
        "  total duration conservation: PASSED"
    )

    print(
        "  session-count distribution: PASSED"
    )

    print(
        "  original-customer retention <1%: PASSED"
    )

    return shuffled


# ============================================================================
# BUILD CUSTOMER FEATURES FROM SESSIONS
# ============================================================================

SELECTED_FEATURES = [
    "avg_data_mb",
    "avg_session_duration_min",
    "max_session_data_mb",
    "unique_app_count",
]


def build_customer_feature_vectors(
    sessions,
):
    """
    Aggregate session records to customer features and
    standardize the four Stage 15A selected features.
    """

    customer_features = (
        sessions
        .groupBy("customer_id")
        .agg(
            F.avg(
                "data_consumed_mb"
            ).alias(
                "avg_data_mb"
            ),
            F.avg(
                "session_duration_minutes"
            ).alias(
                "avg_session_duration_min"
            ),
            F.max(
                "data_consumed_mb"
            ).alias(
                "max_session_data_mb"
            ),
            F.countDistinct(
                "app_name"
            ).cast("double").alias(
                "unique_app_count"
            ),
        )
    )

    # ------------------------------------------------------------------------
    # Exactly one row per customer.
    # ------------------------------------------------------------------------

    customer_count = (
        customer_features
        .select("customer_id")
        .distinct()
        .count()
    )

    if customer_count != EXPECTED_CUSTOMERS:
        raise RuntimeError(
            "Control customer count mismatch: "
            f"expected {EXPECTED_CUSTOMERS}, "
            f"got {customer_count}"
        )

    # ------------------------------------------------------------------------
    # Null validation.
    # ------------------------------------------------------------------------

    null_condition = (
        F.col("avg_data_mb").isNull()
        |
        F.col(
            "avg_session_duration_min"
        ).isNull()
        |
        F.col(
            "max_session_data_mb"
        ).isNull()
        |
        F.col(
            "unique_app_count"
        ).isNull()
    )

    null_count = (
        customer_features
        .filter(null_condition)
        .count()
    )

    if null_count != 0:
        raise RuntimeError(
            "Null customer feature count: "
            f"{null_count}"
        )

    # ------------------------------------------------------------------------
    # Check numeric finiteness.
    # ------------------------------------------------------------------------

    nonfinite_count = (
        customer_features
        .filter(
            F.isnan("avg_data_mb")
            |
            F.isnan(
                "avg_session_duration_min"
            )
            |
            F.isnan(
                "max_session_data_mb"
            )
            |
            F.isnan(
                "unique_app_count"
            )
        )
        .count()
    )

    if nonfinite_count != 0:
        raise RuntimeError(
            "NaN customer feature count: "
            f"{nonfinite_count}"
        )

    # ------------------------------------------------------------------------
    # Assemble raw numeric features.
    # ------------------------------------------------------------------------

    assembler = VectorAssembler(
        inputCols=SELECTED_FEATURES,
        outputCol="_raw_features",
    )

    assembled = assembler.transform(
        customer_features
    )

    # ------------------------------------------------------------------------
    # Standardize control features independently.
    # ------------------------------------------------------------------------

    scaler = StandardScaler(
        inputCol="_raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    scaler_model = scaler.fit(
        assembled
    )

    result = (
        scaler_model
        .transform(assembled)
        .select(
            "customer_id",
            "features",
        )
    )

    # ------------------------------------------------------------------------
    # Validate vector length.
    # ------------------------------------------------------------------------

    vector_lengths = (
        result
        .select(
            F.size(
                vector_to_array("features")
            ).alias("length")
        )
        .groupBy("length")
        .count()
        .collect()
    )

    if (
        len(vector_lengths) != 1
        or int(vector_lengths[0]["length"])
        != len(SELECTED_FEATURES)
    ):
        raise RuntimeError(
            "Control feature vector length "
            "validation failed: "
            f"{vector_lengths}"
        )

    return result


# ============================================================================
# RUN CONTROL SWEEP
# ============================================================================

def run_control_sweep(
    repeat_sessions,
    customer_counts,
):
    """
    Run K=2..8 on two shuffled-session controls using seed 42.

    The controls provide a null benchmark for how much apparent
    clustering can arise when session records are randomly
    reassigned to customer slots.
    """

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    all_controls = []

    for control_seed in CONTROL_SEEDS:

        print()
        print("-" * 72)
        print(
            f"CONTROL SEED = {control_seed}"
        )
        print("-" * 72)

        shuffled = build_shuffled_sessions(
            repeat_sessions,
            customer_counts,
            control_seed,
        )

        try:

            control_features = (
                build_customer_feature_vectors(
                    shuffled
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            control_features.count()

            control_results = []

            for k in K_VALUES:

                print(
                    f"Control seed={control_seed}, "
                    f"K={k} ..."
                )

                model = (
                    KMeans(
                        k=k,
                        seed=42,
                        maxIter=MAX_ITER,
                        tol=TOL,
                        featuresCol="features",
                        predictionCol="prediction",
                    )
                    .fit(control_features)
                )

                predictions = (
                    model
                    .transform(control_features)
                    .select(
                        "customer_id",
                        "features",
                        "prediction",
                    )
                    .persist(
                        StorageLevel.MEMORY_AND_DISK
                    )
                )

                prediction_count = (
                    predictions.count()
                )

                if prediction_count != EXPECTED_CUSTOMERS:
                    predictions.unpersist()

                    raise RuntimeError(
                        "Control prediction row count "
                        "mismatch: "
                        f"expected={EXPECTED_CUSTOMERS}, "
                        f"got={prediction_count}"
                    )

                silhouette = float(
                    evaluator.evaluate(
                        predictions
                    )
                )

                counts = {
                    int(row["prediction"]):
                    int(row["count"])
                    for row in (
                        predictions
                        .groupBy("prediction")
                        .count()
                        .orderBy("prediction")
                        .collect()
                    )
                }

                if len(counts) != k:
                    predictions.unpersist()

                    raise RuntimeError(
                        f"Control K={k} produced "
                        f"{len(counts)} clusters."
                    )

                min_share = (
                    min(counts.values())
                    / EXPECTED_CUSTOMERS
                )

                wssse = float(
                    model.summary.trainingCost
                )

                iterations = int(
                    model.summary.numIter
                )

                control_results.append(
                    {
                        "k": k,
                        "control_seed": control_seed,
                        "kmeans_seed": 42,
                        "silhouette": silhouette,
                        "wssse": wssse,
                        "iterations": iterations,
                        "min_cluster_share":
                            float(min_share),
                        "cluster_counts": {
                            str(key): value
                            for key, value
                            in counts.items()
                        },
                    }
                )

                print(
                    f"  silhouette="
                    f"{silhouette:.6f} "
                    f"wssse="
                    f"{wssse:.4f} "
                    f"iterations="
                    f"{iterations} "
                    f"min_share="
                    f"{min_share:.4%}"
                )

                predictions.unpersist()

            all_controls.extend(
                control_results
            )

            control_features.unpersist()

        finally:
            shuffled.unpersist()

    return all_controls


# ============================================================================
# SAVE REPORT
# ============================================================================

def save_outputs(
    config,
    actual_rows,
    distinct_ids,
    vector_length,
    results,
    summary,
    eligible,
    selected_k,
    best_mean_silhouette,
    control_results,
):
    """Write JSON and human-readable Stage 15B reports."""

    output = {
        "stage": 15,
        "part": "15B",
        "purpose": (
            "Customer-level K-Means model selection "
            "with seed stability and shuffled-session "
            "null controls"
        ),
        "input_path": INPUT_PATH,
        "input_table": CUSTOMER_FEATURE_TABLE,
        "session_control_input_path":
            SESSION_FEATURE_PATH,
        "row_count": actual_rows,
        "distinct_customer_ids":
            distinct_ids,
        "feature_vector_length":
            vector_length,
        "selected_features":
            config["selected_features"],
        "k_values": K_VALUES,
        "seeds": SEEDS,
        "control_seeds":
            CONTROL_SEEDS,
        "max_iter": MAX_ITER,
        "tol": TOL,
        "minimum_cluster_share":
            MIN_CLUSTER_SHARE,
        "ari_threshold":
            ARI_THRESHOLD,
        "silhouette_tolerance":
            SILHOUETTE_TOLERANCE,
        "expected_repeat_sessions":
            EXPECTED_REPEAT_SESSIONS,
        "results_by_run": results,
        "summary_by_k": summary,
        "eligible_k": [
            row["k"]
            for row in eligible
        ],
        "best_mean_silhouette":
            best_mean_silhouette,
        "selected_k": selected_k,
        "control_results":
            control_results,
        "validation": {
            "population_matches_stage15a":
                actual_rows
                == EXPECTED_CUSTOMERS,

            "distinct_customer_ids":
                distinct_ids
                == EXPECTED_CUSTOMERS,

            "vector_length":
                vector_length
                == len(
                    config["selected_features"]
                ),

            "repeat_session_count":
                EXPECTED_REPEAT_SESSIONS,
        },
    }

    JSON_OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        JSON_OUTPUT,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            output,
            f,
            indent=2,
        )

    # ------------------------------------------------------------------------
    # Human-readable report.
    # ------------------------------------------------------------------------

    with open(
        REPORT_OUTPUT,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "STAGE 15B — CUSTOMER K-MEANS SWEEP REPORT\n"
        )

        f.write(
            "=" * 72 + "\n\n"
        )

        f.write(
            f"Customers: {actual_rows:,}\n"
        )

        f.write(
            "Distinct customer IDs: "
            f"{distinct_ids:,}\n"
        )

        f.write(
            f"Feature vector length: "
            f"{vector_length}\n"
        )

        f.write(
            "Selected features:\n"
        )

        for feature in config[
            "selected_features"
        ]:

            f.write(
                f"  - {feature}\n"
            )

        f.write("\n")

        f.write(
            "Session control input:\n"
        )

        f.write(
            f"  {SESSION_FEATURE_PATH}\n\n"
        )

        f.write(
            f"K values: {K_VALUES}\n"
        )

        f.write(
            f"Seeds: {SEEDS}\n"
        )

        f.write(
            f"Control seeds: "
            f"{CONTROL_SEEDS}\n\n"
        )

        f.write(
            "Selection rules:\n"
        )

        f.write(
            f"  Minimum cluster share: "
            f"{MIN_CLUSTER_SHARE:.2%}\n"
        )

        f.write(
            f"  Mean pairwise ARI threshold: "
            f"{ARI_THRESHOLD:.2f}\n"
        )

        f.write(
            f"  Silhouette tie tolerance: "
            f"{SILHOUETTE_TOLERANCE:.2f}\n\n"
        )

        f.write(
            "REAL CUSTOMER K-MEANS SUMMARY\n"
        )

        f.write(
            "-" * 72 + "\n"
        )

        for row in summary:

            f.write(
                f"K={row['k']} | "
                f"silhouette="
                f"{row['mean_silhouette']:.6f} | "
                f"silhouette_std="
                f"{row['std_silhouette']:.6f} | "
                f"WSSSE="
                f"{row['mean_wssse']:.4f} | "
                f"ARI="
                f"{row['mean_pairwise_ari']:.6f} | "
                f"min_share="
                f"{row['min_cluster_share']:.4%} | "
                f"stable="
                f"{row['stable']} | "
                f"share_ok="
                f"{row['all_seeds_above_share_threshold']}\n"
            )

        f.write("\n")

        f.write(
            "Eligible K: "
            f"{[row['k'] for row in eligible]}\n"
        )

        f.write(
            "Best mean silhouette: "
            f"{best_mean_silhouette}\n"
        )

        f.write(
            "Selected K: "
            f"{selected_k}\n\n"
        )

        f.write(
            "SHUFFLED-SESSION CONTROL RESULTS\n"
        )

        f.write(
            "-" * 72 + "\n"
        )

        for row in control_results:

            f.write(
                f"control_seed="
                f"{row['control_seed']} | "
                f"K={row['k']} | "
                f"silhouette="
                f"{row['silhouette']:.6f} | "
                f"WSSSE="
                f"{row['wssse']:.4f} | "
                f"iterations="
                f"{row['iterations']} | "
                f"min_share="
                f"{row['min_cluster_share']:.4%}\n"
            )


# ============================================================================
# MAIN
# ============================================================================

def main():

    spark = get_spark(
        "Stage15B_Customer_KMeans"
    )

    try:

        print("=" * 72)
        print(
            "STAGE 15B — CUSTOMER K-MEANS SWEEP"
        )
        print("=" * 72)

        config = load_config()

        # --------------------------------------------------------------------
        # Validate Stage 15A input.
        # --------------------------------------------------------------------

        (
            df,
            actual_rows,
            distinct_ids,
            vector_length,
        ) = validate_feature_dataset(
            spark,
            config,
        )

        # --------------------------------------------------------------------
        # Cache real customer features.
        # --------------------------------------------------------------------

        df = df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        df.count()

        # --------------------------------------------------------------------
        # Real K-Means sweep.
        # --------------------------------------------------------------------

        real_results = run_real_sweep(
            df,
            actual_rows,
        )

        df.unpersist()

        # --------------------------------------------------------------------
        # Summarize and select K.
        # --------------------------------------------------------------------

        summary = summarize_results(
            real_results
        )

        (
            eligible,
            selected_k,
            best_mean_silhouette,
        ) = select_k(
            summary
        )

        print()
        print("=" * 72)
        print(
            "REAL CUSTOMER K SELECTION"
        )
        print("=" * 72)

        print(
            f"Eligible K: "
            f"{[row['k'] for row in eligible]}"
        )

        print(
            f"Best mean silhouette: "
            f"{best_mean_silhouette}"
        )

        print(
            f"Selected K: "
            f"{selected_k}"
        )

        if selected_k is None:
            raise RuntimeError(
                "No eligible K satisfied "
                "the predefined stability and "
                "cluster-size rules."
            )

        # --------------------------------------------------------------------
        # Load repeat-customer sessions.
        # --------------------------------------------------------------------

        (
            repeat_sessions,
            customer_counts,
            repeat_session_count,
        ) = load_repeat_customer_sessions(
            spark
        )

        try:

            # ----------------------------------------------------------------
            # Shuffled-session controls.
            # ----------------------------------------------------------------

            control_results = run_control_sweep(
                repeat_sessions,
                customer_counts,
            )

        finally:

            repeat_sessions.unpersist()

        # --------------------------------------------------------------------
        # Save reports.
        # --------------------------------------------------------------------

        save_outputs(
            config=config,
            actual_rows=actual_rows,
            distinct_ids=distinct_ids,
            vector_length=vector_length,
            results=real_results,
            summary=summary,
            eligible=eligible,
            selected_k=selected_k,
            best_mean_silhouette=
                best_mean_silhouette,
            control_results=control_results,
        )

        # --------------------------------------------------------------------
        # Final gate.
        # --------------------------------------------------------------------

        print()
        print("=" * 72)
        print(
            "STAGE 15B COMPLETE"
        )
        print("=" * 72)

        print(
            f"Customers: "
            f"{actual_rows:,}"
        )

        print(
            f"Repeat sessions used for control: "
            f"{repeat_session_count:,}"
        )

        print(
            f"Eligible K: "
            f"{[row['k'] for row in eligible]}"
        )

        print(
            f"Selected K: "
            f"{selected_k}"
        )

        print(
            f"JSON report: "
            f"{JSON_OUTPUT}"
        )

        print(
            f"Text report: "
            f"{REPORT_OUTPUT}"
        )

        print()
        print(
            "STAGE 15B CHECKS: PASSED"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()