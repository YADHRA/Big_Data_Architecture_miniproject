"""Stage 13B: fit and validate the final session-level K-Means model."""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import functions as F

from src.utils.spark_session import (
    get_spark,
    FEATURES_PATH,
    RESULTS_PATH,
)


# ---------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = (
    ROOT
    / "configs"
    / "session_feature_config.json"
)

SWEEP_PATH = (
    ROOT
    / "docs"
    / "stage13_k_sweep.json"
)

INFO_PATH = (
    ROOT
    / "configs"
    / "session_kmeans_info.json"
)


# ---------------------------------------------------------------------
# Final-model settings
# ---------------------------------------------------------------------

SEED = 42
MAX_ITER = 50
TOL = 1e-6

FEATURE_PATH = (
    f"{FEATURES_PATH}/session_ml_features"
)

CLUSTERS_PATH = (
    f"{RESULTS_PATH}/session_clusters"
)


# ---------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------

def load_json(path: Path) -> dict:
    """Load a JSON file into a Python dictionary."""

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:

    # ================================================================
    # LOAD CONFIGURATION
    # ================================================================

    config = load_json(CONFIG_PATH)
    sweep = load_json(SWEEP_PATH)

    expected_rows = int(
        config["row_count"]
    )

    expected_vector_length = int(
        config["feature_vector_length"]
    )

    scaler_mean = [
        float(x)
        for x in config[
            "standard_scaler"
        ]["mean"]
    ]

    scaler_std = [
        float(x)
        for x in config[
            "standard_scaler"
        ]["std"]
    ]

    numeric_features = config[
        "numeric_features"
    ]

    app_labels = config[
        "app_name_labels"
    ]

    # -----------------------------------------------------------------
    # Stage 13 sweep selected K
    # -----------------------------------------------------------------

    selected_k = sweep.get(
        "selected_k"
    )

    if selected_k is None:
        raise RuntimeError(
            "Stage 13 sweep does not contain "
            "'selected_k'."
        )

    k = int(selected_k)

    # ================================================================
    # HEADER
    # ================================================================

    print("=" * 72)
    print("STAGE 13B — FINAL K-MEANS MODEL")
    print("=" * 72)

    print(
        f"Selected K from sweep: {k}"
    )

    print(
        f"Seed: {SEED}"
    )

    print(
        f"Max iterations: {MAX_ITER}"
    )

    print(
        f"Tolerance: {TOL}"
    )

    print(
        f"Expected rows: {expected_rows:,}"
    )

    print(
        f"Expected vector length: "
        f"{expected_vector_length}"
    )

    print(
        f"App labels: {app_labels}"
    )

    # ================================================================
    # START SPARK
    # ================================================================

    spark = get_spark(
        "Stage13B_KMeans_Final",
        hive=True,
    )

    feats = None
    predictions = None
    joined = None

    try:

        # ============================================================
        # 1. LOAD STAGE 12 FEATURES
        # ============================================================

        print()
        print("=" * 72)
        print("1. LOAD AND VALIDATE FEATURES")
        print("=" * 72)

        feats = (
            spark.read
            .parquet(FEATURE_PATH)
            .select(
                "session_id",
                "features",
            )
            .cache()
        )

        n = feats.count()

        if n != expected_rows:
            raise RuntimeError(
                f"Row count mismatch: "
                f"expected {expected_rows:,}, "
                f"got {n:,}"
            )

        distinct_ids = (
            feats
            .select("session_id")
            .distinct()
            .count()
        )

        if distinct_ids != expected_rows:
            raise RuntimeError(
                "Distinct session ID count mismatch: "
                f"expected {expected_rows:,}, "
                f"got {distinct_ids:,}"
            )

        print(
            f"Rows: {n:,} ✓"
        )

        print(
            f"Distinct session IDs: "
            f"{distinct_ids:,} ✓"
        )

        # ============================================================
        # 2. FIT FINAL K-MEANS
        # ============================================================

        print()
        print("=" * 72)
        print("2. FIT FINAL K-MEANS")
        print("=" * 72)

        t0 = time.time()

        kmeans = (
            KMeans(
                k=k,
                seed=SEED,
                maxIter=MAX_ITER,
                tol=TOL,
                featuresCol="features",
                predictionCol="prediction",
            )
        )

        model = kmeans.fit(feats)

        fit_seconds = (
            time.time() - t0
        )

        iterations = int(
            model.summary.numIter
        )

        wssse = float(
            model.summary.trainingCost
        )

        print(
            f"Fit time: {fit_seconds:.1f} seconds"
        )

        print(
            f"Iterations: {iterations}"
        )

        print(
            f"WSSSE: {wssse:.4f}"
        )

        # ============================================================
        # 3. FINAL SILHOUETTE
        # ============================================================

        predictions = (
            model
            .transform(feats)
            .cache()
        )

        prediction_count = (
            predictions.count()
        )

        if prediction_count != n:
            raise RuntimeError(
                "Prediction row count mismatch."
            )

        evaluator = (
            ClusteringEvaluator(
                featuresCol="features",
                predictionCol="prediction",
                metricName="silhouette",
                distanceMeasure="squaredEuclidean",
            )
        )

        silhouette = float(
            evaluator.evaluate(
                predictions
            )
        )

        print(
            f"Final silhouette: "
            f"{silhouette:.6f}"
        )

        # ============================================================
        # 4. CLUSTER SIZES
        # ============================================================

        cluster_sizes = (
            predictions
            .groupBy("prediction")
            .count()
            .orderBy("prediction")
            .collect()
        )

        sizes = {
            int(row["prediction"]):
            int(row["count"])
            for row in cluster_sizes
        }

        if set(sizes) != set(range(k)):
            raise RuntimeError(
                f"Expected cluster IDs "
                f"0..{k - 1}, got "
                f"{sorted(sizes)}"
            )

        print()
        print("Cluster sizes:")

        for cluster_id in range(k):

            count = sizes[
                cluster_id
            ]

            percentage = (
                100.0 * count / n
            )

            print(
                f"  Cluster {cluster_id}: "
                f"{count:,} "
                f"({percentage:.2f}%)"
            )

        # ============================================================
        # 5. SAVE FINAL MODEL
        # ============================================================

        print()
        print("=" * 72)
        print("3. SAVE FINAL MODEL")
        print("=" * 72)

        model_path = (
            f"{RESULTS_PATH}/models/"
            f"session_kmeans_k{k}"
        )

        (
            model
            .write()
            .overwrite()
            .save(model_path)
        )

        print(
            f"Model saved: {model_path}"
        )

        # ============================================================
        # 6. SAVE CLUSTER ASSIGNMENTS
        # ============================================================

        cluster_output = (
            predictions
            .select(
                "session_id",
                F.col(
                    "prediction"
                ).alias("cluster"),
            )
        )

        (
            cluster_output
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(CLUSTERS_PATH)
        )

        print(
            f"Cluster assignments saved: "
            f"{CLUSTERS_PATH}"
        )

        # ============================================================
        # 7. CREATE / REFRESH HIVE TABLE
        # ============================================================

        print()
        print(
            "Preparing Hive database and table..."
        )

        # IMPORTANT:
        # The previous run failed because the
        # internet_usage database did not exist
        # in this fresh Spark session.
        #
        # Create it first.

        spark.sql(
            "CREATE DATABASE IF NOT EXISTS "
            "internet_usage"
        )

        print(
            "Hive database verified: "
            "internet_usage"
        )

        spark.sql(
            "DROP TABLE IF EXISTS "
            "internet_usage.session_clusters"
        )

        spark.sql(
            f"""
            CREATE TABLE
                internet_usage.session_clusters
            USING PARQUET
            LOCATION '{CLUSTERS_PATH}'
            """
        )

        hive_count = (
            spark
            .table(
                "internet_usage.session_clusters"
            )
            .count()
        )

        print(
            "Hive table: "
            "internet_usage.session_clusters"
        )

        print(
            f"Hive rows: {hive_count:,}"
        )

        # ============================================================
        # 8. LOAD ORIGINAL FEATURES FOR PROFILING
        # ============================================================

        print()
        print("=" * 72)
        print("4. CLUSTER PROFILES")
        print("=" * 72)

        original = (
            spark
            .table(
                "internet_usage.session_features"
            )
            .select(
                "session_id",
                "data_consumed_mb",
                "session_duration_minutes",
                "app_name",
                "app_category",
                "operator",
                "network_type",
                "time_of_day",
                "is_night",
                "is_weekend",
            )
        )

        cluster_assignments = (
            spark
            .read
            .parquet(CLUSTERS_PATH)
        )

        joined = (
            cluster_assignments
            .join(
                original,
                "session_id",
                "inner",
            )
            .cache()
        )

        joined_count = (
            joined.count()
        )

        if joined_count != n:
            raise RuntimeError(
                "Cluster/original-data join "
                "did not preserve all rows."
            )

        # ------------------------------------------------------------
        # Cluster numerical profile
        # ------------------------------------------------------------

        profile = (
            joined
            .groupBy("cluster")
            .agg(
                F.count("*").alias(
                    "sessions"
                ),

                F.round(
                    100
                    * F.count("*")
                    / n,
                    2,
                ).alias("pct"),

                F.round(
                    F.avg(
                        "data_consumed_mb"
                    ),
                    2,
                ).alias("avg_mb"),

                F.round(
                    F.percentile_approx(
                        "data_consumed_mb",
                        0.5,
                    ),
                    2,
                ).alias("median_mb"),

                F.round(
                    F.min(
                        "data_consumed_mb"
                    ),
                    2,
                ).alias("min_mb"),

                F.round(
                    F.max(
                        "data_consumed_mb"
                    ),
                    2,
                ).alias("max_mb"),

                F.round(
                    F.avg(
                        "session_duration_minutes"
                    ),
                    2,
                ).alias(
                    "avg_duration_min"
                ),

                F.round(
                    F.percentile_approx(
                        "session_duration_minutes",
                        0.5,
                    ),
                    2,
                ).alias(
                    "median_duration_min"
                ),

                F.round(
                    100
                    * F.avg("is_night"),
                    2,
                ).alias(
                    "pct_night"
                ),

                F.round(
                    100
                    * F.avg("is_weekend"),
                    2,
                ).alias(
                    "pct_weekend"
                ),
            )
            .orderBy("cluster")
        )

        profile.show(
            50,
            truncate=False,
        )

        # ============================================================
        # 9. SHOW K-MEANS CENTERS
        # ============================================================

        print()
        print("=" * 72)
        print(
            "5. K-MEANS CENTERS "
            "(STAGE 12 SCALED FEATURE SPACE)"
        )
        print("=" * 72)

        centers = [
            np.array(center)
            for center in model.clusterCenters()
        ]

        # Stage 12 feature vector:
        #
        # slot 0 = standardized data consumption
        # slot 1 = standardized session duration
        # slots 2..9 = app_name one-hot

        feature_names = [
            "data_consumed_mb_standardized",
            "session_duration_minutes_standardized",
        ] + [
            f"app_name={label}"
            for label in app_labels
        ]

        for slot, feature_name in enumerate(
            feature_names
        ):

            values = " ".join(
                f"c{cluster}="
                f"{centers[cluster][slot]:.4f}"
                for cluster in range(k)
            )

            print(
                f"{feature_name:45s} "
                f"{values}"
            )

        # ============================================================
        # 10. UN-SCALE NUMERIC CENTERS
        # ============================================================

        print()
        print("=" * 72)
        print(
            "6. CENTERS CONVERTED BACK "
            "TO ORIGINAL NUMERIC UNITS"
        )
        print("=" * 72)

        unscaled_centers = {}

        for cluster_id in range(k):

            data_value = (
                centers[cluster_id][0]
                * scaler_std[0]
                + scaler_mean[0]
            )

            duration_value = (
                centers[cluster_id][1]
                * scaler_std[1]
                + scaler_mean[1]
            )

            unscaled_centers[
                cluster_id
            ] = {
                "data_consumed_mb":
                    float(data_value),

                "session_duration_minutes":
                    float(duration_value),
            }

            print(
                f"Cluster {cluster_id}: "
                f"data={data_value:.2f} MB, "
                f"duration={duration_value:.2f} min"
            )

        # ============================================================
        # 11. CATEGORY / APP COMPOSITION
        # ============================================================

        print()
        print("=" * 72)
        print(
            "7. CATEGORY / APP COMPOSITION"
        )
        print("=" * 72)

        def print_composition(
            column: str,
        ) -> None:

            rows = (
                joined
                .groupBy(
                    "cluster",
                    column,
                )
                .count()
                .collect()
            )

            by_cluster = {}

            for row in rows:

                cluster_id = int(
                    row["cluster"]
                )

                value = row[column]

                count = int(
                    row["count"]
                )

                by_cluster.setdefault(
                    cluster_id,
                    {},
                )[value] = count

            values = sorted(
                {
                    value
                    for cluster_values
                    in by_cluster.values()
                    for value
                    in cluster_values
                },
                key=str,
            )

            print()
            print(
                f"--- {column} ---"
            )

            for cluster_id in range(k):

                cluster_values = (
                    by_cluster
                    .get(
                        cluster_id,
                        {},
                    )
                )

                total = sum(
                    cluster_values.values()
                )

                pieces = []

                for value in values:

                    pct = (
                        100
                        * cluster_values.get(
                            value,
                            0,
                        )
                        / total
                    )

                    pieces.append(
                        f"{value}={pct:.2f}%"
                    )

                print(
                    f"Cluster {cluster_id}: "
                    + ", ".join(pieces)
                )

        for column in [
            "app_category",
            "app_name",
            "operator",
            "network_type",
            "time_of_day",
        ]:

            print_composition(
                column
            )

        # ============================================================
        # 12. FINAL VALIDATION CHECKS
        # ============================================================

        print()
        print("=" * 72)
        print("8. FINAL VALIDATION")
        print("=" * 72)

        # ------------------------------------------------------------
        # Check 1: assignments
        # ------------------------------------------------------------

        assignment_count = (
            cluster_assignments
            .count()
        )

        assignment_ids = (
            cluster_assignments
            .select("session_id")
            .distinct()
            .count()
        )

        ok_assignments = (
            assignment_count == n
            and assignment_ids == n
            and joined_count == n
        )

        print(
            "Assignments preserve rows/IDs: "
            f"{ok_assignments}"
        )

        # ------------------------------------------------------------
        # Check 2: cluster IDs and sizes
        # ------------------------------------------------------------

        model_sizes = [
            int(x)
            for x in model.summary.clusterSizes
        ]

        ok_clusters = (
            set(sizes)
            == set(range(k))
            and all(
                count > 0
                for count in sizes.values()
            )
            and sum(sizes.values()) == n
            and [
                sizes[c]
                for c in range(k)
            ]
            == model_sizes
        )

        print(
            "Cluster IDs/sizes valid: "
            f"{ok_clusters}"
        )

        # ------------------------------------------------------------
        # Check 3: convergence
        # ------------------------------------------------------------

        ok_convergence = (
            iterations < MAX_ITER
        )

        print(
            "Converged before iteration cap: "
            f"{ok_convergence} "
            f"({iterations} < {MAX_ITER})"
        )

        # ------------------------------------------------------------
        # Check 4: centers vs actual averages
        # ------------------------------------------------------------

        actual_averages = {
            int(row["cluster"]): row
            for row in (
                joined
                .groupBy("cluster")
                .agg(
                    F.avg(
                        "data_consumed_mb"
                    ).alias(
                        "avg_data"
                    ),

                    F.avg(
                        "session_duration_minutes"
                    ).alias(
                        "avg_duration"
                    ),
                )
                .collect()
            )
        }

        center_differences = []

        for cluster_id in range(k):

            predicted_data = (
                centers[cluster_id][0]
                * scaler_std[0]
                + scaler_mean[0]
            )

            predicted_duration = (
                centers[cluster_id][1]
                * scaler_std[1]
                + scaler_mean[1]
            )

            actual_data = float(
                actual_averages[
                    cluster_id
                ]["avg_data"]
            )

            actual_duration = float(
                actual_averages[
                    cluster_id
                ]["avg_duration"]
            )

            center_differences.extend(
                [
                    abs(
                        predicted_data
                        - actual_data
                    ),
                    abs(
                        predicted_duration
                        - actual_duration
                    ),
                ]
            )

        max_center_difference = max(
            center_differences
        )

        ok_centers = (
            max_center_difference
            < 1e-6
        )

        print(
            "Unscaled centers match actual "
            f"cluster averages: {ok_centers} "
            f"(max diff="
            f"{max_center_difference:.3e})"
        )

        # ------------------------------------------------------------
        # Check 5: saved model reload
        # ------------------------------------------------------------

        loaded_model = (
            KMeansModel
            .load(model_path)
        )

        loaded_centers = [
            np.array(center)
            for center
            in loaded_model.clusterCenters()
        ]

        max_model_difference = max(
            float(
                np.max(
                    np.abs(
                        a - b
                    )
                )
            )
            for a, b in zip(
                centers,
                loaded_centers,
            )
        )

        ok_model_reload = (
            max_model_difference
            < 1e-12
        )

        print(
            "Saved model reload check: "
            f"{ok_model_reload} "
            f"(max center diff="
            f"{max_model_difference:.3e})"
        )

        # ------------------------------------------------------------
        # Check 6: Hive table
        # ------------------------------------------------------------

        ok_hive = (
            hive_count == n
        )

        print(
            "Hive table row count valid: "
            f"{ok_hive}"
        )

        # ============================================================
        # 13. SAVE MODEL INFORMATION
        # ============================================================

        info = {
            "stage": 13,

            "purpose": (
                "Final session-level K-Means "
                "usage segmentation"
            ),

            "k": k,

            "seed": SEED,

            "max_iter": MAX_ITER,

            "tol": TOL,

            "iterations": iterations,

            "wssse": wssse,

            "silhouette": silhouette,

            "suggested_k_by_sweep": int(
                sweep["selected_k"]
            ),

            "feature_vector_length":
                expected_vector_length,

            "numeric_features":
                numeric_features,

            "app_name_labels":
                app_labels,

            "centers_scaled": [
                center.tolist()
                for center in centers
            ],

            "centers_unscaled_numeric":
                {
                    str(cluster_id):
                    values
                    for cluster_id, values
                    in unscaled_centers.items()
                },

            "cluster_sizes": [
                sizes[cluster_id]
                for cluster_id
                in range(k)
            ],

            "model_path":
                model_path,

            "clusters_path":
                CLUSTERS_PATH,

            "hive_table":
                "internet_usage.session_clusters",

            "note": (
                "Cluster IDs are arbitrary labels. "
                "Cluster names should be assigned "
                "only after profile review."
            ),
        }

        INFO_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        INFO_PATH.write_text(
            json.dumps(
                info,
                indent=2,
            ),
            encoding="utf-8",
        )

        print()
        print(
            f"Model information saved: "
            f"{INFO_PATH}"
        )

        # ============================================================
        # FINAL RESULT
        # ============================================================

        all_checks = all(
            [
                ok_assignments,
                ok_clusters,
                ok_convergence,
                ok_centers,
                ok_model_reload,
                ok_hive,
            ]
        )

        print()
        print("=" * 72)

        if all_checks:

            print(
                "STAGE 13 FINAL CHECKS: PASSED"
            )

        else:

            print(
                "STAGE 13 FINAL CHECKS: FAILED"
            )

        print(
            f"assignments={ok_assignments}, "
            f"clusters={ok_clusters}, "
            f"converged={ok_convergence}, "
            f"centers={ok_centers}, "
            f"model={ok_model_reload}, "
            f"hive={ok_hive}"
        )

        print("=" * 72)

        if not all_checks:
            raise RuntimeError(
                "One or more Stage 13 "
                "final validation checks failed."
            )

    finally:

        # Always release cached Spark DataFrames,
        # even if an exception occurs.

        if joined is not None:
            joined.unpersist()

        if predictions is not None:
            predictions.unpersist()

        if feats is not None:
            feats.unpersist()

        spark.stop()


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()