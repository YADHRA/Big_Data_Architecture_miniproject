"""Stage 13: K-Means sweep for session-level usage segmentation."""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.utils.spark_session import get_spark


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = (
    PROJECT_ROOT
    / "configs"
    / "session_feature_config.json"
)

JSON_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "stage13_k_sweep.json"
)

REPORT_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "stage13_sweep_report.txt"
)


INPUT_PATH = (
    "hdfs://localhost:9000/"
    "internet_usage/features/session_ml_features"
)


# K values to test.
K_VALUES = list(range(2, 9))

# Multiple seeds are used to check clustering stability.
SEEDS = [42, 7, 2024]

MAX_ITER = 50
TOL = 1e-6

# Every cluster must contain at least 1% of the data.
MIN_CLUSTER_SHARE = 0.01

# Mean pairwise ARI must be at least 0.80.
ARI_THRESHOLD = 0.80

# If several K values are within 0.01 silhouette of the best,
# select the smaller K.
SILHOUETTE_TOLERANCE = 0.01


def load_config() -> dict:
    """Load Stage 12 feature configuration."""

    with open(
        CONFIG_PATH,
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def contingency_ari(
    df_a,
    df_b,
) -> float:
    """
    Calculate Adjusted Rand Index between two cluster assignments.

    Each input DataFrame must contain:
        session_id
        prediction_a

    The prediction column is renamed independently before joining
    so Spark does not encounter duplicate column names.
    """

    # --------------------------------------------------------------
    # Rename the assignment columns BEFORE joining.
    # --------------------------------------------------------------

    a = (
        df_a
        .select(
            "session_id",
            F.col("prediction_a").alias("cluster_a"),
        )
    )

    b = (
        df_b
        .select(
            "session_id",
            F.col("prediction_a").alias("cluster_b"),
        )
    )

    # --------------------------------------------------------------
    # Build contingency table.
    #
    # Rows    = clusters from assignment A
    # Columns = clusters from assignment B
    # Values  = number of sessions in each combination
    # --------------------------------------------------------------

    contingency = (
        a
        .join(
            b,
            on="session_id",
            how="inner",
        )
        .groupBy("cluster_a")
        .pivot("cluster_b")
        .count()
        .fillna(0)
    )

    # The first column is cluster_a.
    # All remaining columns are cluster B labels.
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

    return adjusted_rand_from_contingency(matrix)


def evaluate_stability(
    assignments: dict[int, object],
) -> float:
    """
    Calculate mean pairwise ARI across all seeds.

    With three seeds, this evaluates:
        seed 42 vs 7
        seed 42 vs 2024
        seed 7 vs 2024
    """

    seed_values = sorted(assignments)

    scores = []

    for i in range(len(seed_values)):
        for j in range(i + 1, len(seed_values)):

            a = assignments[seed_values[i]]
            b = assignments[seed_values[j]]

            ari = contingency_ari(a, b)

            scores.append(ari)

    if not scores:
        raise RuntimeError(
            "No pairwise ARI scores were calculated."
        )

    return float(np.mean(scores))


def main() -> None:

    spark = get_spark(
        "Stage13_KMeans_Sweep"
    )

    try:

        print("=" * 72)
        print("STAGE 13 — K-MEANS SWEEP")
        print("=" * 72)

        config = load_config()

        expected_rows = int(
            config["row_count"]
        )

        expected_vector_length = int(
            config["feature_vector_length"]
        )

        print(
            f"Input: {INPUT_PATH}"
        )

        print(
            f"Expected rows: "
            f"{expected_rows:,}"
        )

        print(
            f"Expected vector length: "
            f"{expected_vector_length}"
        )

        print(
            f"K values: {K_VALUES}"
        )

        print(
            f"Seeds: {SEEDS}"
        )

        # ==========================================================
        # LOAD STAGE 12 FEATURE DATASET
        # ==========================================================

        df = (
            spark.read
            .parquet(INPUT_PATH)
            .select(
                "session_id",
                "features",
            )
        )

        actual_rows = df.count()

        if actual_rows != expected_rows:
            raise RuntimeError(
                f"Row count mismatch: "
                f"expected {expected_rows}, "
                f"got {actual_rows}"
            )

        # ----------------------------------------------------------
        # Validate feature vector length.
        #
        # Spark ML VectorUDT is internally represented as a struct,
        # so vector_to_array() is required before using size().
        # ----------------------------------------------------------

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
                "Vector length mismatch: "
                f"expected {expected_vector_length}, "
                f"got {actual_vector_length}"
            )

        # ----------------------------------------------------------
        # Validate unique session IDs.
        # ----------------------------------------------------------

        distinct_ids = (
            df
            .select("session_id")
            .distinct()
            .count()
        )

        if distinct_ids != expected_rows:
            raise RuntimeError(
                "Distinct session ID mismatch: "
                f"expected {expected_rows}, "
                f"got {distinct_ids}"
            )

        print(
            f"Verified rows: "
            f"{actual_rows:,}"
        )

        print(
            f"Verified distinct session IDs: "
            f"{distinct_ids:,}"
        )

        print(
            f"Verified feature vector length: "
            f"{actual_vector_length}"
        )

        # ----------------------------------------------------------
        # Cache because the same feature dataset is used repeatedly.
        # ----------------------------------------------------------

        df = df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        df.count()

        # ----------------------------------------------------------
        # Silhouette evaluator.
        # ----------------------------------------------------------

        evaluator = ClusteringEvaluator(
            featuresCol="features",
            predictionCol="prediction",
            metricName="silhouette",
            distanceMeasure="squaredEuclidean",
        )

        results = []

        # ==========================================================
        # K-MEANS SWEEP
        # ==========================================================

        for k in K_VALUES:

            print()
            print("-" * 72)
            print(f"K = {k}")
            print("-" * 72)

            assignments_for_k = {}

            for seed in SEEDS:

                print(
                    f"Running K={k}, seed={seed} ..."
                )

                # --------------------------------------------------
                # Train K-Means.
                # --------------------------------------------------

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

                # --------------------------------------------------
                # Keep features because ClusteringEvaluator needs
                # both features and prediction.
                # --------------------------------------------------

                predictions = (
                    model
                    .transform(df)
                    .select(
                        "session_id",
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
                    raise RuntimeError(
                        "Prediction row count mismatch "
                        f"for K={k}, seed={seed}"
                    )

                # --------------------------------------------------
                # Silhouette score.
                # --------------------------------------------------

                silhouette = float(
                    evaluator.evaluate(
                        predictions
                    )
                )

                # --------------------------------------------------
                # Cluster sizes.
                # --------------------------------------------------

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
                    raise RuntimeError(
                        f"Expected {k} clusters "
                        f"but got {len(counts)} "
                        f"for K={k}, seed={seed}"
                    )

                min_share = (
                    min(counts.values())
                    / actual_rows
                )

                # --------------------------------------------------
                # Cluster centers.
                # --------------------------------------------------

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

                # --------------------------------------------------
                # Training cost and iterations.
                # --------------------------------------------------

                wssse = float(
                    model.summary.trainingCost
                )

                iterations = int(
                    model.summary.numIter
                )

                # --------------------------------------------------
                # Store run-level result.
                # --------------------------------------------------

                result = {
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
                        for key, value
                        in counts.items()
                    },
                    "max_center_norm": center_norm,
                }

                results.append(result)

                # --------------------------------------------------
                # Store ONLY session_id + prediction for ARI.
                #
                # This keeps the ARI dataframe much smaller than
                # keeping the full feature vectors.
                # --------------------------------------------------

                assignments_for_k[seed] = (
                    predictions
                    .select(
                        "session_id",
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

                # --------------------------------------------------
                # The assignment dataframe is now persisted.
                # The full predictions dataframe is no longer needed.
                # --------------------------------------------------

                predictions.unpersist()

            # ======================================================
            # STABILITY ACROSS SEEDS
            # ======================================================

            mean_ari = evaluate_stability(
                assignments_for_k
            )

            for seed_df in assignments_for_k.values():
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

        # ----------------------------------------------------------
        # Release main feature dataframe.
        # ----------------------------------------------------------

        df.unpersist()

        # ==========================================================
        # AGGREGATE RESULTS BY K
        # ==========================================================

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

        # ==========================================================
        # SELECT ELIGIBLE K
        # ==========================================================

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

        if eligible:

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

        else:

            best_mean_silhouette = None
            selected_k = None

        # ==========================================================
        # SAVE JSON REPORT
        # ==========================================================

        output = {
            "stage": 13,
            "purpose": (
                "K-Means model selection using "
                "silhouette, cluster-size validity, "
                "and seed stability"
            ),
            "input_path": INPUT_PATH,
            "row_count": actual_rows,
            "distinct_session_ids":
                distinct_ids,
            "feature_vector_length":
                actual_vector_length,
            "k_values": K_VALUES,
            "seeds": SEEDS,
            "max_iter": MAX_ITER,
            "tol": TOL,
            "minimum_cluster_share":
                MIN_CLUSTER_SHARE,
            "ari_threshold":
                ARI_THRESHOLD,
            "silhouette_tolerance":
                SILHOUETTE_TOLERANCE,
            "results_by_run": results,
            "summary_by_k": summary,
            "eligible_k": [
                row["k"]
                for row in eligible
            ],
            "best_mean_silhouette":
                best_mean_silhouette,
            "selected_k": selected_k,
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

        # ==========================================================
        # SAVE HUMAN-READABLE REPORT
        # ==========================================================

        with open(
            REPORT_OUTPUT,
            "w",
            encoding="utf-8",
        ) as f:

            f.write(
                "STAGE 13 — K-MEANS SWEEP REPORT\n"
            )

            f.write(
                "=" * 72 + "\n\n"
            )

            f.write(
                f"Rows: {actual_rows:,}\n"
            )

            f.write(
                "Distinct session IDs: "
                f"{distinct_ids:,}\n"
            )

            f.write(
                "Feature vector length: "
                f"{actual_vector_length}\n"
            )

            f.write(
                f"K values: {K_VALUES}\n"
            )

            f.write(
                f"Seeds: {SEEDS}\n\n"
            )

            f.write(
                "SUMMARY BY K\n"
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
                f"{selected_k}\n"
            )

        # ==========================================================
        # FINAL CONSOLE OUTPUT
        # ==========================================================

        print()
        print("=" * 72)
        print("STAGE 13 SWEEP COMPLETE")
        print("=" * 72)

        print(
            f"JSON report:   "
            f"{JSON_OUTPUT}"
        )

        print(
            f"Text report:   "
            f"{REPORT_OUTPUT}"
        )

        print(
            f"Eligible K:    "
            f"{[row['k'] for row in eligible]}"
        )

        print(
            f"Selected K:    "
            f"{selected_k}"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()