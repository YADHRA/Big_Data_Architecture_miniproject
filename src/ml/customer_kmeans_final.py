
"""Stage 15C: fit final customer-level K-Means, save model + assignments, validate everything."""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import functions as F

from src.utils.spark_session import get_spark, FEATURES_PATH, RESULTS_PATH


# ============================================================
# CONFIGURATION
# ============================================================

SEED = 42
MAX_ITER = 50
KM_TOL = 1e-4
CHECK_TOL = 1e-6

CONFIG_PATH = ROOT / "configs" / "customer_feature_config.json"
SWEEP_JSON = ROOT / "docs" / "stage15_customer_k_sweep.json"
SWEEP_REPORT = ROOT / "docs" / "stage15_customer_kmeans_report.txt"
INFO_PATH = ROOT / "configs" / "customer_kmeans_info.json"

CUSTOMER_FEATURE_PATH = f"{FEATURES_PATH}/customer_ml_features"
CLUSTERS_PATH_BASE = f"{RESULTS_PATH}/customer_clusters"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def section(number, title):
    print()
    print("=" * 70)
    print(f"SECTION {number}: {title}")
    print("=" * 70)


def require_file(path):
    if not path.exists():
        raise FileNotFoundError(
            f"Required file not found: {path}"
        )


# ============================================================
# ARGUMENTS
# ============================================================

ap = argparse.ArgumentParser(
    description="Stage 15C - Final customer-level K-Means"
)

ap.add_argument(
    "--k",
    type=int,
    default=None,
    help="Override the K selected by Stage 15B"
)

args = ap.parse_args()


# ============================================================
# CHECK REQUIRED FILES
# ============================================================

require_file(CONFIG_PATH)
require_file(SWEEP_JSON)
require_file(SWEEP_REPORT)


# ============================================================
# LOAD CONFIGURATION
# ============================================================

cfg = json.loads(
    CONFIG_PATH.read_text()
)

sweep = json.loads(
    SWEEP_JSON.read_text()
)


# ============================================================
# VERIFY STAGE 15B
# ============================================================

# The machine-readable Stage 15B JSON is authoritative.
# We do not depend on an exact sentence in the text report.

if not sweep:
    raise RuntimeError(
        "Stage 15B sweep JSON is empty or invalid."
    )

if "results_by_run" not in sweep or not sweep["results_by_run"]:
    raise RuntimeError(
        "Stage 15B sweep results_by_run are missing."
    )

if "summary_by_k" not in sweep or not sweep["summary_by_k"]:
    raise RuntimeError(
        "Stage 15B summary_by_k is missing."
    )

if sweep.get("selected_k") is None:
    raise RuntimeError(
        "Stage 15B did not produce selected_k."
    )

suggested = int(
    sweep["selected_k"]
)

print(
    "Stage 15B prerequisite: VERIFIED"
)

print(
    f"Stage 15B selected K: {suggested}"
)


# ============================================================
# FEATURE CONFIGURATION
# ============================================================

selected = cfg["selected_features"]

k = (
    args.k
    if args.k is not None
    else suggested
)

if k < 2:
    raise ValueError(
        "K must be at least 2."
    )


# ============================================================
# OUTPUT PATHS
# ============================================================

CLUSTERS_PATH = (
    f"{CLUSTERS_PATH_BASE}_k{k}"
)

MODEL_PATH = (
    f"{RESULTS_PATH}/models/"
    f"customer_kmeans_k{k}"
)


# ============================================================
# START SPARK
# ============================================================

spark = get_spark(
    "Stage15C-CustomerFinal",
    hive=True
)

feats = None

try:

    # ========================================================
    # SECTION 1
    # LOAD CUSTOMER ML FEATURES
    # ========================================================

    section(
        1,
        "LOAD CUSTOMER ML FEATURES"
    )

    feats = (
        spark.read
        .parquet(CUSTOMER_FEATURE_PATH)
        .cache()
    )

    n = feats.count()

    print(
        "Customer ML feature path:",
        CUSTOMER_FEATURE_PATH
    )

    print(
        "Rows:",
        n
    )

    print(
        "Selected features:",
        selected
    )

    print(
        "Expected rows from config:",
        cfg["row_count"]
    )

    if n != cfg["row_count"]:
        raise RuntimeError(
            "Customer ML feature row count "
            f"mismatch: expected {cfg['row_count']}, "
            f"got {n}"
        )


    # ========================================================
    # SECTION 2
    # SELECT K
    # ========================================================

    section(
        2,
        "SELECTED K"
    )

    # Find the summary record for the selected K.
    summary_row = next(
        (
            r
            for r in sweep["summary_by_k"]
            if int(r["k"]) == k
        ),
        None
    )

    # Find the seed-42 run for the selected K.
    run_row = next(
        (
            r
            for r in sweep["results_by_run"]
            if int(r["k"]) == k
            and int(r["seed"]) == SEED
        ),
        None
    )

    print(
        f"K selected by Stage 15B: "
        f"{suggested} | "
        f"K used: {k}"
        f"{' (MANUAL OVERRIDE)' if k != suggested else ''}"
    )

    if summary_row:

        print(
            f"K={k} Stage 15B summary:"
        )

        print(
            f"  Mean silhouette: "
            f"{summary_row['mean_silhouette']:.6f}"
        )

        print(
            f"  Mean pairwise ARI: "
            f"{summary_row['mean_pairwise_ari']:.6f}"
        )

        print(
            f"  Minimum cluster share: "
            f"{100 * summary_row['min_cluster_share']:.2f}%"
        )

        print(
            f"  Stable across seeds: "
            f"{summary_row['stable']}"
        )

    if run_row:

        print(
            f"Seed-{SEED} evidence:"
        )

        print(
            f"  Silhouette: "
            f"{run_row['silhouette']:.6f}"
        )

        print(
            f"  WSSSE: "
            f"{run_row['wssse']:.2f}"
        )

        print(
            f"  Iterations: "
            f"{run_row['iterations']}"
        )

        print(
            f"  Minimum cluster share: "
            f"{100 * run_row['min_cluster_share']:.2f}%"
        )


    # ========================================================
    # SECTION 3
    # FINAL K-MEANS MODEL
    # ========================================================

    section(
        3,
        "FINAL K-MEANS MODEL"
    )

    t0 = time.time()

    model = (
        KMeans(
            k=k,
            seed=SEED,
            maxIter=MAX_ITER,
            tol=KM_TOL,
            initMode="k-means||",
            featuresCol="features",
            predictionCol="prediction"
        )
        .fit(feats)
    )

    n_iter = model.summary.numIter

    wssse = model.summary.trainingCost

    pred = model.transform(feats)

    evaluator = ClusteringEvaluator(
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
        featuresCol="features"
    )

    silhouette = evaluator.evaluate(
        pred
    )

    fit_seconds = (
        time.time() - t0
    )

    print(
        f"K={k} | "
        f"seed={SEED} | "
        f"iterations={n_iter} "
        f"(cap {MAX_ITER}) | "
        f"WSSSE={wssse:.1f} | "
        f"silhouette={silhouette:.4f} | "
        f"fit time={fit_seconds:.0f}s"
    )

    if run_row:

        print(
            f"Stage 15B seed-{SEED} "
            f"silhouette: "
            f"{run_row['silhouette']:.4f}"
        )

        silhouette_difference = abs(
            silhouette -
            run_row["silhouette"]
        )

        print(
            f"Difference from Stage 15B: "
            f"{silhouette_difference:.2e}"
        )


    # ========================================================
    # SECTION 4
    # SAVE MODEL AND ASSIGNMENTS
    # ========================================================

    section(
        4,
        "SAVE MODEL AND CLUSTER ASSIGNMENTS"
    )

    model.write().overwrite().save(
        MODEL_PATH
    )

    (
        pred
        .select(
            "customer_id",
            F.col("prediction").alias(
                "cluster"
            )
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option(
            "compression",
            "snappy"
        )
        .parquet(
            CLUSTERS_PATH
        )
    )

    print(
        "Model saved:",
        MODEL_PATH
    )

    print(
        "Assignments saved:",
        CLUSTERS_PATH
    )


    # ========================================================
    # SECTION 5
    # REGISTER HIVE TABLE
    # ========================================================

    section(
        5,
        "REGISTER HIVE TABLE"
    )

    spark.sql(
        "DROP TABLE IF EXISTS "
        "internet_usage.customer_clusters"
    )

    spark.sql(
        f"""
        CREATE TABLE
        internet_usage.customer_clusters
        USING PARQUET
        LOCATION '{CLUSTERS_PATH}'
        """
    )

    print(
        "Hive table registered: "
        "internet_usage.customer_clusters"
    )


    # ========================================================
    # SECTION 6
    # CLUSTER SIZES
    # ========================================================

    section(
        6,
        "CLUSTER SIZES"
    )

    cl = (
        spark.read
        .parquet(CLUSTERS_PATH)
    )

    size_rows = (
        cl.groupBy("cluster")
        .count()
        .collect()
    )

    sizes = {
        int(r["cluster"]): int(r["count"])
        for r in size_rows
    }

    for c in range(k):

        count = sizes.get(
            c,
            0
        )

        print(
            f"Cluster {c}: "
            f"{count:>6d} customers "
            f"({100 * count / n:.2f}%)"
        )

    print(
        "Total:",
        sum(sizes.values())
    )


    # ========================================================
    # SECTION 7
    # VALIDATION
    # ========================================================

    section(
        7,
        "VALIDATION - STAGE 15C"
    )

    cf = spark.table(
        "internet_usage.customer_features"
    )


    # --------------------------------------------------------
    # A. INPUT POPULATION
    # --------------------------------------------------------

    n_cf = cf.count()

    n_cf_ids = (
        cf
        .select("customer_id")
        .distinct()
        .count()
    )

    ok_a = (
        n == n_cf == cfg["row_count"]
        and n_cf_ids == n_cf
    )

    print(
        f"A. input rows={n} | "
        f"customer_features rows={n_cf} | "
        f"distinct IDs={n_cf_ids} -> {ok_a}"
    )


    # --------------------------------------------------------
    # B. ASSIGNMENT UNIQUENESS
    # --------------------------------------------------------

    n_cl = cl.count()

    n_cl_ids = (
        cl
        .select("customer_id")
        .distinct()
        .count()
    )

    nulls = (
        cl
        .filter(
            F.col("cluster").isNull()
            | F.col("customer_id").isNull()
        )
        .count()
    )

    no_duplicates = (
        n_cl == n_cl_ids
    )

    ok_b = (
        n_cl == n
        and n_cl_ids == n
        and no_duplicates
        and nulls == 0
    )

    print(
        f"B. assignments={n_cl} | "
        f"distinct customer_id={n_cl_ids} | "
        f"duplicates={not no_duplicates} | "
        f"nulls={nulls} -> {ok_b}"
    )


    # --------------------------------------------------------
    # C. CUSTOMER COVERAGE
    # --------------------------------------------------------

    missing = (
        cf
        .select("customer_id")
        .join(
            cl.select("customer_id"),
            "customer_id",
            "left_anti"
        )
        .count()
    )

    extra = (
        cl
        .select("customer_id")
        .join(
            cf.select("customer_id"),
            "customer_id",
            "left_anti"
        )
        .count()
    )

    ok_c = (
        missing == 0
        and extra == 0
    )

    print(
        f"C. missing customers={missing} | "
        f"unknown assignments={extra} -> {ok_c}"
    )


    # --------------------------------------------------------
    # D. CLUSTER IDS AND SIZES
    # --------------------------------------------------------

    expected_ids = set(
        range(k)
    )

    actual_ids = set(
        sizes.keys()
    )

    summary_sizes = [
        int(x)
        for x in model.summary.clusterSizes
    ]

    ok_d = (
        actual_ids == expected_ids
        and all(
            v > 0
            for v in sizes.values()
        )
        and sum(sizes.values()) == n
        and [
            sizes[c]
            for c in range(k)
        ] == summary_sizes
    )

    print(
        f"D. cluster IDs exactly "
        f"0..{k - 1}, non-empty, "
        f"correct total, match training "
        f"summary -> {ok_d}"
    )


    # --------------------------------------------------------
    # E. CONVERGENCE
    # --------------------------------------------------------

    ok_e = (
        n_iter < MAX_ITER
    )

    print(
        f"E. converged before iteration cap "
        f"({n_iter} < {MAX_ITER}) -> {ok_e}"
    )


    # --------------------------------------------------------
    # F. CLUSTER CENTER VALIDATION
    # --------------------------------------------------------

    centers = [
        np.array(center)
        for center in model.clusterCenters()
    ]

    agg_exprs = [
        F.avg(feature).alias(
            f"a{i}"
        )
        for i, feature in enumerate(
            selected
        )
    ]

    avg_rows = (
        cl
        .join(
            cf,
            "customer_id"
        )
        .groupBy("cluster")
        .agg(*agg_exprs)
        .collect()
    )

    avg = {
        int(r["cluster"]): r
        for r in avg_rows
    }

    diffs = []

    for c in range(k):

        for i in range(
            len(selected)
        ):

            center_unscaled = (
                centers[c][i]
                * cfg["scaler"]["std"][i]
                + cfg["scaler"]["mean"][i]
            )

            actual_average = (
                avg[c][f"a{i}"]
            )

            diffs.append(
                abs(
                    center_unscaled
                    - actual_average
                )
            )

    max_diff = max(
        diffs
    )

    ok_f = (
        max_diff < CHECK_TOL
    )

    print(
        f"F. unscaled centers vs actual "
        f"cluster averages: "
        f"max difference="
        f"{max_diff:.2e} -> {ok_f}"
    )


    # --------------------------------------------------------
    # G. MODEL RELOAD VALIDATION
    # --------------------------------------------------------

    loaded = KMeansModel.load(
        MODEL_PATH
    )

    center_difference = max(
        float(
            np.max(
                np.abs(
                    a - np.array(b)
                )
            )
        )
        for a, b in zip(
            centers,
            loaded.clusterCenters()
        )
    )

    ok_g = (
        center_difference < 1e-12
        and loaded.getK() == k
    )

    print(
        f"G. saved model reload: "
        f"max center difference="
        f"{center_difference:.2e} -> {ok_g}"
    )


    # --------------------------------------------------------
    # H. HIVE VALIDATION
    # --------------------------------------------------------

    n_hive = (
        spark
        .table(
            "internet_usage.customer_clusters"
        )
        .count()
    )

    ok_h = (
        n_hive == n
    )

    print(
        f"H. HDFS rows={n_cl} | "
        f"Hive rows={n_hive} -> {ok_h}"
    )


    # ========================================================
    # SECTION 8
    # SAVE FINAL MODEL INFORMATION
    # ========================================================

    section(
        8,
        "SAVE FINAL MODEL INFORMATION"
    )

    INFO_PATH.write_text(
        json.dumps(
            {
                "k": k,
                "suggested_k_by_rules": suggested,
                "seed": SEED,
                "max_iter": MAX_ITER,
                "iterations": n_iter,
                "wssse": wssse,
                "silhouette": silhouette,
                "selected_features": selected,
                "cluster_sizes": [
                    sizes[c]
                    for c in range(k)
                ],
                "centers_unscaled": {
                    str(c): {
                        selected[i]: float(
                            centers[c][i]
                            * cfg["scaler"]["std"][i]
                            + cfg["scaler"]["mean"][i]
                        )
                        for i in range(
                            len(selected)
                        )
                    }
                    for c in range(k)
                },
                "model_path": MODEL_PATH,
                "clusters_path": CLUSTERS_PATH,
                "stage15b_selected_k": int(
                    sweep["selected_k"]
                ),
                "stage15b_mean_silhouette": (
                    float(
                        summary_row["mean_silhouette"]
                    )
                    if summary_row
                    else None
                ),
                "stage15b_mean_pairwise_ari": (
                    float(
                        summary_row["mean_pairwise_ari"]
                    )
                    if summary_row
                    else None
                ),
                "note": (
                    "Cluster IDs are arbitrary labels. "
                    "Names are assigned only after "
                    "profile review."
                )
            },
            indent=2
        )
    )

    print(
        "Final information saved:",
        INFO_PATH
    )


    # ========================================================
    # FINAL RESULT
    # ========================================================

    all_checks = [
        ok_a,
        ok_b,
        ok_c,
        ok_d,
        ok_e,
        ok_f,
        ok_g,
        ok_h,
    ]

    print()
    print("=" * 70)

    if all(all_checks):

        print(
            "STAGE 15C CHECKS: PASSED"
        )

    else:

        print(
            "STAGE 15C CHECKS: FAILED"
        )

    print("=" * 70)


finally:

    if feats is not None:
        feats.unpersist()

    spark.stop()

