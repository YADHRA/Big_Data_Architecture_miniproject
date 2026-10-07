from pathlib import Path
import json
import math

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark


# ============================================================
# PATHS
# ============================================================

CUSTOMER_FEATURE_PATH = (
    "hdfs://localhost:9000/internet_usage/features/customer_features"
)

CUSTOMER_ML_FEATURE_PATH = (
    "hdfs://localhost:9000/internet_usage/features/customer_ml_features"
)

CUSTOMER_CLUSTER_PATH = (
    "hdfs://localhost:9000/internet_usage/results/customer_clusters_k3"
)

OUTPUT_PATH = (
    "hdfs://localhost:9000/internet_usage/results/customer_cluster_profile"
)

CONFIG_PATH = Path(
    "configs/customer_kmeans_info.json"
)

REPORT_PATH = Path(
    "docs/stage15_customer_cluster_profile.json"
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def clean_number(value):
    """
    Convert numeric values into JSON-safe numbers.

    None, NaN and infinity are converted to None.
    """
    if value is None:
        return None

    value = float(value)

    if math.isnan(value) or math.isinf(value):
        return None

    return value


def round_dict(obj, digits=4):
    """
    Recursively round numbers in dictionaries/lists
    for a clean JSON report.
    """

    if isinstance(obj, dict):
        return {
            key: round_dict(value, digits)
            for key, value in obj.items()
        }

    if isinstance(obj, list):
        return [
            round_dict(value, digits)
            for value in obj
        ]

    if isinstance(obj, float):
        return round(obj, digits)

    return obj


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("STAGE 15D: CUSTOMER CLUSTER PROFILING")
    print("=" * 70)

    # --------------------------------------------------------
    # Start Spark
    # --------------------------------------------------------

    spark = get_spark(
        app_name="InternetUsage_Stage15D_CustomerClusterProfile",
        hive=True,
    )

    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------------
    # SECTION 1: LOAD DATA
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 1: LOAD DATA")
    print("=" * 70)

    customer_features = spark.read.parquet(
        CUSTOMER_FEATURE_PATH
    )

    customer_ml_features = spark.read.parquet(
        CUSTOMER_ML_FEATURE_PATH
    )

    assignments = (
        spark.read
        .parquet(CUSTOMER_CLUSTER_PATH)
        .select(
            "customer_id",
            "cluster"
        )
    )

    customer_count = (
        customer_features
        .select("customer_id")
        .distinct()
        .count()
    )

    ml_feature_count = (
        customer_ml_features
        .select("customer_id")
        .distinct()
        .count()
    )

    assignment_count = (
        assignments
        .select("customer_id")
        .distinct()
        .count()
    )

    print(
        f"Customer feature rows: {customer_count}"
    )

    print(
        f"Customer ML feature rows: {ml_feature_count}"
    )

    print(
        f"Cluster assignment rows: {assignment_count}"
    )

    # --------------------------------------------------------
    # Validate expected customer count
    # --------------------------------------------------------

    assert customer_count == 33766, (
        f"Expected 33766 customer rows, "
        f"found {customer_count}"
    )

    assert ml_feature_count == 33766, (
        f"Expected 33766 ML feature rows, "
        f"found {ml_feature_count}"
    )

    assert assignment_count == 33766, (
        f"Expected 33766 assignment rows, "
        f"found {assignment_count}"
    )

    print("Input row-count checks: PASSED")

    # --------------------------------------------------------
    # SECTION 2: JOIN CUSTOMER FEATURES WITH CLUSTERS
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 2: JOIN CUSTOMER FEATURES WITH CLUSTERS")
    print("=" * 70)

    profile_df = (
        customer_features
        .join(
            assignments,
            on="customer_id",
            how="inner"
        )
    )

    joined_count = profile_df.count()

    print(
        f"Joined rows: {joined_count}"
    )

    assert joined_count == 33766, (
        f"Expected 33766 joined rows, "
        f"found {joined_count}"
    )

    # --------------------------------------------------------
    # Check cluster IDs
    # --------------------------------------------------------

    cluster_ids = [
        int(row["cluster"])
        for row in (
            profile_df
            .select("cluster")
            .distinct()
            .orderBy("cluster")
            .collect()
        )
    ]

    print(
        f"Cluster IDs found: {cluster_ids}"
    )

    assert cluster_ids == [0, 1, 2], (
        f"Expected cluster IDs [0, 1, 2], "
        f"found {cluster_ids}"
    )

    print("Cluster ID validation: PASSED")

    # --------------------------------------------------------
    # SECTION 3: CALCULATE CLUSTER PROFILES
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 3: CALCULATE CLUSTER PROFILES")
    print("=" * 70)

    cluster_profile = (
        profile_df
        .groupBy("cluster")
        .agg(
            F.count("*").alias(
                "customer_count"
            ),

            F.avg("total_sessions").alias(
                "avg_total_sessions"
            ),

            F.avg("total_data_mb").alias(
                "avg_total_data_mb"
            ),

            F.avg("avg_data_mb").alias(
                "avg_data_mb"
            ),

            F.avg("avg_session_duration_min").alias(
                "avg_session_duration_min"
            ),

            F.avg("max_session_data_mb").alias(
                "avg_max_session_data_mb"
            ),

            F.avg("unique_app_count").alias(
                "avg_unique_app_count"
            ),

            F.avg("unique_network_count").alias(
                "avg_unique_network_count"
            ),

            F.avg("weekend_sessions").alias(
                "avg_weekend_sessions"
            ),

            F.avg("night_sessions").alias(
                "avg_night_sessions"
            ),
        )
        .withColumn(
            "customer_share_pct",
            F.col("customer_count")
            * 100.0
            / F.lit(joined_count)
        )
        .orderBy("cluster")
    )

    cluster_profile.cache()

    rows = cluster_profile.collect()

    # --------------------------------------------------------
    # Print cluster profiles
    # --------------------------------------------------------

    for row in rows:

        print(
            f"\nCluster {row['cluster']}"
        )

        print(
            f"  Customers: "
            f"{row['customer_count']} "
            f"({row['customer_share_pct']:.2f}%)"
        )

        print(
            f"  Avg sessions: "
            f"{row['avg_total_sessions']:.2f}"
        )

        print(
            f"  Avg total data: "
            f"{row['avg_total_data_mb']:.2f} MB"
        )

        print(
            f"  Avg data/session: "
            f"{row['avg_data_mb']:.2f} MB"
        )

        print(
            f"  Avg session duration: "
            f"{row['avg_session_duration_min']:.2f} min"
        )

        print(
            f"  Avg maximum session data: "
            f"{row['avg_max_session_data_mb']:.2f} MB"
        )

        print(
            f"  Avg unique apps: "
            f"{row['avg_unique_app_count']:.2f}"
        )

        print(
            f"  Avg unique networks: "
            f"{row['avg_unique_network_count']:.2f}"
        )

        print(
            f"  Avg weekend sessions: "
            f"{row['avg_weekend_sessions']:.2f}"
        )

        print(
            f"  Avg night sessions: "
            f"{row['avg_night_sessions']:.2f}"
        )

    # --------------------------------------------------------
    # SECTION 4: BUILD PROFILE DICTIONARY
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 4: CLUSTER INTERPRETATION")
    print("=" * 70)

    profiles = {}

    for row in rows:

        profiles[int(row["cluster"])] = {

            "customer_count": int(
                row["customer_count"]
            ),

            "customer_share_pct": clean_number(
                row["customer_share_pct"]
            ),

            "avg_total_sessions": clean_number(
                row["avg_total_sessions"]
            ),

            "avg_total_data_mb": clean_number(
                row["avg_total_data_mb"]
            ),

            "avg_data_mb": clean_number(
                row["avg_data_mb"]
            ),

            "avg_session_duration_min": clean_number(
                row["avg_session_duration_min"]
            ),

            "avg_max_session_data_mb": clean_number(
                row["avg_max_session_data_mb"]
            ),

            "avg_unique_app_count": clean_number(
                row["avg_unique_app_count"]
            ),

            "avg_unique_network_count": clean_number(
                row["avg_unique_network_count"]
            ),

            "avg_weekend_sessions": clean_number(
                row["avg_weekend_sessions"]
            ),

            "avg_night_sessions": clean_number(
                row["avg_night_sessions"]
            ),
        }

    # --------------------------------------------------------
    # Evidence-based cluster identification
    #
    # We intentionally DO NOT use session duration for labels.
    #
    # Reason:
    #
    # Cluster 0 ≈ 90.27 min
    # Cluster 1 ≈ 90.95 min
    # Cluster 2 ≈ 91.59 min
    #
    # These values are very close, so calling clusters
    # "Long-Session" or "Short-Session" would be misleading.
    #
    # The strongest differentiator is average data/session,
    # with unique app count providing additional context.
    # --------------------------------------------------------

    highest_data_cluster = max(
        profiles,
        key=lambda k: profiles[k]["avg_data_mb"]
    )

    lowest_data_cluster = min(
        profiles,
        key=lambda k: profiles[k]["avg_data_mb"]
    )

    labels = {}

    for cluster_id in profiles:

        if cluster_id == highest_data_cluster:

            label = "High-Data Customers"

        elif cluster_id == lowest_data_cluster:

            label = "Low-Data Single-App Customers"

        else:

            label = "Moderate-Data Multi-App Customers"

        labels[cluster_id] = label

    # --------------------------------------------------------
    # Print evidence-based labels
    # --------------------------------------------------------

    print("\nEvidence-based cluster labels:")

    for cluster_id in sorted(profiles):

        p = profiles[cluster_id]

        print(
            f"\nCluster {cluster_id}: "
            f"{labels[cluster_id]}"
        )

        print(
            f"  Avg data/session: "
            f"{p['avg_data_mb']:.2f} MB"
        )

        print(
            f"  Avg unique apps: "
            f"{p['avg_unique_app_count']:.2f}"
        )

        print(
            f"  Avg duration: "
            f"{p['avg_session_duration_min']:.2f} min"
        )

    # --------------------------------------------------------
    # SECTION 5: SAVE PROFILE TABLE
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 5: SAVE PROFILE TABLE")
    print("=" * 70)

    # --------------------------------------------------------
    # Add cluster names to the profile DataFrame
    # --------------------------------------------------------

    profile_output = (
        cluster_profile
        .withColumn(
            "cluster_name",
            F.when(
                F.col("cluster")
                == int(highest_data_cluster),
                F.lit("High-Data Customers")
            )
            .when(
                F.col("cluster")
                == int(lowest_data_cluster),
                F.lit("Low-Data Single-App Customers")
            )
            .otherwise(
                F.lit("Moderate-Data Multi-App Customers")
            )
        )
    )

    (
        profile_output
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(
        f"Profile saved: {OUTPUT_PATH}"
    )

    # --------------------------------------------------------
    # SECTION 6: REGISTER HIVE TABLE
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 6: REGISTER HIVE TABLE")
    print("=" * 70)

    spark.sql(
        "DROP TABLE IF EXISTS "
        "internet_usage.customer_cluster_profile"
    )

    spark.sql(
        f"""
        CREATE TABLE internet_usage.customer_cluster_profile
        USING PARQUET
        LOCATION '{OUTPUT_PATH}'
        """
    )

    hive_count = (
        spark.sql(
            """
            SELECT COUNT(*)
            FROM internet_usage.customer_cluster_profile
            """
        )
        .collect()[0][0]
    )

    print(
        f"Hive table rows: {hive_count}"
    )

    assert hive_count == 3, (
        f"Expected 3 Hive rows, "
        f"found {hive_count}"
    )

    print("Hive table validation: PASSED")

    # --------------------------------------------------------
    # SECTION 7: VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 7: VALIDATION")
    print("=" * 70)

    # --------------------------------------------------------
    # A. Exactly three clusters
    # --------------------------------------------------------

    distinct_clusters = (
        profile_output
        .select("cluster")
        .distinct()
        .count()
    )

    check_a = (
        distinct_clusters == 3
    )

    print(
        f"A. Cluster count = "
        f"{distinct_clusters} -> {check_a}"
    )

    # --------------------------------------------------------
    # B. Customer totals
    # --------------------------------------------------------

    profile_customer_total = (
        profile_output
        .agg(
            F.sum("customer_count")
        )
        .collect()[0][0]
    )

    check_b = (
        profile_customer_total == 33766
    )

    print(
        f"B. Profile customer total = "
        f"{profile_customer_total} -> {check_b}"
    )

    # --------------------------------------------------------
    # C. No null cluster names
    # --------------------------------------------------------

    null_cluster_names = (
        profile_output
        .filter(
            F.col("cluster_name").isNull()
        )
        .count()
    )

    check_c = (
        null_cluster_names == 0
    )

    print(
        f"C. Null cluster names = "
        f"{null_cluster_names} -> {check_c}"
    )

    # --------------------------------------------------------
    # D. Unique cluster names
    # --------------------------------------------------------

    unique_cluster_names = (
        profile_output
        .select("cluster_name")
        .distinct()
        .count()
    )

    check_d = (
        unique_cluster_names == 3
    )

    print(
        f"D. Unique cluster names = "
        f"{unique_cluster_names} -> {check_d}"
    )

    # --------------------------------------------------------
    # E. Customer shares sum to approximately 100%
    # --------------------------------------------------------

    share_sum = (
        profile_output
        .agg(
            F.sum("customer_share_pct")
        )
        .collect()[0][0]
    )

    check_e = (
        abs(float(share_sum) - 100.0) < 1e-6
    )

    print(
        f"E. Customer share total = "
        f"{share_sum:.6f}% -> {check_e}"
    )

    # --------------------------------------------------------
    # F. Verify saved HDFS profile
    # --------------------------------------------------------

    saved_profile = (
        spark.read
        .parquet(OUTPUT_PATH)
    )

    saved_count = saved_profile.count()

    check_f = (
        saved_count == 3
    )

    print(
        f"F. Saved HDFS profile rows = "
        f"{saved_count} -> {check_f}"
    )

    # --------------------------------------------------------
    # G. Verify Hive table
    # --------------------------------------------------------

    hive_profile_count = (
        spark.sql(
            """
            SELECT COUNT(*)
            FROM internet_usage.customer_cluster_profile
            """
        )
        .collect()[0][0]
    )

    check_g = (
        hive_profile_count == 3
    )

    print(
        f"G. Hive profile rows = "
        f"{hive_profile_count} -> {check_g}"
    )

    # --------------------------------------------------------
    # H. Verify label mapping
    # --------------------------------------------------------

    saved_names = {
        row["cluster_name"]
        for row in (
            saved_profile
            .select("cluster_name")
            .collect()
        )
    }

    expected_names = {
        "High-Data Customers",
        "Low-Data Single-App Customers",
        "Moderate-Data Multi-App Customers",
    }

    check_h = (
        saved_names == expected_names
    )

    print(
        f"H. Evidence-based labels = "
        f"{check_h}"
    )

    # --------------------------------------------------------
    # Overall validation
    # --------------------------------------------------------

    all_checks = all(
        [
            check_a,
            check_b,
            check_c,
            check_d,
            check_e,
            check_f,
            check_g,
            check_h,
        ]
    )

    print("\n" + "=" * 70)

    if all_checks:

        print(
            "STAGE 15D CHECKS: PASSED"
        )

    else:

        print(
            "STAGE 15D CHECKS: FAILED"
        )

        raise AssertionError(
            "One or more Stage 15D validation checks failed."
        )

    print("=" * 70)

    # --------------------------------------------------------
    # SECTION 8: SAVE JSON REPORT
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SECTION 8: SAVE FINAL PROFILE REPORT")
    print("=" * 70)

    # --------------------------------------------------------
    # Add labels into profile JSON
    # --------------------------------------------------------

    for cluster_id in profiles:

        profiles[cluster_id][
            "cluster_name"
        ] = labels[cluster_id]

    # --------------------------------------------------------
    # Build final report
    # --------------------------------------------------------

    report = {

        "stage": "15D",

        "purpose": (
            "Profile and interpret final K=3 "
            "repeat-customer clusters"
        ),

        "input_customer_count": 33766,

        "selected_k": 3,

        "profile_path": OUTPUT_PATH,

        "hive_table": (
            "internet_usage.customer_cluster_profile"
        ),

        "cluster_labels": {
            str(cluster_id): labels[cluster_id]
            for cluster_id in sorted(labels)
        },

        "profiles": {
            str(cluster_id): profiles[cluster_id]
            for cluster_id in sorted(profiles)
        },

        "labeling_method": {

            "primary_metric": (
                "Average data consumed per session"
            ),

            "secondary_metric": (
                "Average unique applications"
            ),

            "excluded_metric_for_naming": (
                "Average session duration"
            ),

            "reason": (
                "The three clusters have very similar "
                "average session durations, so duration "
                "was not used to distinguish cluster names."
            ),
        },

        "validation": {

            "cluster_count": check_a,

            "customer_total": check_b,

            "non_null_labels": check_c,

            "unique_cluster_names": check_d,

            "customer_share_total": check_e,

            "saved_hdfs_profile": check_f,

            "hive_profile": check_g,

            "evidence_based_labels": check_h,

            "all_checks": all_checks,
        },

        "interpretation_note": (
            "Clusters are descriptive aggregate usage "
            "segments. Shuffled-session customer controls "
            "produced similar silhouette values, so these "
            "segments should not be interpreted as strong "
            "evidence of persistent individual customer "
            "behavior."
        ),
    }

    # --------------------------------------------------------
    # Round numerical values for readability
    # --------------------------------------------------------

    report = round_dict(
        report,
        digits=4
    )

    # --------------------------------------------------------
    # Ensure report directory exists
    # --------------------------------------------------------

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Save JSON
    # --------------------------------------------------------

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            report,
            f,
            indent=2,
            ensure_ascii=False
        )

    print(
        f"Profile report saved: {REPORT_PATH}"
    )

    # --------------------------------------------------------
    # FINAL SUMMARY
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("STAGE 15D FINAL SUMMARY")
    print("=" * 70)

    for cluster_id in sorted(profiles):

        p = profiles[cluster_id]

        print(
            f"\nCluster {cluster_id}: "
            f"{labels[cluster_id]}"
        )

        print(
            f"  Customers: "
            f"{p['customer_count']} "
            f"({p['customer_share_pct']:.2f}%)"
        )

        print(
            f"  Avg data/session: "
            f"{p['avg_data_mb']:.2f} MB"
        )

        print(
            f"  Avg duration: "
            f"{p['avg_session_duration_min']:.2f} min"
        )

        print(
            f"  Avg unique apps: "
            f"{p['avg_unique_app_count']:.2f}"
        )

    print("\n" + "=" * 70)
    print("STAGE 15D COMPLETED SUCCESSFULLY")
    print("=" * 70)

    spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()