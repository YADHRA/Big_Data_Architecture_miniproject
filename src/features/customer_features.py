"""Stage 14: reusable customer-level aggregation.

customer_id is used ONLY as the grouping key and is never an ML feature.
"""

from pyspark.sql import DataFrame, functions as F


MIN_SESSIONS = 2

FEATURE_COLS = [
    "total_sessions",
    "total_data_mb",
    "avg_data_mb",
    "avg_session_duration_min",
    "max_session_data_mb",
    "unique_app_count",
    "unique_network_count",
    "weekend_sessions",
    "night_sessions",
]


def repeat_customer_sessions(
    sessions: DataFrame,
    min_sessions: int = MIN_SESSIONS,
) -> DataFrame:
    """Return sessions belonging to repeat customers."""

    ids = (
        sessions
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") >= min_sessions)
        .select("customer_id")
    )

    return sessions.join(ids, "customer_id", "left_semi")


def build_customer_features(sessions: DataFrame) -> DataFrame:
    """Build one feature row per customer with at least 2 sessions."""

    return (
        repeat_customer_sessions(sessions)
        .groupBy("customer_id")
        .agg(
            F.count("*").cast("int").alias("total_sessions"),
            F.sum("data_consumed_mb").alias("total_data_mb"),
            F.avg("data_consumed_mb").alias("avg_data_mb"),
            F.avg("session_duration_minutes").alias(
                "avg_session_duration_min"
            ),
            F.max("data_consumed_mb").alias("max_session_data_mb"),
            F.countDistinct("app_name").cast("int").alias(
                "unique_app_count"
            ),
            F.countDistinct("network_type").cast("int").alias(
                "unique_network_count"
            ),
            F.sum("is_weekend").cast("int").alias("weekend_sessions"),
            F.sum("is_night").cast("int").alias("night_sessions"),
        )
    )
