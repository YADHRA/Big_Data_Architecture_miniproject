"""Stage 14 consistency analysis for repeat-customer behavior.

This script performs descriptive consistency checks on the customer/session
structure of the dataset.

It does NOT modify the dataset and does NOT make causal claims.
"""

from __future__ import annotations

import math

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark


def main() -> None:
    """Run Stage 14 customer consistency checks."""

    spark = get_spark(
        "Stage14_CustomerConsistency",
        hive=True,
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=" * 72)
        print("STAGE 14 — CUSTOMER CONSISTENCY CHECK")
        print("=" * 72)

        # ------------------------------------------------------------------
        # Load Stage 11 session-level features
        # ------------------------------------------------------------------
        sessions = spark.table(
            "internet_usage.session_features"
        )

        # ==================================================================
        # 1. CUSTOMER-ID RANGE / COLLISION HYPOTHESIS
        # ==================================================================
        print("\n1. CUSTOMER-ID RANGE / COLLISION HYPOTHESIS")

        ids = (
            sessions
            .select(
                F.substring(
                    "customer_id",
                    5,
                    20,
                )
                .cast("long")
                .alias("customer_num")
            )
        )

        id_range = (
            ids
            .agg(
                F.min("customer_num").alias("min_id"),
                F.max("customer_num").alias("max_id"),
                F.countDistinct("customer_num").alias(
                    "unique_ids"
                ),
            )
            .first()
        )

        min_id = int(id_range["min_id"])
        max_id = int(id_range["max_id"])
        unique_ids = int(id_range["unique_ids"])

        print(
            f"Numeric customer ID minimum: {min_id:,}"
        )
        print(
            f"Numeric customer ID maximum: {max_id:,}"
        )
        print(
            f"Unique numeric IDs: {unique_ids:,}"
        )

        # Total number of session records.
        n_sessions = sessions.count()

        # Size of the observed numeric ID range.
        id_space = max_id - min_id + 1

        if id_space > 0:
            expected_lambda = (
                n_sessions / id_space
            )

            poisson_expected: dict[int, float] = {}

            # Expected number of IDs appearing exactly k times
            # under a simple uniform random-collision model.
            for k in range(1, 6):
                poisson_expected[k] = (
                    id_space
                    * math.exp(-expected_lambda)
                    * expected_lambda**k
                    / math.factorial(k)
                )

            # Expected number of IDs appearing zero times.
            poisson_expected[0] = (
                id_space
                * math.exp(-expected_lambda)
            )

            print(
                f"Observed ID-space size: "
                f"{id_space:,}"
            )

            print(
                f"Poisson-style lambda: "
                f"{expected_lambda:.6f}"
            )

            print(
                "\nExpected customer counts under a "
                "uniform random-collision model:"
            )

            for k in range(1, 6):
                print(
                    f"  {k} session(s): "
                    f"{poisson_expected[k]:,.1f}"
                )

        # ------------------------------------------------------------------
        # Observed customer session-count distribution
        # ------------------------------------------------------------------
        observed = (
            sessions
            .groupBy("customer_id")
            .count()
            .groupBy("count")
            .agg(
                F.count("*").alias(
                    "customer_count"
                )
            )
            .orderBy("count")
            .collect()
        )

        print("\nObserved customer counts:")

        for row in observed:
            print(
                f"  {int(row['count'])} session(s): "
                f"{int(row['customer_count']):,}"
            )

        print(
            "\nNOTE: The Poisson calculation is only a "
            "reference model under a uniform random-ID "
            "assumption. It is not proof of how the "
            "dataset was generated."
        )

        # ==================================================================
        # 2. EXACTLY-TWO-SESSION CUSTOMER PAIRING
        # ==================================================================
        print(
            "\n2. EXACTLY-TWO-SESSION CUSTOMER PAIRING"
        )

        # Find customers with exactly two sessions.
        counts = (
            sessions
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") == 2)
            .select("customer_id")
        )

        two_sessions = sessions.join(
            counts,
            "customer_id",
            "inner",
        )

        # Order the two sessions chronologically.
        # session_id is used as a deterministic tie-breaker
        # when timestamps are identical.
        window = (
            Window
            .partitionBy("customer_id")
            .orderBy(
                F.col("timestamp"),
                F.col("session_id"),
            )
        )

        numbered_sessions = (
            two_sessions
            .withColumn(
                "rn",
                F.row_number().over(window),
            )
        )

        # Earlier session.
        first = (
            numbered_sessions
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        # Later session.
        second = (
            numbered_sessions
            .filter(F.col("rn") == 2)
            .drop("rn")
        )

        # Select the variables needed for comparison.
        a = first.select(
            "customer_id",
            F.col("data_consumed_mb").alias(
                "data_1"
            ),
            F.col(
                "session_duration_minutes"
            ).alias(
                "duration_1"
            ),
            F.col("app_name").alias(
                "app_1"
            ),
            F.col("operator").alias(
                "operator_1"
            ),
            F.col("network_type").alias(
                "network_1"
            ),
        )

        b = second.select(
            "customer_id",
            F.col("data_consumed_mb").alias(
                "data_2"
            ),
            F.col(
                "session_duration_minutes"
            ).alias(
                "duration_2"
            ),
            F.col("app_name").alias(
                "app_2"
            ),
            F.col("operator").alias(
                "operator_2"
            ),
            F.col("network_type").alias(
                "network_2"
            ),
        )

        paired = a.join(
            b,
            "customer_id",
            "inner",
        )

        total_pairs = paired.count()

        print(
            f"Exactly-two customers: "
            f"{total_pairs:,}"
        )

        # ------------------------------------------------------------------
        # Earlier/later numerical correlations
        # ------------------------------------------------------------------
        data_corr = paired.stat.corr(
            "data_1",
            "data_2",
        )

        duration_corr = paired.stat.corr(
            "duration_1",
            "duration_2",
        )

        print(
            f"Earlier/later data correlation: "
            f"{data_corr:.6f}"
        )

        print(
            f"Earlier/later duration correlation: "
            f"{duration_corr:.6f}"
        )

        # ------------------------------------------------------------------
        # Same-category/session characteristics
        # ------------------------------------------------------------------
        same_app = (
            paired
            .filter(
                F.col("app_1")
                == F.col("app_2")
            )
            .count()
        )

        same_operator = (
            paired
            .filter(
                F.col("operator_1")
                == F.col("operator_2")
            )
            .count()
        )

        same_network = (
            paired
            .filter(
                F.col("network_1")
                == F.col("network_2")
            )
            .count()
        )

        if total_pairs > 0:
            same_app_rate = (
                100 * same_app / total_pairs
            )

            same_operator_rate = (
                100
                * same_operator
                / total_pairs
            )

            same_network_rate = (
                100
                * same_network
                / total_pairs
            )
        else:
            same_app_rate = 0.0
            same_operator_rate = 0.0
            same_network_rate = 0.0

        print(
            f"Same app: "
            f"{same_app:,} "
            f"({same_app_rate:.2f}%)"
        )

        print(
            f"Same operator: "
            f"{same_operator:,} "
            f"({same_operator_rate:.2f}%)"
        )

        print(
            f"Same network: "
            f"{same_network:,} "
            f"({same_network_rate:.2f}%)"
        )

        # ------------------------------------------------------------------
        # Independence reference rates
        # ------------------------------------------------------------------
        def independence_expected(
            column: str,
        ) -> float:
            """Return expected same-value rate under independence."""

            value_counts = (
                sessions
                .groupBy(column)
                .count()
            )

            total = (
                value_counts
                .agg(F.sum("count"))
                .first()[0]
            )

            if total is None or int(total) == 0:
                return 0.0

            probabilities = [
                float(row["count"])
                / float(total)
                for row in value_counts.collect()
            ]

            return sum(
                probability**2
                for probability in probabilities
            )

        expected_app = (
            independence_expected("app_name")
        )

        expected_operator = (
            independence_expected("operator")
        )

        expected_network = (
            independence_expected("network_type")
        )

        print(
            f"Expected same-app rate under "
            f"independence: "
            f"{100 * expected_app:.2f}%"
        )

        print(
            f"Expected same-operator rate under "
            f"independence: "
            f"{100 * expected_operator:.2f}%"
        )

        print(
            f"Expected same-network rate under "
            f"independence: "
            f"{100 * expected_network:.2f}%"
        )

        # ==================================================================
        # 3. INTERPRETATION
        # ==================================================================
        print("\n3. INTERPRETATION")

        print(
            "These measurements are descriptive consistency "
            "checks only. They do not establish causality, "
            "user identity, or the dataset generation process."
        )

        print(
            "The Poisson calculation is only a reference "
            "comparison under a uniform random-ID assumption."
        )

        print(
            "Earlier/later correlations describe association "
            "between the two sessions of exactly-two-session "
            "customers; they are not causal estimates."
        )

        print("\n" + "=" * 72)
        print(
            "STAGE 14 CONSISTENCY CHECK: COMPLETE"
        )
        print("=" * 72)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()