"""Stage 12: measure eta-squared evidence for candidate session features."""

from pyspark.sql import functions as F

from src.features.eta_squared import Evidence
from src.utils.spark_session import get_spark


CANDIDATES = [
    "app_name",
    "app_category",
    "hour",
    "time_of_day",
    "is_night",
    "day_of_week",
    "is_weekend",
    "operator",
    "network_type",
]


def main():
    spark = get_spark("Stage12FeatureEvidence", hive=True)

    try:
        print("=" * 70)
        print("STAGE 12 - FEATURE EVIDENCE")
        print("=" * 70)

        print("\nReading Hive table: internet_usage.session_features")

        df = spark.table("internet_usage.session_features")

        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")

        evidence = Evidence(df)

        print("\nOverall statistics:")
        for y in evidence.Y if hasattr(evidence, "Y") else [
            "data_consumed_mb",
            "session_duration_minutes",
        ]:
            print(
                f"  {y}: "
                f"mean={evidence.overall[f'mean_{y}']:.6f}, "
                f"variance={evidence.overall[f'var_{y}']:.6f}"
            )

        print("\n" + "=" * 70)
        print("ETA-SQUARED RESULTS")
        print("=" * 70)

        results = []

        for feature in CANDIDATES:
            result = evidence.measure(F.col(feature))

            eta_data = result["eta"][0]
            eta_duration = result["eta"][1]

            results.append(
                (
                    feature,
                    result["k"],
                    eta_data,
                    eta_duration,
                    max(eta_data, eta_duration),
                )
            )

        print(
            f"{'Feature':<18}"
            f"{'Groups':>8}"
            f"{'η² Data':>14}"
            f"{'η² Duration':>16}"
            f"{'Max η²':>12}"
            f"{'Candidate':>12}"
        )

        print("-" * 80)

        for feature, k, eta_data, eta_duration, max_eta in results:
            candidate = "YES" if max_eta >= 0.01 else "NO"

            print(
                f"{feature:<18}"
                f"{k:>8}"
                f"{eta_data:>14.6f}"
                f"{eta_duration:>16.6f}"
                f"{max_eta:>12.6f}"
                f"{candidate:>12}"
            )

        print("\n" + "=" * 70)
        print("RULE")
        print("=" * 70)
        print("Candidate if η² >= 0.01 for data consumption OR duration.")

        print("\nSTAGE 12 FEATURE EVIDENCE CHECK: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
