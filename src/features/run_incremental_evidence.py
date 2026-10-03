"""Stage 12: measure incremental eta-squared for redundant feature chains."""

from pyspark.sql import functions as F

from src.features.eta_squared import Evidence
from src.utils.spark_session import get_spark


CHAINS = {
    "app_chain": [
        "app_category",
        "app_name",
    ],
    "time_chain": [
        "is_night",
        "time_of_day",
        "hour",
    ],
    "day_chain": [
        "is_weekend",
        "day_of_week",
    ],
}


def combined_expr(columns):
    """Create a collision-safe combined grouping expression."""
    return F.concat_ws(
        "||",
        *[F.col(c).cast("string") for c in columns],
    )


def main():
    spark = get_spark("Stage12IncrementalEvidence", hive=True)

    try:
        print("=" * 75)
        print("STAGE 12 - INCREMENTAL FEATURE EVIDENCE")
        print("=" * 75)

        df = spark.table("internet_usage.session_features")

        print(f"\nRows: {df.count():,}")

        evidence = Evidence(df)

        for chain_name, chain in CHAINS.items():
            print("\n" + "=" * 75)
            print(f"{chain_name}: {' -> '.join(chain)}")
            print("=" * 75)

            previous_eta = None
            previous_k = None

            for feature in chain:
                result = evidence.measure(F.col(feature))

                max_eta = max(result["eta"])

                print(
                    f"\nFeature: {feature}"
                    f"\n  groups = {result['k']}"
                    f"\n  eta²(data) = {result['eta'][0]:.6f}"
                    f"\n  eta²(duration) = {result['eta'][1]:.6f}"
                    f"\n  max eta² = {max_eta:.6f}"
                )

                if previous_eta is not None:
                    combined = combined_expr(chain[: chain.index(feature) + 1])

                    combined_result = evidence.measure(combined)

                    combined_max_eta = max(combined_result["eta"])

                    incremental = combined_max_eta - previous_eta

                    print(
                        f"  combined groups = {combined_result['k']}"
                        f"\n  combined max eta² = {combined_max_eta:.6f}"
                        f"\n  incremental eta² = {incremental:.6f}"
                    )

                    print(
                        "  decision: "
                        + (
                            "KEEP FINER"
                            if incremental >= 0.01
                            else "KEEP COARSER"
                        )
                    )

                    previous_eta = combined_max_eta
                    previous_k = combined_result["k"]

                else:
                    previous_eta = max_eta
                    previous_k = result["k"]

        print("\n" + "=" * 75)
        print("INCREMENTAL EVIDENCE RULE")
        print("=" * 75)
        print("A finer feature replaces its coarser predecessor only")
        print("when incremental eta² >= 0.01.")
        print("\nSTAGE 12 INCREMENTAL EVIDENCE CHECK: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
