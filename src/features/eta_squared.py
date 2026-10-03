"""Stage 12 helper: eta-squared evidence for session-level ML features."""

from pyspark.sql import Column, DataFrame, functions as F


Y = [
    "data_consumed_mb",
    "session_duration_minutes",
]


class Evidence:
    """Calculate eta-squared for candidate grouping expressions."""

    def __init__(self, df: DataFrame):
        self.df = df
        self.n = df.count()

        agg = []

        for y in Y:
            agg.extend(
                [
                    F.avg(y).alias(f"mean_{y}"),
                    F.var_pop(y).alias(f"var_{y}"),
                ]
            )

        self.overall = df.agg(*agg).collect()[0].asDict()

    def measure(self, expr: Column) -> dict:
        """Return eta-squared for both target variables."""

        grouped = (
            self.df
            .groupBy(expr.alias("group"))
            .agg(
                F.count("*").alias("n"),
                F.avg(Y[0]).alias("mean_0"),
                F.avg(Y[1]).alias("mean_1"),
            )
            .collect()
        )

        eta_values = []

        for i, y in enumerate(Y):
            ss_between = sum(
                row["n"]
                * (row[f"mean_{i}"] - self.overall[f"mean_{y}"]) ** 2
                for row in grouped
            )

            denominator = self.n * self.overall[f"var_{y}"]

            eta_values.append(
                ss_between / denominator
                if denominator != 0
                else 0.0
            )

        return {
            "k": len(grouped),
            "eta": eta_values,
        }
