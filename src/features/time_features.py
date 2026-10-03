"""Stage 11: reusable time-feature logic derived from the session timestamp."""
from pyspark.sql import DataFrame, functions as F

TIME_FEATURES = [
    "usage_date",
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "is_weekend",
    "is_night",
]


def add_time_features(df: DataFrame) -> DataFrame:
    """Add validated time features without changing the original columns."""
    ts = F.col("timestamp")  # timestamp_ntz: no time-zone shifting
    hour = F.hour(ts)

    # Spark dayofweek: 1=Sunday ... 7=Saturday.
    # Convert to ISO: 1=Monday ... 7=Sunday.
    dow = ((F.dayofweek(ts) + 5) % 7) + 1

    return (
        df.withColumn("usage_date", F.to_date(ts))
        .withColumn("hour", hour)
        .withColumn("day_of_week", dow)
        .withColumn("day_of_month", F.dayofmonth(ts))
        .withColumn("month", F.month(ts))
        .withColumn("is_weekend", (dow >= 6).cast("int"))
        # Dataset-validated definition: night = 22:00-04:59.
        .withColumn(
            "is_night",
            ((hour >= 22) | (hour <= 4)).cast("int"),
        )
    )
