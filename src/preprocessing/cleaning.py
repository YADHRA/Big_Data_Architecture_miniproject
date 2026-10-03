"""Stage 7: reusable cleaning and validation rules (also used by Stage 8).
Cleaning is kept separate from anomaly detection: extreme usage values are NEVER removed here."""
from functools import reduce
from pyspark.sql import DataFrame, functions as F

EXPECTED_SCHEMA = {
    "session_id": "string",
    "timestamp": "timestamp_ntz",
    "customer_id": "string",
    "operator": "string",
    "app_name": "string",
    "app_category": "string",
    "data_consumed_mb": "double",
    "session_duration_minutes": "bigint",
    "network_type": "string",
    "time_of_day": "string",
}

STRING_COLS = [c for c, t in EXPECTED_SCHEMA.items() if t == "string"]

VALID_OPERATORS = ["MTN", "Airtel", "Glo", "9mobile"]
VALID_NETWORKS = ["2G", "3G", "4G", "5G"]

APP_TO_CATEGORY = {
    "WhatsApp": "social",
    "Facebook": "social",
    "Instagram": "social",
    "Telegram": "social",
    "Twitter/X": "social",
    "YouTube": "streaming",
    "TikTok": "streaming",
    "Other": "other",
}

MIN_TS = "2000-01-01 00:00:00"
MAX_TS = "2026-12-31 23:59:59"

MAX_SESSION_MINUTES = 1440  # 24 hours: physical limit, not a statistical one


def _all(conds):
    return reduce(lambda a, b: a & b, conds)


def _any(conds):
    return reduce(lambda a, b: a | b, conds)


def standardize(df: DataFrame) -> DataFrame:
    """Trim whitespace on string columns; keep column order and original capitalization."""
    return df.select(*[
        F.trim(F.col(c)).alias(c) if c in STRING_COLS else F.col(c)
        for c in EXPECTED_SCHEMA
    ])


def build_rules() -> dict:
    """Each rule is a Column that is True when the row PASSES. Null counts as failing."""
    ts = F.col("timestamp")
    mb = F.col("data_consumed_mb")
    dur = F.col("session_duration_minutes")
    hour = F.hour(ts)

    expected_tod = (
        F.when((hour >= 22) | (hour <= 4), "night")
        .when(hour <= 11, "morning")
        .when(hour <= 16, "afternoon")
        .otherwise("evening")
    )

    pair_ok = _any([
        (F.col("app_name") == app) & (F.col("app_category") == cat)
        for app, cat in APP_TO_CATEGORY.items()
    ])

    rules = {
        "no_nulls_or_blanks": _all(
            [F.col(c).isNotNull() for c in EXPECTED_SCHEMA]
            + [F.length(F.col(c)) > 0 for c in STRING_COLS]
        ),

        "id_format": (
            F.col("session_id").rlike(r"^SES[0-9]{8}$")
            & F.col("customer_id").rlike(r"^CUST[0-9]{7}$")
        ),

        "timestamp_range": (
            (ts >= F.lit(MIN_TS).cast("timestamp_ntz"))
            & (ts <= F.lit(MAX_TS).cast("timestamp_ntz"))
        ),

        "data_positive_finite": (
            (mb > 0)
            & (~F.isnan(mb))
            & (mb < F.lit(float("inf")))
        ),

        "duration_valid": (
            (dur >= 1)
            & (dur <= MAX_SESSION_MINUTES)
        ),

        "operator_known": F.col("operator").isin(VALID_OPERATORS),

        "network_known": F.col("network_type").isin(VALID_NETWORKS),

        "app_category_consistent": pair_ok,

        "time_of_day_consistent": (
            F.col("time_of_day") == expected_tod
        ),
    }

    # Null -> False, so a row with a missing value can never pass a rule by accident.
    return {
        name: F.coalesce(cond, F.lit(False))
        for name, cond in rules.items()
    }


def add_validation(df: DataFrame) -> DataFrame:
    """Adds ok_<rule> columns, is_valid, and reject_reason."""
    rules = build_rules()

    for name, cond in rules.items():
        df = df.withColumn(f"ok_{name}", cond)

    ok_cols = [f"ok_{n}" for n in rules]

    df = df.withColumn(
        "is_valid",
        _all([F.col(c) for c in ok_cols])
    )

    failed = F.array(*[
        F.when(
            ~F.col(f"ok_{n}"),
            F.lit(n)
        )
        for n in rules
    ])

    return df.withColumn(
        "reject_reason",
        F.concat_ws(",", failed)
    )
