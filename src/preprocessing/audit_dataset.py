"""Stage 3: data-quality audit. READ-ONLY: reports problems, changes nothing."""
from pathlib import Path
import io
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "data" / "raw" / "application_data_consumption_breakdown.parquet"
REPORT = ROOT / "docs" / "audit_report.txt"

df = pq.read_table(PATH).to_pandas()
out = io.StringIO()


def p(*args):
    print(*args)
    print(*args, file=out)


p("=== 1. SHAPE ===")
p("rows:", len(df), "| columns:", df.shape[1])

p("\n=== 2. DATA TYPES ===")
p(df.dtypes.to_string())

p("\n=== 3. NULLS PER COLUMN ===")
p(df.isna().sum().to_string())

p("\n=== 4. EMPTY / WHITESPACE STRINGS ===")
for c in df.select_dtypes(include=["object", "string"]).columns:
    blank = (df[c].astype(str).str.strip() == "").sum()
    padded = (df[c].dropna().astype(str) != df[c].dropna().astype(str).str.strip()).sum()
    p(f"{c:15s} blank={blank}  leading/trailing spaces={padded}")

p("\n=== 5. DUPLICATES ===")
p("fully duplicate rows        :", int(df.duplicated().sum()))
p("duplicate session_id values :", int(df["session_id"].duplicated().sum()))

p("\n=== 6. IDENTIFIERS ===")
p("unique session_id  :", df["session_id"].nunique())
p("unique customer_id :", df["customer_id"].nunique())
sess = df.groupby("customer_id").size()
p("customers with 1 session   :", int((sess == 1).sum()),
  f"({(sess == 1).mean() * 100:.2f}% of customers)")
p("customers with >=2 sessions:", int((sess >= 2).sum()))
p("max sessions per customer  :", int(sess.max()))
p("session count distribution :")
p(sess.value_counts().sort_index().to_string())
p("session_id length distribution:", df["session_id"].str.len().value_counts().to_dict())
p("customer_id length distribution:", df["customer_id"].str.len().value_counts().to_dict())
p("sample session_id :", df["session_id"].iloc[0], "| sample customer_id:", df["customer_id"].iloc[0])

p("\n=== 7. TIMESTAMP ===")
p("min:", df["timestamp"].min(), "| max:", df["timestamp"].max())
p("distinct dates:", df["timestamp"].dt.date.nunique())
p("rows per date (first 5 / last 5):")
per_day = df["timestamp"].dt.date.value_counts().sort_index()
p(per_day.head(5).to_string())
p(per_day.tail(5).to_string())

p("\n=== 8. CATEGORICAL COLUMNS ===")
for c in ["operator", "app_name", "app_category", "network_type", "time_of_day"]:
    p(f"\n--- {c} (unique = {df[c].nunique()}) ---")
    p(df[c].value_counts(dropna=False).to_string())

p("\n=== 9. APP -> CATEGORY MAPPING (is each app in exactly one category?) ===")
p(df.groupby("app_name")["app_category"].nunique().to_string())
p(df.groupby(["app_category", "app_name"]).size().to_string())

p("\n=== 10. NUMERIC SUMMARY ===")
num = df[["data_consumed_mb", "session_duration_minutes"]]
p(num.describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).T.to_string())
p("data_consumed_mb <= 0          :", int((df["data_consumed_mb"] <= 0).sum()))
p("session_duration_minutes <= 0  :", int((df["session_duration_minutes"] <= 0).sum()))
p("data_consumed_mb is NaN/inf    :", int((~df["data_consumed_mb"].between(-1e18, 1e18)).sum()))

p("\n=== 11. DATA CONSUMED BY CATEGORY (why one global anomaly threshold may mislead) ===")
p(df.groupby("app_category")["data_consumed_mb"].describe(percentiles=[.5, .95, .99]).to_string())

p("\n=== 12. DATA CONSUMED BY NETWORK TYPE ===")
p(df.groupby("network_type")["data_consumed_mb"].describe(percentiles=[.5, .95]).to_string())

p("\n=== 13. IS time_of_day REDUNDANT WITH timestamp HOUR? ===")
p(pd.crosstab(df["timestamp"].dt.hour, df["time_of_day"]).to_string())

p("\n=== 14. SESSION DURATION vs DATA (correlation) ===")
p(num.corr().to_string())

REPORT.parent.mkdir(exist_ok=True)
REPORT.write_text(out.getvalue())
print(f"\nReport saved to: {REPORT}")
