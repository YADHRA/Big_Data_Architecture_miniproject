"""Stage 10: a FRESH process that registers nothing and queries the table by name only.
If this works, the table definitions persisted in the Hive metastore."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.spark_session import get_spark


spark = get_spark("Stage10-PersistenceCheck", hive=True)

print("\n===== FRESH SESSION: tables found in the metastore =====")
spark.sql("SHOW TABLES IN internet_usage").show(truncate=False)

print("===== QUERY BY NAME (no path, no schema given) =====")

spark.sql("""
    SELECT
        operator,
        COUNT(*) AS sessions
    FROM internet_usage.cleaned_usage
    GROUP BY operator
    ORDER BY sessions DESC
""").show(truncate=False)

spark.stop()
