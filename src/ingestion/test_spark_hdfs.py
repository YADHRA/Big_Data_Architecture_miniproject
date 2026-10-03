"""Stage 6: verify Spark can read the raw Parquet file from HDFS."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark, RAW_COMPAT_PATH

spark = get_spark("Stage6-Test")

print("\n===== SPARK INFO =====")
print("Spark version:", spark.version)
print("Master       :", spark.sparkContext.master)
print("Reading from :", RAW_COMPAT_PATH)

df = spark.read.parquet(RAW_COMPAT_PATH)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== SAMPLE (5 rows) =====")
df.show(5, truncate=False)

print("\n===== ROW COUNT (action: scans the data) =====")
print("rows:", df.count())

print("\n===== PARTITIONS =====")
print("partitions:", df.rdd.getNumPartitions())

print("\n===== TIMESTAMP RANGE =====")
df.agg(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
).show(truncate=False)

spark.stop()
