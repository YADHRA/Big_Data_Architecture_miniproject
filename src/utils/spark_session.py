"""Shared SparkSession builder and HDFS paths for the whole project."""
from pyspark.sql import SparkSession

HDFS_BASE = "hdfs://localhost:9000/internet_usage"
RAW_PATH = f"{HDFS_BASE}/raw/application_data_consumption_breakdown.parquet"
RAW_COMPAT_PATH = f"{HDFS_BASE}/raw_compat/application_data_consumption_breakdown_us.parquet"
CLEANED_PATH = f"{HDFS_BASE}/cleaned"
FEATURES_PATH = f"{HDFS_BASE}/features"
RESULTS_PATH = f"{HDFS_BASE}/results"


def get_spark(app_name: str = "InternetUsageAnalytics") -> SparkSession:
    """Local-mode Spark sized for a 7.6 GiB WSL machine."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
