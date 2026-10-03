"""Shared SparkSession builder and HDFS paths for the whole project."""
from pathlib import Path
from pyspark.sql import SparkSession

HDFS_BASE = "hdfs://localhost:9000/internet_usage"
RAW_PATH = f"{HDFS_BASE}/raw/application_data_consumption_breakdown.parquet"
RAW_COMPAT_PATH = f"{HDFS_BASE}/raw_compat/application_data_consumption_breakdown_us.parquet"
CLEANED_PATH = f"{HDFS_BASE}/cleaned"
FEATURES_PATH = f"{HDFS_BASE}/features"
RESULTS_PATH = f"{HDFS_BASE}/results"

METASTORE_DIR = Path.home() / "hive_metastore"
WAREHOUSE_DIR = "hdfs://localhost:9000/user/hive/warehouse"


def get_spark(
    app_name: str = "InternetUsageAnalytics",
    hive: bool = False,
) -> SparkSession:
    """Build a local SparkSession, optionally with Hive support."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if hive:
        METASTORE_DIR.mkdir(parents=True, exist_ok=True)

        builder = (
            builder
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={METASTORE_DIR}/metastore_db;create=true",
            )
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dderby.stream.error.file={METASTORE_DIR}/derby.log",
            )
            .enableHiveSupport()
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
