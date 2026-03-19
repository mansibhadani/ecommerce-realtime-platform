"""
storage/iceberg_setup.py
-------------------------
Creates Apache Iceberg tables on S3 and demonstrates key features:
  - Schema evolution
  - Partition spec (year/month/day)
  - Time-travel / snapshot queries
  - Snapshot rollback

Run:
    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
        storage/iceberg_setup.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://your-bucket/iceberg")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergSetup")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


def create_tables(spark: SparkSession):
    print("Creating Iceberg tables...")

    # Orders table — partitioned by year/month/day for efficient pruning
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.ecommerce.orders (
            order_id            STRING NOT NULL,
            customer_id         STRING NOT NULL,
            customer_segment    STRING,
            product_id          STRING NOT NULL,
            product_name        STRING,
            category            STRING,
            quantity            INT,
            unit_price          DOUBLE,
            line_revenue        DOUBLE,
            order_total_amount  DOUBLE,
            payment_method      STRING,
            order_status        STRING,
            currency            STRING,
            event_ts            TIMESTAMP,
            order_date          DATE,
            order_hour          TIMESTAMP,
            year                INT,
            month               INT,
            day                 INT
        )
        USING iceberg
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES (
            'write.target-file-size-bytes'      = '134217728',
            'write.parquet.compression-codec'   = 'snappy',
            'history.expire.min-snapshots-to-keep' = '10',
            'history.expire.max-snapshot-age-ms' = '604800000'
        )
    """)

    # Clickstream — high volume, partition more granularly
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.ecommerce.clickstream (
            event_id        STRING NOT NULL,
            session_id      STRING,
            customer_id     STRING,
            product_id      STRING,
            page_type       STRING,
            action          STRING,
            referrer        STRING,
            device          STRING,
            duration_seconds INT,
            event_ts        TIMESTAMP,
            year            INT,
            month           INT,
            day             INT
        )
        USING iceberg
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '268435456',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)

    # Reviews raw — for NLP pipeline consumption
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.ecommerce.reviews_raw (
            review_id           STRING NOT NULL,
            customer_id         STRING,
            product_id          STRING,
            rating              INT,
            title               STRING,
            body                STRING,
            verified_purchase   BOOLEAN,
            helpful_votes       INT,
            event_ts            TIMESTAMP,
            year                INT,
            month               INT,
            day                 INT
        )
        USING iceberg
        PARTITIONED BY (year, month, day)
    """)

    print("Tables created successfully.")


def demonstrate_time_travel(spark: SparkSession):
    """
    Shows Iceberg's time-travel and snapshot features — a key talking
    point in FAANG interviews about data lake reliability.
    """
    print("\n=== Iceberg Time-Travel Demo ===\n")

    # 1. List all snapshots
    print("1. Snapshot history:")
    spark.sql("SELECT snapshot_id, committed_at, operation FROM iceberg.ecommerce.orders.snapshots") \
        .show(truncate=False)

    # 2. Query data AS OF a specific snapshot (rollback scenario)
    spark.sql("""
        SELECT *
        FROM iceberg.ecommerce.orders
        TIMESTAMP AS OF '2024-06-01 00:00:00'
        LIMIT 5
    """).show()

    # 3. Query AS OF snapshot ID (useful in pipelines for idempotent reads)
    snapshots = spark.sql(
        "SELECT snapshot_id FROM iceberg.ecommerce.orders.snapshots ORDER BY committed_at LIMIT 1"
    ).collect()
    if snapshots:
        snapshot_id = snapshots[0]["snapshot_id"]
        print(f"2. Reading snapshot {snapshot_id}:")
        spark.sql(f"""
            SELECT COUNT(*) AS row_count
            FROM iceberg.ecommerce.orders
            VERSION AS OF {snapshot_id}
        """).show()

    # 4. Show partition stats — great for understanding data skew
    print("3. Partition statistics:")
    spark.sql("""
        SELECT partition, record_count, file_count, total_size
        FROM iceberg.ecommerce.orders.partitions
        ORDER BY record_count DESC
        LIMIT 10
    """).show()

    # 5. Show file-level metadata — useful for debugging
    print("4. Data file distribution:")
    spark.sql("""
        SELECT file_format, COUNT(*) AS file_count, 
               SUM(record_count) AS total_records,
               ROUND(SUM(file_size_in_bytes) / 1048576.0, 2) AS total_size_mb
        FROM iceberg.ecommerce.orders.files
        GROUP BY file_format
    """).show()


def expire_old_snapshots(spark: SparkSession, retain_last_n: int = 5):
    """Expire old snapshots to control storage costs."""
    print(f"\nExpiring snapshots older than 7 days (keeping at least {retain_last_n})...")
    spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => 'iceberg.ecommerce.orders',
            older_than => TIMESTAMP '2024-01-01 00:00:00',
            retain_last => {retain_last_n}
        )
    """).show()


def optimize_table(spark: SparkSession):
    """Compact small files — crucial for query performance after high-frequency streaming writes."""
    print("\nCompacting small files (rewrite_data_files)...")
    spark.sql("""
        CALL iceberg.system.rewrite_data_files(
            table => 'iceberg.ecommerce.orders',
            strategy => 'binpack',
            options => map('target-file-size-bytes', '134217728')
        )
    """).show()

    print("Rewriting manifests for faster planning...")
    spark.sql("""
        CALL iceberg.system.rewrite_manifests('iceberg.ecommerce.orders')
    """).show()


if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    create_tables(spark)
    demonstrate_time_travel(spark)
    optimize_table(spark)

    spark.stop()
    print("\nIceberg setup complete.")
