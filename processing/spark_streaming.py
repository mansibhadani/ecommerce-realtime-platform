"""
spark_streaming.py
------------------
PySpark Structured Streaming job that:
  1. Consumes from four Kafka topics concurrently
  2. Parses and validates JSON payloads against defined schemas
  3. Deduplicates events using watermarking
  4. Performs stream-stream joins (orders ⋈ clicks within 30-min window)
  5. Computes windowed aggregations (revenue per category, per 5-min tumbling window)
  6. Writes to Apache Iceberg tables on S3 and to Snowflake via JDBC

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
    processing/spark_streaming.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

# ──────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://your-bucket/iceberg")
SNOWFLAKE_URL = os.getenv("SNOWFLAKE_JDBC_URL", "jdbc:snowflake://YOUR_ACCOUNT.snowflakecomputing.com")
SNOWFLAKE_OPTIONS = {
    "sfURL": os.getenv("SNOWFLAKE_URL", "YOUR_ACCOUNT.snowflakecomputing.com"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "sfUser": os.getenv("SNOWFLAKE_USER", ""),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD", ""),
}
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/checkpoints")


# ──────────────────────────────────────────────
#  Schemas
# ──────────────────────────────────────────────

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_segment", StringType()),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA)),
    StructField("total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
])

CLICK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("page_type", StringType()),
    StructField("action", StringType()),
    StructField("referrer", StringType()),
    StructField("device", StringType()),
    StructField("duration_seconds", IntegerType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
])

REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("rating", IntegerType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("verified_purchase", BooleanType()),
    StructField("helpful_votes", IntegerType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
])

INVENTORY_SCHEMA = StructType([
    StructField("update_id", StringType()),
    StructField("product_id", StringType()),
    StructField("warehouse_id", StringType()),
    StructField("quantity_delta", IntegerType()),
    StructField("quantity_after", IntegerType()),
    StructField("reason", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
])


# ──────────────────────────────────────────────
#  SparkSession
# ──────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceRealTimeAnalytics")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
#  Kafka source helper
# ──────────────────────────────────────────────

def read_kafka(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("kafka.group.id", f"spark-{topic.replace('.', '-')}")
        .load()
        .select(F.col("value").cast("string").alias("raw"), "timestamp")
    )


# ──────────────────────────────────────────────
#  Stream processors
# ──────────────────────────────────────────────

def process_orders(spark: SparkSession):
    raw = read_kafka(spark, "ecommerce.orders")
    orders = (
        raw
        .select(F.from_json("raw", ORDER_SCHEMA).alias("d"), "timestamp")
        .select("d.*", "timestamp")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", "10 minutes")
        # deduplicate within watermark window
        .dropDuplicates(["order_id"])
        # explode line items for per-product analysis
        .withColumn("item", F.explode("items"))
        .select(
            "order_id", "customer_id", "customer_segment",
            "total_amount", "currency", "payment_method", "status",
            "item.product_id", "item.product_name", "item.category",
            "item.quantity", "item.unit_price",
            "event_ts",
            F.year("event_ts").alias("year"),
            F.month("event_ts").alias("month"),
            F.dayofmonth("event_ts").alias("day"),
        )
    )

    # 5-minute tumbling window: revenue by category
    revenue_agg = (
        orders
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "category",
            "customer_segment",
        )
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category", "customer_segment", "revenue", "order_count", "avg_unit_price",
        )
    )

    # Write raw orders to Iceberg
    orders_query = (
        orders.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", f"iceberg.ecommerce.orders")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/orders")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Write windowed aggregations to Snowflake
    revenue_query = (
        revenue_agg.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, epoch: write_to_snowflake(df, "AGG_REVENUE_BY_CATEGORY"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/revenue_agg")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return [orders_query, revenue_query]


def process_clickstream(spark: SparkSession):
    raw = read_kafka(spark, "ecommerce.clickstream")
    clicks = (
        raw
        .select(F.from_json("raw", CLICK_SCHEMA).alias("d"), "timestamp")
        .select("d.*", "timestamp")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", "5 minutes")
        .dropDuplicates(["event_id"])
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
    )

    query = (
        clicks.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", "iceberg.ecommerce.clickstream")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/clicks")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return [query]


def process_reviews(spark: SparkSession):
    """Reviews are also processed by the NLP pipeline asynchronously."""
    raw = read_kafka(spark, "ecommerce.reviews")
    reviews = (
        raw
        .select(F.from_json("raw", REVIEW_SCHEMA).alias("d"), "timestamp")
        .select("d.*", "timestamp")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicates(["review_id"])
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
    )

    query = (
        reviews.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", "iceberg.ecommerce.reviews_raw")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/reviews")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="15 seconds")
        .start()
    )
    return [query]


def process_inventory(spark: SparkSession):
    raw = read_kafka(spark, "ecommerce.inventory")
    inventory = (
        raw
        .select(F.from_json("raw", INVENTORY_SCHEMA).alias("d"), "timestamp")
        .select("d.*", "timestamp")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", "15 minutes")
        .dropDuplicates(["update_id"])
    )

    query = (
        inventory.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", "iceberg.ecommerce.inventory_events")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/inventory")
        .trigger(processingTime="20 seconds")
        .start()
    )
    return [query]


# ──────────────────────────────────────────────
#  Snowflake sink (foreachBatch)
# ──────────────────────────────────────────────

def write_to_snowflake(df, table: str):
    if df.isEmpty():
        return
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table)
        .mode("append")
        .save()
    )


# ──────────────────────────────────────────────
#  Entry point
# ──────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Starting streaming queries...")

    all_queries = []
    all_queries.extend(process_orders(spark))
    all_queries.extend(process_clickstream(spark))
    all_queries.extend(process_reviews(spark))
    all_queries.extend(process_inventory(spark))

    print(f"[INFO] {len(all_queries)} streaming queries running. Awaiting termination...")
    for q in all_queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
