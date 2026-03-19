"""
tests/unit/test_spark_processing.py
-------------------------------------
Unit tests for event parsing, deduplication, and aggregation logic.
Uses a local SparkSession (no Kafka required).
"""

import pytest
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Fixtures ──────────────────────────────────

@pytest.fixture
def sample_orders():
    return [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-A",
            "customer_segment": "premium",
            "product_id": "PROD-001",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 299.99,
            "order_total_amount": 599.98,
            "payment_method": "credit_card",
            "status": "placed",
            "currency": "USD",
            "event_time": "2024-06-01T10:00:00+00:00",
        },
        {
            "order_id": "ORD-002",
            "customer_id": "CUST-B",
            "customer_segment": "standard",
            "product_id": "PROD-002",
            "category": "Apparel",
            "quantity": 1,
            "unit_price": 49.99,
            "order_total_amount": 49.99,
            "payment_method": "paypal",
            "status": "placed",
            "currency": "USD",
            "event_time": "2024-06-01T10:01:00+00:00",
        },
        # Duplicate — should be dropped
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-A",
            "customer_segment": "premium",
            "product_id": "PROD-001",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 299.99,
            "order_total_amount": 599.98,
            "payment_method": "credit_card",
            "status": "placed",
            "currency": "USD",
            "event_time": "2024-06-01T10:00:00+00:00",
        },
    ]


# ── Tests ─────────────────────────────────────

class TestOrderParsing:

    def test_order_count_after_dedup(self, spark, sample_orders):
        df = spark.createDataFrame(sample_orders)
        deduped = df.dropDuplicates(["order_id"])
        assert deduped.count() == 2, "Deduplication should remove the duplicate order"

    def test_line_revenue_calculation(self, spark, sample_orders):
        df = spark.createDataFrame(sample_orders).dropDuplicates(["order_id"])
        df = df.withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        rows = {r["order_id"]: r["line_revenue"] for r in df.collect()}
        assert abs(rows["ORD-001"] - 599.98) < 0.01
        assert abs(rows["ORD-002"] - 49.99) < 0.01

    def test_event_ts_parsing(self, spark, sample_orders):
        df = spark.createDataFrame(sample_orders).dropDuplicates(["order_id"])
        df = df.withColumn("event_ts", F.to_timestamp("event_time"))
        null_count = df.filter(F.col("event_ts").isNull()).count()
        assert null_count == 0, "All event_time strings should parse to valid timestamps"

    def test_null_filtering(self, spark):
        dirty = [
            {"order_id": None, "customer_id": "CUST-A", "quantity": 1, "unit_price": 10.0},
            {"order_id": "ORD-X", "customer_id": None, "quantity": 1, "unit_price": 10.0},
            {"order_id": "ORD-Y", "customer_id": "CUST-B", "quantity": 0, "unit_price": 10.0},  # invalid qty
            {"order_id": "ORD-Z", "customer_id": "CUST-C", "quantity": 2, "unit_price": 5.0},   # valid
        ]
        df = spark.createDataFrame(dirty)
        valid = df.filter(
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & (F.col("quantity") > 0)
        )
        assert valid.count() == 1
        assert valid.collect()[0]["order_id"] == "ORD-Z"


class TestAggregations:

    def test_revenue_by_category(self, spark, sample_orders):
        df = spark.createDataFrame(sample_orders).dropDuplicates(["order_id"])
        df = df.withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        agg = df.groupBy("category").agg(F.sum("line_revenue").alias("total_revenue"))
        result = {r["category"]: r["total_revenue"] for r in agg.collect()}
        assert abs(result["Electronics"] - 599.98) < 0.01
        assert abs(result["Apparel"] - 49.99) < 0.01

    def test_order_count_by_segment(self, spark, sample_orders):
        df = spark.createDataFrame(sample_orders).dropDuplicates(["order_id"])
        agg = df.groupBy("customer_segment").agg(F.count("order_id").alias("orders"))
        result = {r["customer_segment"]: r["orders"] for r in agg.collect()}
        assert result["premium"] == 1
        assert result["standard"] == 1


class TestDataQuality:

    def test_currency_validation(self, spark):
        valid_currencies = {"USD", "EUR", "GBP", "CAD"}
        data = [
            {"order_id": "A", "currency": "USD"},
            {"order_id": "B", "currency": "XYZ"},   # invalid
            {"order_id": "C", "currency": "EUR"},
        ]
        df = spark.createDataFrame(data)
        invalid = df.filter(~F.col("currency").isin(list(valid_currencies)))
        assert invalid.count() == 1
        assert invalid.collect()[0]["order_id"] == "B"

    def test_unit_price_bounds(self, spark):
        data = [
            {"order_id": "A", "unit_price": 10.0},
            {"order_id": "B", "unit_price": -5.0},   # negative
            {"order_id": "C", "unit_price": 0.0},    # zero
            {"order_id": "D", "unit_price": 999.99},
        ]
        df = spark.createDataFrame(data)
        valid = df.filter(F.col("unit_price") > 0)
        assert valid.count() == 2
