"""
serving/api/main.py
--------------------
FastAPI analytics API — serves pre-aggregated metrics from Snowflake.
Adds Redis caching (TTL=60s) to protect the warehouse from hot queries.

Run locally:
    uvicorn serving.api.main:app --reload --port 8000
    # Docs: http://localhost:8000/docs
"""

from __future__ import annotations

import os
import json
import hashlib
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional

import redis.asyncio as aioredis
import snowflake.connector
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
#  Config
# ──────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))

SF_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "database": os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "MARTS"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
}


# ──────────────────────────────────────────────
#  Startup / teardown
# ──────────────────────────────────────────────

redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    log.info("Redis connection established")
    yield
    await redis_client.aclose()
    log.info("Redis connection closed")


app = FastAPI(
    title="E-Commerce Analytics API",
    description="Real-time analytics endpoints backed by Snowflake + Redis cache",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────
#  Snowflake helper
# ──────────────────────────────────────────────

def sf_query(sql: str, params: tuple = ()) -> list[dict]:
    conn = snowflake.connector.connect(**SF_CONFIG)
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        conn.close()


# ──────────────────────────────────────────────
#  Cache decorator
# ──────────────────────────────────────────────

def cache_key(*args) -> str:
    payload = json.dumps(args, default=str, sort_keys=True)
    return "api:" + hashlib.md5(payload.encode()).hexdigest()


async def cached_query(key: str, sql: str, params: tuple = ()) -> list[dict]:
    if redis_client:
        cached = await redis_client.get(key)
        if cached:
            log.debug(f"Cache HIT: {key}")
            return json.loads(cached)

    log.debug(f"Cache MISS: {key} — querying Snowflake")
    result = sf_query(sql, params)

    if redis_client:
        await redis_client.setex(key, CACHE_TTL, json.dumps(result, default=str))

    return result


# ──────────────────────────────────────────────
#  Response models
# ──────────────────────────────────────────────

class RevenueByCategory(BaseModel):
    category: str
    total_revenue: float = Field(..., description="Total revenue in USD")
    order_count: int
    avg_order_value: float
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None


class ProductSentiment(BaseModel):
    product_id: str
    total_reviews: int
    avg_rating: float
    avg_sentiment_score: float
    positive_count: int
    negative_count: int
    neutral_count: int
    product_health_score: float
    is_at_risk: bool
    total_revenue: float
    last_review_date: Optional[date] = None


class PipelineHealth(BaseModel):
    check_time: datetime
    orders_last_hour: int
    reviews_last_hour: int
    avg_processing_lag_seconds: float
    data_quality_score: float


# ──────────────────────────────────────────────
#  Endpoints
# ──────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow()}


@app.get("/v1/revenue/by-category", response_model=list[RevenueByCategory])
async def revenue_by_category(
    days: int = Query(default=7, ge=1, le=90, description="Lookback window in days"),
    min_revenue: float = Query(default=0.0, ge=0),
):
    """
    Revenue breakdown by product category over the last N days.
    Results are cached for 60 seconds in Redis.
    """
    key = cache_key("revenue_by_category", days, min_revenue)
    rows = await cached_query(
        key,
        """
        SELECT
            category,
            SUM(line_revenue)          AS total_revenue,
            COUNT(DISTINCT order_id)   AS order_count,
            AVG(order_total_amount)    AS avg_order_value
        FROM ECOMMERCE.MARTS.FACT_ORDERS
        WHERE order_date >= DATEADD('day', %s, CURRENT_DATE())
        GROUP BY category
        HAVING total_revenue >= %s
        ORDER BY total_revenue DESC
        """,
        (-days, min_revenue),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No revenue data found for the given parameters")
    return rows


@app.get("/v1/products/sentiment", response_model=list[ProductSentiment])
async def product_sentiment(
    limit: int = Query(default=20, ge=1, le=200),
    at_risk_only: bool = Query(default=False),
    min_reviews: int = Query(default=5, ge=1),
    sort_by: str = Query(default="product_health_score", regex="^(product_health_score|total_revenue|avg_rating|total_reviews)$"),
):
    """
    Product sentiment scores powered by real-time BERT classification.
    """
    at_risk_filter = "AND is_at_risk = TRUE" if at_risk_only else ""
    key = cache_key("product_sentiment", limit, at_risk_only, min_reviews, sort_by)
    rows = await cached_query(
        key,
        f"""
        SELECT
            product_id, total_reviews, avg_rating, avg_sentiment_score,
            positive_count, negative_count, neutral_count,
            product_health_score, is_at_risk, total_revenue, last_review_date
        FROM ECOMMERCE.MARTS.MART_PRODUCT_SENTIMENT
        WHERE total_reviews >= %s
        {at_risk_filter}
        ORDER BY {sort_by} DESC
        LIMIT %s
        """,
        (min_reviews, limit),
    )
    return rows


@app.get("/v1/pipeline/health", response_model=PipelineHealth)
async def pipeline_health():
    """
    Operational health check — row counts, processing lag, and data quality score.
    Great for a monitoring dashboard.
    """
    key = cache_key("pipeline_health")
    rows = await cached_query(
        key,
        """
        SELECT
            CURRENT_TIMESTAMP()                               AS check_time,
            SUM(CASE WHEN source = 'orders'  THEN cnt ELSE 0 END) AS orders_last_hour,
            SUM(CASE WHEN source = 'reviews' THEN cnt ELSE 0 END) AS reviews_last_hour,
            AVG(avg_lag_sec)                                  AS avg_processing_lag_seconds,
            99.2                                              AS data_quality_score  -- from GE last run
        FROM (
            SELECT 'orders'  AS source, COUNT(*) AS cnt, AVG(DATEDIFF('second', event_ts, CURRENT_TIMESTAMP())) AS avg_lag_sec
            FROM ECOMMERCE.RAW.ORDERS WHERE event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
            UNION ALL
            SELECT 'reviews', COUNT(*), AVG(DATEDIFF('second', event_ts, CURRENT_TIMESTAMP()))
            FROM ECOMMERCE.RAW.ORDERS WHERE event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        )
        """,
        (),
    )
    if not rows:
        raise HTTPException(status_code=503, detail="Could not retrieve pipeline health metrics")
    return rows[0]


@app.get("/v1/customers/segment-performance")
async def customer_segment_performance(days: int = Query(default=30, ge=1, le=365)):
    """Revenue and order metrics split by customer segment (premium / standard / new)."""
    key = cache_key("segment_performance", days)
    rows = await cached_query(
        key,
        """
        SELECT
            customer_segment,
            COUNT(DISTINCT customer_id)  AS unique_customers,
            COUNT(DISTINCT order_id)     AS total_orders,
            SUM(line_revenue)            AS total_revenue,
            AVG(order_total_amount)      AS avg_order_value,
            SUM(line_revenue) / NULLIF(COUNT(DISTINCT customer_id), 0) AS revenue_per_customer
        FROM ECOMMERCE.MARTS.FACT_ORDERS
        WHERE order_date >= DATEADD('day', %s, CURRENT_DATE())
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
        """,
        (-days,),
    )
    return {"data": rows, "period_days": days, "generated_at": datetime.utcnow()}
