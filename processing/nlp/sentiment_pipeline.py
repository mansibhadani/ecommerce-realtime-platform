"""
nlp/sentiment_pipeline.py
--------------------------
Reads unprocessed reviews from the Iceberg raw table, runs BERT-based
sentiment classification, and writes enriched records back to Iceberg
and to Snowflake for the mart_product_sentiment dbt model to consume.

Designed to run as a scheduled Airflow task (every 15 minutes) or as a
one-off batch job.

Requirements:
    pip install transformers torch pyarrow pyiceberg snowflake-connector-python
"""

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

import torch
from transformers import pipeline, Pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MODEL_NAME = os.getenv("SENTIMENT_MODEL", "distilbert-base-uncased-finetuned-sst-2-english")
BATCH_SIZE = int(os.getenv("NLP_BATCH_SIZE", "64"))
MAX_LENGTH = 512


@dataclass
class ReviewRecord:
    review_id: str
    product_id: str
    customer_id: str
    rating: int
    body: str
    event_time: str


@dataclass
class EnrichedReview:
    review_id: str
    product_id: str
    customer_id: str
    rating: int
    sentiment_label: str          # POSITIVE | NEGATIVE | NEUTRAL
    sentiment_score: float        # 0.0 – 1.0 confidence
    rating_sentiment_match: bool  # True if rating and model agree
    processed_at: str


class SentimentPipeline:
    """Wraps HuggingFace pipeline with batching and neutral-label logic."""

    def __init__(self, model_name: str = MODEL_NAME):
        device = 0 if torch.cuda.is_available() else -1
        log.info(f"Loading model '{model_name}' on {'GPU' if device == 0 else 'CPU'}")
        self._pipe: Pipeline = pipeline(
            "text-classification",
            model=model_name,
            device=device,
            truncation=True,
            max_length=MAX_LENGTH,
        )
        log.info("Model loaded successfully")

    def classify(self, texts: list[str]) -> list[dict]:
        """Returns list of {label, score} dicts. Adds NEUTRAL bucket for mid-scores."""
        results = self._pipe(texts, batch_size=BATCH_SIZE, truncation=True)
        enriched = []
        for r in results:
            label = r["label"]        # POSITIVE or NEGATIVE
            score = r["score"]        # confidence

            # Reclassify low-confidence predictions as NEUTRAL
            if score < 0.70:
                label = "NEUTRAL"

            enriched.append({"label": label, "score": round(score, 4)})
        return enriched

    def enrich_batch(self, reviews: list[ReviewRecord]) -> list[EnrichedReview]:
        texts = [r.body for r in reviews]
        classifications = self.classify(texts)

        enriched = []
        for review, cls in zip(reviews, classifications):
            # Check if star rating and model agree
            rating_sentiment = (
                "POSITIVE" if review.rating >= 4
                else "NEGATIVE" if review.rating <= 2
                else "NEUTRAL"
            )
            match = rating_sentiment == cls["label"]

            enriched.append(EnrichedReview(
                review_id=review.review_id,
                product_id=review.product_id,
                customer_id=review.customer_id,
                rating=review.rating,
                sentiment_label=cls["label"],
                sentiment_score=cls["score"],
                rating_sentiment_match=match,
                processed_at=datetime.now(timezone.utc).isoformat(),
            ))
        return enriched


def batch_iter(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i: i + size]


def fetch_unprocessed_reviews(lookback_minutes: int = 20) -> list[ReviewRecord]:
    """
    Read reviews written to Iceberg in the last `lookback_minutes` minutes
    that haven't been enriched yet. In production this would use PyIceberg
    or Spark; here we show the query pattern.
    """
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("default", **{
        "type": "rest",
        "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
    })
    table = catalog.load_table("ecommerce.reviews_raw")
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    arrow_table = (
        table.scan(
            row_filter=f"event_time >= '{cutoff.isoformat()}'",
            selected_fields=("review_id", "product_id", "customer_id", "rating", "body", "event_time"),
        )
        .to_arrow()
    )

    return [
        ReviewRecord(**{k: row[k] for k in ReviewRecord.__dataclass_fields__})
        for row in arrow_table.to_pylist()
    ]


def write_enriched_reviews(records: list[EnrichedReview]):
    """Write results to Snowflake staging table for dbt to pick up."""
    import snowflake.connector
    import json

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cursor = conn.cursor()

    rows = [
        (
            r.review_id, r.product_id, r.customer_id, r.rating,
            r.sentiment_label, r.sentiment_score,
            r.rating_sentiment_match, r.processed_at,
        )
        for r in records
    ]

    cursor.executemany(
        """
        INSERT INTO RAW.REVIEW_SENTIMENTS
            (review_id, product_id, customer_id, rating,
             sentiment_label, sentiment_score, rating_sentiment_match, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"Wrote {len(rows)} enriched reviews to Snowflake")


def run(lookback_minutes: int = 20):
    pipe = SentimentPipeline()

    log.info(f"Fetching reviews from last {lookback_minutes} minutes...")
    reviews = fetch_unprocessed_reviews(lookback_minutes)
    log.info(f"Found {len(reviews)} unprocessed reviews")

    if not reviews:
        log.info("No new reviews to process. Exiting.")
        return

    all_enriched: list[EnrichedReview] = []
    for i, batch in enumerate(batch_iter(reviews, BATCH_SIZE)):
        log.info(f"Processing batch {i + 1} ({len(batch)} reviews)...")
        enriched = pipe.enrich_batch(batch)
        all_enriched.extend(enriched)

    match_rate = sum(r.rating_sentiment_match for r in all_enriched) / len(all_enriched)
    log.info(f"Rating↔sentiment agreement rate: {match_rate:.1%}")

    pos = sum(1 for r in all_enriched if r.sentiment_label == "POSITIVE")
    neg = sum(1 for r in all_enriched if r.sentiment_label == "NEGATIVE")
    neu = sum(1 for r in all_enriched if r.sentiment_label == "NEUTRAL")
    log.info(f"Sentiment distribution → POS: {pos} | NEG: {neg} | NEU: {neu}")

    write_enriched_reviews(all_enriched)
    log.info("NLP pipeline complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-minutes", type=int, default=20)
    args = parser.parse_args()
    run(args.lookback_minutes)
