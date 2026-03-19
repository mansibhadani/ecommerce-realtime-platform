"""
kafka_producer.py
-----------------
Simulates real-time e-commerce events across four Kafka topics:
  - ecommerce.orders
  - ecommerce.clickstream
  - ecommerce.inventory
  - ecommerce.reviews

Usage:
    python ingestion/kafka_producer.py --events-per-second 500 --duration 300
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

PRODUCT_CATALOG = [
    {"id": f"PROD-{i:04d}", "name": fake.catch_phrase(), "category": random.choice(
        ["Electronics", "Apparel", "Home", "Beauty", "Sports", "Books"]
    ), "price": round(random.uniform(5.99, 999.99), 2)}
    for i in range(1, 201)
]

CUSTOMER_POOL = [
    {"id": str(uuid.uuid4()), "name": fake.name(), "email": fake.email(),
     "segment": random.choice(["premium", "standard", "new"])}
    for _ in range(10_000)
]

REVIEW_TEMPLATES = {
    "positive": [
        "Absolutely love this product! Exceeded all expectations.",
        "Great quality and fast shipping. Will buy again.",
        "Five stars - exactly as described and works perfectly.",
        "Amazing value for the price. Highly recommend!",
    ],
    "negative": [
        "Very disappointed. Product broke after two days.",
        "Not as described. Poor quality materials.",
        "Terrible customer service and slow delivery.",
        "Would not recommend. Complete waste of money.",
    ],
    "neutral": [
        "Decent product. Does what it says, nothing more.",
        "Average quality. Got what I paid for.",
        "Fine product but shipping took longer than expected.",
        "Works okay. Not amazing but not terrible either.",
    ],
}


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 131072,
        "batch.num.messages": 500,
        "linger.ms": 10,
        "compression.type": "snappy",
        "acks": "1",
    })


# ──────────────────────────────────────────────
#  Event generators
# ──────────────────────────────────────────────

def generate_order() -> dict[str, Any]:
    customer = random.choice(CUSTOMER_POOL)
    items = random.sample(PRODUCT_CATALOG, k=random.randint(1, 5))
    total = sum(p["price"] * random.randint(1, 3) for p in items)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer["id"],
        "customer_segment": customer["segment"],
        "items": [
            {
                "product_id": p["id"],
                "product_name": p["name"],
                "category": p["category"],
                "quantity": random.randint(1, 3),
                "unit_price": p["price"],
            }
            for p in items
        ],
        "total_amount": round(total, 2),
        "currency": "USD",
        "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "debit_card"]),
        "status": "placed",
        "shipping_address": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": "US",
        },
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "order_placed",
    }


def generate_click_event() -> dict[str, Any]:
    customer = random.choice(CUSTOMER_POOL)
    product = random.choice(PRODUCT_CATALOG)
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "customer_id": customer["id"],
        "product_id": product["id"],
        "page_type": random.choice(["pdp", "category", "search", "homepage", "cart"]),
        "action": random.choice(["view", "add_to_cart", "remove_from_cart", "wishlist", "share"]),
        "referrer": random.choice(["google", "instagram", "email", "direct", "facebook"]),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "duration_seconds": random.randint(1, 300),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "click",
    }


def generate_inventory_update() -> dict[str, Any]:
    product = random.choice(PRODUCT_CATALOG)
    delta = random.randint(-50, 200)
    return {
        "update_id": str(uuid.uuid4()),
        "product_id": product["id"],
        "warehouse_id": f"WH-{random.randint(1, 10):02d}",
        "quantity_delta": delta,
        "quantity_after": max(0, random.randint(0, 1000) + delta),
        "reason": random.choice(["sale", "restock", "return", "damaged", "transfer"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "inventory_update",
    }


def generate_review() -> dict[str, Any]:
    customer = random.choice(CUSTOMER_POOL)
    product = random.choice(PRODUCT_CATALOG)
    sentiment_bucket = random.choices(
        ["positive", "negative", "neutral"], weights=[0.60, 0.20, 0.20]
    )[0]
    rating = {"positive": random.randint(4, 5), "negative": random.randint(1, 2), "neutral": 3}[sentiment_bucket]
    return {
        "review_id": str(uuid.uuid4()),
        "customer_id": customer["id"],
        "product_id": product["id"],
        "rating": rating,
        "title": fake.sentence(nb_words=6),
        "body": random.choice(REVIEW_TEMPLATES[sentiment_bucket]),
        "verified_purchase": random.choice([True, True, True, False]),
        "helpful_votes": random.randint(0, 200),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "review_submitted",
    }


# ──────────────────────────────────────────────
#  Main producer loop
# ──────────────────────────────────────────────

TOPIC_GENERATORS = {
    "ecommerce.orders": (generate_order, 0.10),        # 10% of traffic
    "ecommerce.clickstream": (generate_click_event, 0.70),  # 70% of traffic
    "ecommerce.inventory": (generate_inventory_update, 0.05),  # 5%
    "ecommerce.reviews": (generate_review, 0.15),       # 15%
}


def run(events_per_second: int, duration_seconds: int):
    producer = make_producer()
    topics = list(TOPIC_GENERATORS.keys())
    weights = [TOPIC_GENERATORS[t][1] for t in topics]

    interval = 1.0 / events_per_second
    end_time = time.time() + duration_seconds
    produced = 0

    print(f"[INFO] Producing {events_per_second} events/sec for {duration_seconds}s → "
          f"~{events_per_second * duration_seconds:,} total events")

    try:
        while time.time() < end_time:
            loop_start = time.time()

            topic = random.choices(topics, weights=weights, k=1)[0]
            generator, _ = TOPIC_GENERATORS[topic]
            event = generator()
            key = event.get("customer_id", str(uuid.uuid4()))

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            produced += 1
            if produced % 10_000 == 0:
                producer.flush()
                print(f"[INFO] Produced {produced:,} events | "
                      f"elapsed={time.time() - (end_time - duration_seconds):.1f}s")

            # throttle to target rate
            elapsed = time.time() - loop_start
            if elapsed < interval:
                time.sleep(interval - elapsed)

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    finally:
        producer.flush()
        print(f"[INFO] Done. Total events produced: {produced:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Kafka Event Producer")
    parser.add_argument("--events-per-second", type=int, default=100)
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()
    run(args.events_per_second, args.duration)
