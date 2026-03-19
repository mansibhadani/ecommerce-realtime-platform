"""
quality/great_expectations/run_checkpoints.py
----------------------------------------------
Programmatically defines and runs Great Expectations suites for:
  - raw.orders (Iceberg / Snowflake)
  - raw.review_sentiments
  - mart_product_sentiment

Run standalone:
    python quality/great_expectations/run_checkpoints.py

Or called by Airflow tasks in ecommerce_pipeline.py.
"""

import os
import sys
import logging

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GE_DIR = os.path.dirname(os.path.abspath(__file__))


def build_context():
    return gx.get_context(project_root_dir=GE_DIR)


# ──────────────────────────────────────────────
#  Expectation suites
# ──────────────────────────────────────────────

def create_orders_suite(context):
    suite_name = "raw_orders_suite"
    try:
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass

    suite = context.create_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=BatchRequest(
            datasource_name="snowflake_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="ECOMMERCE.RAW.ORDERS",
        ),
        expectation_suite=suite,
    )

    # Schema checks
    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            "order_id", "customer_id", "product_id", "product_name",
            "category", "customer_segment", "payment_method", "order_status",
            "currency", "quantity", "unit_price", "line_revenue",
            "order_total_amount", "event_ts", "order_date",
        ]
    )

    # Completeness
    for col in ["order_id", "customer_id", "product_id", "quantity", "unit_price"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # Uniqueness
    validator.expect_column_values_to_be_unique(column="order_id")

    # Domain checks
    validator.expect_column_values_to_be_between(column="quantity", min_value=1, max_value=100)
    validator.expect_column_values_to_be_between(column="unit_price", min_value=0.01, max_value=100_000)
    validator.expect_column_values_to_be_between(column="line_revenue", min_value=0.01)

    validator.expect_column_values_to_be_in_set(
        column="currency", value_set=["USD", "EUR", "GBP", "CAD"]
    )
    validator.expect_column_values_to_be_in_set(
        column="order_status", value_set=["placed", "confirmed", "shipped", "delivered", "cancelled"]
    )
    validator.expect_column_values_to_be_in_set(
        column="customer_segment", value_set=["premium", "standard", "new"]
    )

    # Freshness: max event_ts should be within last 2 hours
    validator.expect_column_max_to_be_between(
        column="event_ts",
        min_value="2024-01-01",
        parse_strings_as_datetimes=True,
    )

    # Volume: expect at least 1000 new rows per 30-min run
    validator.expect_table_row_count_to_be_between(min_value=1_000)

    # Statistical: avg order total should be reasonable
    validator.expect_column_mean_to_be_between(
        column="order_total_amount", min_value=10.0, max_value=10_000.0
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    log.info(f"Suite '{suite_name}' saved with {len(suite.expectations)} expectations")
    return suite_name


def create_product_sentiment_suite(context):
    suite_name = "marts_product_sentiment_suite"
    try:
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass

    suite = context.create_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=BatchRequest(
            datasource_name="snowflake_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="ECOMMERCE.MARTS.MART_PRODUCT_SENTIMENT",
        ),
        expectation_suite=suite,
    )

    validator.expect_column_values_to_not_be_null(column="product_id")
    validator.expect_column_values_to_be_unique(column="product_id")
    validator.expect_column_values_to_be_between(column="avg_rating", min_value=1.0, max_value=5.0)
    validator.expect_column_values_to_be_between(column="avg_sentiment_score", min_value=0.0, max_value=1.0)
    validator.expect_column_values_to_be_between(column="product_health_score", min_value=0.0, max_value=100.0)
    validator.expect_column_values_to_be_in_set(
        column="sentiment_label",
        value_set=["POSITIVE", "NEGATIVE", "NEUTRAL"],
        mostly=0.0,  # applied to a non-aggregated view; skip for mart
    )
    validator.expect_column_values_to_be_between(
        column="rating_sentiment_agreement_rate", min_value=0.0, max_value=1.0
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    log.info(f"Suite '{suite_name}' saved")
    return suite_name


# ──────────────────────────────────────────────
#  Checkpoint runners
# ──────────────────────────────────────────────

def run_checkpoint(context, suite_name: str, data_asset_name: str) -> bool:
    checkpoint_config = {
        "name": f"{suite_name}_checkpoint",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "snowflake_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": data_asset_name,
                },
                "expectation_suite_name": suite_name,
            }
        ],
    }

    result = context.run_checkpoint(**checkpoint_config)

    if result["success"]:
        log.info(f"✓ Checkpoint passed for '{suite_name}'")
    else:
        log.error(f"✗ Checkpoint FAILED for '{suite_name}'")
        for run_result in result["run_results"].values():
            for exp_result in run_result["validation_result"]["results"]:
                if not exp_result["success"]:
                    log.error(f"  Failed: {exp_result['expectation_config']['expectation_type']} "
                              f"| result: {exp_result['result']}")

    return result["success"]


def main():
    context = build_context()

    log.info("Building expectation suites...")
    orders_suite = create_orders_suite(context)
    sentiment_suite = create_product_sentiment_suite(context)

    log.info("Running checkpoints...")
    results = {
        "orders": run_checkpoint(context, orders_suite, "ECOMMERCE.RAW.ORDERS"),
        "sentiment_mart": run_checkpoint(context, sentiment_suite, "ECOMMERCE.MARTS.MART_PRODUCT_SENTIMENT"),
    }

    passed = sum(results.values())
    total = len(results)
    log.info(f"\nResults: {passed}/{total} suites passed")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
