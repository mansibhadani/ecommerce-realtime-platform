"""
orchestration/airflow/dags/ecommerce_pipeline.py
-------------------------------------------------
Full pipeline DAG:
  1. Data quality checks on raw Iceberg tables (Great Expectations)
  2. dbt transformations (staging → intermediate → marts)
  3. NLP sentiment batch job
  4. Post-load data quality on Snowflake marts
  5. Slack alert on failure / SLA miss

Schedule: every 30 minutes
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────

SLACK_WEBHOOK_CONN = "slack_data_alerts"
DBT_PROJECT_DIR = "/opt/airflow/dags/repo/transformation/dbt"
GE_BASE_DIR = "/opt/airflow/dags/repo/quality/great_expectations"
NLP_SCRIPT = "/opt/airflow/dags/repo/processing/nlp/sentiment_pipeline.py"

DEFAULT_ARGS = {
    "owner": "mansi.bhadani",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "sla": timedelta(minutes=25),
}

# ──────────────────────────────────────────────
#  Callbacks
# ──────────────────────────────────────────────

def on_failure_callback(context):
    """Post a Slack alert when any task fails."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    SlackWebhookOperator(
        task_id="slack_failure_notification",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN,
        message=(
            f":red_circle: *Pipeline Failure*\n"
            f"DAG: `{dag_id}` | Task: `{task_id}`\n"
            f"Execution: `{exec_date}`\n"
            f"<{log_url}|View Logs>"
        ),
    ).execute(context)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    SlackWebhookOperator(
        task_id="slack_sla_miss",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN,
        message=f":warning: *SLA Miss* on DAG `{dag.dag_id}` — Tasks: {[t.task_id for t in blocking_tis]}",
    ).execute({})


# ──────────────────────────────────────────────
#  Great Expectations helpers
# ──────────────────────────────────────────────

def run_ge_checkpoint(checkpoint_name: str):
    import great_expectations as gx
    context = gx.get_context(project_root_dir=GE_BASE_DIR)
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    if not result["success"]:
        failed = [
            v["expectation_config"]["expectation_type"]
            for v in result["run_results"].values()
            for v in v["validation_result"]["results"]
            if not v["success"]
        ]
        raise ValueError(f"GE checkpoint '{checkpoint_name}' failed. Failed expectations: {failed}")
    return f"Checkpoint '{checkpoint_name}' passed."


def check_row_count(**context):
    """
    Branch: if raw orders table has 0 new rows since last run,
    skip transformation and go straight to slack success.
    """
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="ECOMMERCE",
        schema="RAW",
        warehouse="COMPUTE_WH",
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM ECOMMERCE.RAW.ORDERS WHERE EVENT_TS >= DATEADD('minute', -35, CURRENT_TIMESTAMP())"
    )
    row_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="new_row_count", value=row_count)

    if row_count == 0:
        return "skip_run"
    return "ge_raw_orders_check"


# ──────────────────────────────────────────────
#  DAG definition
# ──────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_pipeline_dag",
    default_args=DEFAULT_ARGS,
    description="Real-time e-commerce analytics pipeline",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-engineering", "production"],
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Branch: check if there's data to process ──────────────────────────
    branch = BranchPythonOperator(
        task_id="check_new_data",
        python_callable=check_row_count,
    )

    skip_run = BashOperator(
        task_id="skip_run",
        bash_command='echo "No new data in the last 35 minutes. Skipping pipeline."',
    )

    # ── Great Expectations: raw layer checks ─────────────────────────────
    ge_raw_orders = PythonOperator(
        task_id="ge_raw_orders_check",
        python_callable=run_ge_checkpoint,
        op_args=["raw_orders_checkpoint"],
    )

    ge_raw_reviews = PythonOperator(
        task_id="ge_raw_reviews_check",
        python_callable=run_ge_checkpoint,
        op_args=["raw_reviews_checkpoint"],
    )

    # ── dbt: staging layer ────────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select tag:staging --target prod --no-compile"
        ),
    )

    dbt_staging_test = BashOperator(
        task_id="dbt_staging_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --select tag:staging --target prod"
        ),
    )

    # ── NLP sentiment batch ───────────────────────────────────────────────
    nlp_sentiment = BashOperator(
        task_id="nlp_sentiment_pipeline",
        bash_command=f"python {NLP_SCRIPT} --lookback-minutes 35",
        execution_timeout=timedelta(minutes=15),
    )

    # ── dbt: marts layer ─────────────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select tag:mart --target prod --no-compile"
        ),
    )

    dbt_marts_test = BashOperator(
        task_id="dbt_marts_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --select tag:mart --target prod"
        ),
    )

    # ── Great Expectations: marts (post-transform) ───────────────────────
    ge_marts = PythonOperator(
        task_id="ge_marts_check",
        python_callable=run_ge_checkpoint,
        op_args=["marts_checkpoint"],
    )

    # ── Generate dbt docs (nightly-ish: only at midnight run) ─────────────
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt docs generate --target prod || true"   # non-blocking
        ),
    )

    # ── Slack success notification ────────────────────────────────────────
    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN,
        message=(
            ":large_green_circle: *Pipeline succeeded* — "
            "`ecommerce_pipeline_dag` completed at {{ ts }}"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ──────────────────────────────────────────────
    #  Task dependencies
    # ──────────────────────────────────────────────
    branch >> [skip_run, ge_raw_orders]

    ge_raw_orders >> ge_raw_reviews >> dbt_staging >> dbt_staging_test

    # NLP and dbt staging can run in parallel from staging tests
    dbt_staging_test >> [nlp_sentiment, dbt_marts]
    nlp_sentiment >> dbt_marts          # ensure NLP writes before mart build

    dbt_marts >> dbt_marts_test >> ge_marts >> dbt_docs >> slack_success
