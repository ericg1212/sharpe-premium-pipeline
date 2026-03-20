"""
Pharma Patent Cliff — Airflow DAG
Schedule: monthly on the 1st at 6 AM (matches FDA Orange Book release cadence)
DAG ID: pharma_patent_cliff

Task flow:
  ingest (TaskGroup)
    extract_orange_book
    extract_yfinance
    extract_clinical_trials
    extract_edgar
  ↓
  load_snowflake (TaskGroup)
    copy_orange_book_to_snowflake
    copy_yfinance_to_snowflake
    copy_clinical_trials_to_snowflake
    copy_edgar_to_snowflake
  ↓
  transform (TaskGroup)
    dbt_run
  ↓
  validate (TaskGroup)
    ge_validate_sources
    ge_validate_marts
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Extractor imports — resolved relative to PYTHONPATH
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pharma_patent_cliff import (
    orange_book_extractor,
    yfinance_extractor,
    clinical_trials_extractor,
    edgar_extractor,
    snowflake_loader,
)

logger = logging.getLogger(__name__)

# --- DAG defaults ---
DEFAULT_ARGS = {
    "owner": "ericg",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_pharma")
GE_SUITE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "expectations", "pharma_cliff_suite.json"
)


# --- Helper callables for PythonOperator ---

def _extract_orange_book(**context):
    uri = orange_book_extractor.run()
    # Push S3 URI to XCom so load tasks can reference the exact path
    context["ti"].xcom_push(key="orange_book_s3_uri", value=uri)
    logger.info("Orange Book extracted to %s", uri)


def _extract_yfinance(**context):
    uris = yfinance_extractor.run()
    context["ti"].xcom_push(key="yfinance_s3_uris", value=uris)
    logger.info("yFinance extracted: %d tickers", len(uris))


def _extract_clinical_trials(**context):
    uri = clinical_trials_extractor.run()
    context["ti"].xcom_push(key="clinical_trials_s3_uri", value=uri)
    logger.info("Clinical Trials extracted to %s", uri)


def _copy_orange_book(**context):
    rows = snowflake_loader.load_orange_book()
    logger.info("Orange Book loaded: %d rows", rows)


def _copy_yfinance(**context):
    rows = snowflake_loader.load_yfinance()
    logger.info("yFinance loaded: %d rows", rows)


def _copy_clinical_trials(**context):
    rows = snowflake_loader.load_clinical_trials()
    logger.info("Clinical Trials loaded: %d rows", rows)


def _extract_edgar(**context):
    uri = edgar_extractor.run()
    context["ti"].xcom_push(key="edgar_s3_uri", value=uri)
    logger.info("EDGAR extracted to %s", uri)


def _copy_edgar(**context):
    rows = snowflake_loader.load_edgar()
    logger.info("EDGAR loaded: %d rows", rows)


def _ge_validate_sources(**context):
    """
    Great Expectations source validation.
    Checks Orange Book and yFinance raw inputs before dbt runs.
    """
    import json
    import great_expectations as gx

    context_ge = gx.get_context()
    suite_path = GE_SUITE_PATH

    # Source suite defined in expectations/pharma_cliff_suite.json
    with open(suite_path) as f:
        suite_config = json.load(f)

    results = context_ge.run_checkpoint(
        checkpoint_name="pharma_sources_checkpoint"
    )
    if not results["success"]:
        raise ValueError("Great Expectations source validation failed — see GE docs")
    logger.info("Source validation passed")


def _ge_validate_marts(**context):
    """
    Great Expectations mart validation.
    Checks final mart outputs after dbt run.
    """
    import great_expectations as gx

    context_ge = gx.get_context()
    results = context_ge.run_checkpoint(
        checkpoint_name="pharma_marts_checkpoint"
    )
    if not results["success"]:
        raise ValueError("Great Expectations mart validation failed — see GE docs")
    logger.info("Mart validation passed")


# --- DAG definition ---

with DAG(
    dag_id="pharma_patent_cliff",
    description="Monthly pharma patent cliff pipeline: Orange Book + yFinance + ClinicalTrials → Snowflake → dbt → GE",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pharma", "snowflake", "dbt", "portfolio"],
) as dag:

    # ── Ingest ────────────────────────────────────────────────────────────────
    with TaskGroup("ingest") as ingest_group:

        extract_orange_book = PythonOperator(
            task_id="extract_orange_book",
            python_callable=_extract_orange_book,
        )

        extract_yfinance = PythonOperator(
            task_id="extract_yfinance",
            python_callable=_extract_yfinance,
        )

        extract_clinical_trials = PythonOperator(
            task_id="extract_clinical_trials",
            python_callable=_extract_clinical_trials,
        )

        extract_edgar = PythonOperator(
            task_id="extract_edgar",
            python_callable=_extract_edgar,
        )

    # ── Load Snowflake ────────────────────────────────────────────────────────
    with TaskGroup("load_snowflake") as load_group:

        copy_orange_book_to_snowflake = PythonOperator(
            task_id="copy_orange_book_to_snowflake",
            python_callable=_copy_orange_book,
        )

        copy_yfinance_to_snowflake = PythonOperator(
            task_id="copy_yfinance_to_snowflake",
            python_callable=_copy_yfinance,
        )

        copy_clinical_trials_to_snowflake = PythonOperator(
            task_id="copy_clinical_trials_to_snowflake",
            python_callable=_copy_clinical_trials,
        )

        copy_edgar_to_snowflake = PythonOperator(
            task_id="copy_edgar_to_snowflake",
            python_callable=_copy_edgar,
        )

    # ── Transform (dbt) ───────────────────────────────────────────────────────
    with TaskGroup("transform") as transform_group:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                f"cd {DBT_DIR} && "
                "dbt run --profiles-dir . --target prod"
            ),
        )

    # ── Validate (Great Expectations) ─────────────────────────────────────────
    with TaskGroup("validate") as validate_group:

        ge_validate_sources = PythonOperator(
            task_id="ge_validate_sources",
            python_callable=_ge_validate_sources,
        )

        ge_validate_marts = PythonOperator(
            task_id="ge_validate_marts",
            python_callable=_ge_validate_marts,
        )

    # ── Dependencies ──────────────────────────────────────────────────────────
    ingest_group >> load_group >> transform_group >> validate_group
