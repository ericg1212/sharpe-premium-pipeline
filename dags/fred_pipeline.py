"""
FRED Macro Pipeline: Pull monthly macro indicators from the St. Louis Fed FRED API.

Answers: "Does the AI Sharpe premium hold across different macro regimes?"

Series pulled:
  GS10      — 10-Year Treasury yield (risk-free rate proxy)
  CPIAUCSL  — Consumer Price Index (inflation)
  UNRATE    — Unemployment rate
  FEDFUNDS  — Federal Funds effective rate (monetary policy)

Runs on the 1st of each month — FRED releases monthly data with a ~2-week lag,
so the prior month's values are available by the 1st.

S3 path: macro_indicators/series={series_id}/year={year}/data.json
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import boto3
import requests
from datetime import datetime
from collections import defaultdict
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import GLUE_DATABASE, ATHENA_WORKGROUP, FRED_SERIES
from utils import _s3_client

logger = logging.getLogger(__name__)

FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations'
OBSERVATION_START = '2020-01-01'  # aligns with edgar 2020 cutoff

default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    'fred_pipeline',
    default_args=default_args,
    description='Pull monthly macro indicators from FRED API',
    schedule_interval='0 0 1 * *',  # 1st of every month
    catchup=False,
)


# Task 1: Fetch raw observations for all series from FRED API
def extract_fred_data():
    api_key = os.environ['FRED_API_KEY']
    all_raw = {}

    for series_id, metadata in FRED_SERIES.items():
        logger.info(f"Fetching FRED series {series_id}: {metadata['description']}")

        response = requests.get(
            FRED_BASE_URL,
            params={
                'series_id': series_id,
                'api_key': api_key,
                'file_type': 'json',
                'observation_start': OBSERVATION_START,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if 'observations' not in data:
            raise ValueError(f"No observations in FRED response for {series_id}")

        all_raw[series_id] = {
            'series_id': series_id,
            'description': metadata['description'],
            'units': metadata['units'],
            'observations': data['observations'],
        }
        logger.info(f"{series_id}: {len(data['observations'])} observations fetched")

    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')
    tmp_key = f"tmp/fred/raw/{date_str}.json"
    s3.put_object(
        Bucket=bucket,
        Key=tmp_key,
        Body=json.dumps(all_raw),
        ContentType='application/json',
    )
    logger.info(f"Raw FRED data written to s3://{bucket}/{tmp_key}")


# Task 2: Transform — parse observations, drop missing values, add metadata
def transform_fred_data():
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')

    raw_obj = s3.get_object(Bucket=bucket, Key=f"tmp/fred/raw/{date_str}.json")
    all_raw = json.loads(raw_obj['Body'].read())

    extracted_at = datetime.now().isoformat()
    records = []

    for series_id, series_data in all_raw.items():
        skipped = 0
        for obs in series_data['observations']:
            # FRED uses "." for missing/unreleased values — skip them
            if obs['value'] == '.':
                skipped += 1
                continue

            date_str_obs = obs['date']  # format: YYYY-MM-DD
            year = int(date_str_obs[:4])
            month = int(date_str_obs[5:7])

            records.append({
                'series_id': series_id,
                'description': series_data['description'],
                'units': series_data['units'],
                'date': date_str_obs,
                'year': year,
                'month': month,
                'value': float(obs['value']),
                'extracted_at': extracted_at,
            })

        logger.info(
            f"{series_id}: {len(series_data['observations']) - skipped} records "
            f"transformed, {skipped} missing values skipped"
        )

    if not records:
        raise Exception("No FRED records extracted — all values missing or API error")

    transformed_key = f"tmp/fred/transformed/{datetime.now().strftime('%Y-%m-%d')}.json"
    s3.put_object(
        Bucket=bucket,
        Key=transformed_key,
        Body=json.dumps(records),
        ContentType='application/json',
    )
    logger.info(f"Transformed {len(records)} FRED records to s3://{bucket}/{transformed_key}")


# Task 3: Load — partition by series + year, register Athena partitions
def load_to_s3():
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')

    transformed_obj = s3.get_object(
        Bucket=bucket, Key=f"tmp/fred/transformed/{date_str}.json"
    )
    records = json.loads(transformed_obj['Body'].read())

    # Group by (series_id, year) — one S3 file per partition
    partitions = defaultdict(list)
    for record in records:
        partitions[(record['series_id'], record['year'])].append(record)

    athena = boto3.client(
        'athena',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )

    for (series_id, year), partition_records in partitions.items():
        key = f"macro_indicators/series={series_id}/year={year}/data.json"
        body = '\n'.join(json.dumps(r) for r in partition_records)

        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
        logger.info(f"Written {len(partition_records)} records to s3://{bucket}/{key}")

        location = f"s3://{bucket}/macro_indicators/series={series_id}/year={year}/"
        athena.start_query_execution(
            QueryString=(
                f"ALTER TABLE macro_indicators ADD IF NOT EXISTS "
                f"PARTITION (series_id='{series_id}', year='{year}') LOCATION '{location}'"
            ),
            QueryExecutionContext={'Database': GLUE_DATABASE},
            WorkGroup=ATHENA_WORKGROUP,
        )
        logger.info(f"Registered Athena partition series={series_id}/year={year}")

    logger.info(f"Load complete. {len(partitions)} partitions written.")


# Define tasks
extract_task = PythonOperator(
    task_id='extract_fred_data',
    python_callable=extract_fred_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_fred_data',
    python_callable=transform_fred_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> transform_task >> load_task
