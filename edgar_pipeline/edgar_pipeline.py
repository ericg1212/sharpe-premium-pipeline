"""
SEC EDGAR Pipeline: Pull annual capex + revenue from 10-K filings.

Replaces hardcoded AI_CAPEX in config.py with authoritative SEC data.
Runs quarterly to pick up each company's new 10-K within 3 months of filing.

Data source: SEC EDGAR Company Facts API (free, no auth key required).
  https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json

Writes to S3: fundamentals/cik={cik}/year={year}/data.json
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import boto3
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import EDGAR_CIKS, GLUE_DATABASE, ATHENA_WORKGROUP
from utils import _s3_client

logger = logging.getLogger(__name__)

# Revenue tags vary by company — try in order, use first with data
REVENUE_TAGS = [
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'Revenues',
    'SalesRevenueNet',
]

# SEC requires User-Agent header to identify the requester
EDGAR_HEADERS = {'User-Agent': 'Eric Grynspan ericg@example.com'}

default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    'edgar_pipeline',
    default_args=default_args,
    description='Pull annual capex + revenue from SEC EDGAR 10-K filings',
    schedule_interval='0 0 1 1,4,7,10 *',  # quarterly: Jan 1, Apr 1, Jul 1, Oct 1
    catchup=False,
)


def _extract_annual_records(facts, tag):
    """Extract annual (10-K FY) records for a given XBRL tag."""
    try:
        filings = facts['us-gaap'][tag]['units']['USD']
    except KeyError:
        return []

    return [
        f for f in filings
        if f.get('fp') == 'FY' and f.get('form') == '10-K'
    ]


# Task 1: Extract raw company facts from EDGAR for all 4 companies
def extract_edgar_data():
    all_raw = {}

    for symbol, cik in EDGAR_CIKS.items():
        try:
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            logger.info(f"Fetching EDGAR data for {symbol} (CIK {cik})")

            response = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'facts' not in data:
                raise ValueError(f"No facts in EDGAR response for {symbol}")

            all_raw[symbol] = {
                'cik': cik,
                'entity_name': data.get('entityName', symbol),
                'facts': data['facts'],
            }
            logger.info(f"{symbol}: raw facts fetched successfully")

        except Exception as e:
            logger.error(f"{symbol} EDGAR fetch failed: {str(e)}")
            raise

    # Write raw data to S3 tmp location
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')
    tmp_key = f"tmp/edgar/raw/{date_str}.json"
    s3.put_object(
        Bucket=bucket,
        Key=tmp_key,
        Body=json.dumps(all_raw),
        ContentType='application/json',
    )
    logger.info(f"Raw EDGAR data written to s3://{bucket}/{tmp_key}")


# Task 2: Transform — extract annual capex + revenue per company per year
def transform_fundamentals():
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')

    raw_obj = s3.get_object(Bucket=bucket, Key=f"tmp/edgar/raw/{date_str}.json")
    all_raw = json.loads(raw_obj['Body'].read())

    extracted_at = datetime.now().isoformat()
    records = []

    for symbol, company in all_raw.items():
        cik = company['cik']
        facts = company['facts']

        # Extract annual capex records
        capex_filings = _extract_annual_records(facts, 'PaymentsToAcquirePropertyPlantAndEquipment')
        if not capex_filings:
            logger.warning(f"{symbol}: no annual capex records found")
            continue

        # Extract annual revenue — try tags in order
        revenue_filings = []
        revenue_tag_used = None
        for tag in REVENUE_TAGS:
            revenue_filings = _extract_annual_records(facts, tag)
            if revenue_filings:
                revenue_tag_used = tag
                break

        if not revenue_filings:
            logger.warning(f"{symbol}: no annual revenue records found")

        # Index revenue by fiscal year for easy lookup
        revenue_by_year = {f['fy']: f['val'] for f in revenue_filings}

        # Build one record per fiscal year
        for filing in capex_filings:
            year = filing['fy']
            if year < 2020:
                continue

            record = {
                'cik': cik,
                'symbol': symbol,
                'year': year,
                'capex_usd': filing['val'],
                'revenue_usd': revenue_by_year.get(year),
                'revenue_tag': revenue_tag_used,
                'period_end': filing['end'],
                'filed': filing.get('filed'),
                'extracted_at': extracted_at,
            }
            records.append(record)
            logger.info(
                f"{symbol} {year}: capex=${filing['val']/1e9:.1f}B "
                f"revenue=${revenue_by_year.get(year, 0)/1e9:.1f}B"
            )

    if not records:
        raise Exception("No fundamental records extracted")

    # Write transformed records to S3 tmp location
    transformed_key = f"tmp/edgar/transformed/{date_str}.json"
    s3.put_object(
        Bucket=bucket,
        Key=transformed_key,
        Body=json.dumps(records),
        ContentType='application/json',
    )
    logger.info(f"Transformed {len(records)} records to s3://{bucket}/{transformed_key}")


# Task 3: Load — write per-company per-year to S3, register Athena partitions
def load_to_s3():
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')

    transformed_obj = s3.get_object(
        Bucket=bucket, Key=f"tmp/edgar/transformed/{date_str}.json"
    )
    records = json.loads(transformed_obj['Body'].read())

    # Group records by (cik, year) — one S3 file per partition
    from collections import defaultdict
    partitions = defaultdict(list)
    for record in records:
        partitions[(record['cik'], record['year'])].append(record)

    athena = boto3.client(
        'athena',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )

    for (cik, year), partition_records in partitions.items():
        key = f"fundamentals/cik={cik}/year={year}/data.json"
        body = '\n'.join(json.dumps(r) for r in partition_records)

        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
        logger.info(f"Written {len(partition_records)} records to s3://{bucket}/{key}")

        # Register partition with Athena
        location = f"s3://{bucket}/fundamentals/cik={cik}/year={year}/"
        athena.start_query_execution(
            QueryString=(
                f"ALTER TABLE fundamentals ADD IF NOT EXISTS "
                f"PARTITION (cik='{cik}', year='{year}') LOCATION '{location}'"
            ),
            QueryExecutionContext={'Database': GLUE_DATABASE},
            WorkGroup=ATHENA_WORKGROUP,
        )
        logger.info(f"Registered Athena partition cik={cik}/year={year}")

    logger.info(f"Load complete. {len(partitions)} partitions written.")


# Define tasks
extract_task = PythonOperator(
    task_id='extract_edgar_data',
    python_callable=extract_edgar_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_fundamentals',
    python_callable=transform_fundamentals,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> transform_task >> load_task
