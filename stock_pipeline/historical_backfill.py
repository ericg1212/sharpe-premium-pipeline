"""
Historical Backfill: Write 3 years of monthly stock prices to S3.

One-time script — not a recurring DAG. Run from command line:
    python stock_pipeline/historical_backfill.py

Writes NDJSON to:
    s3://ai-sharpe-analysis-eric/historical_prices/symbol={symbol}/monthly.json

Registers each symbol as a Hive-style partition in Athena so the
historical_prices table is immediately queryable after the backfill.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import boto3
import requests
from datetime import datetime
from time import sleep
from config import STOCKS, GLUE_DATABASE, ATHENA_WORKGROUP, RATE_LIMIT_DELAY
from utils import _s3_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_monthly_prices(symbol):
    """Fetch monthly adjusted close prices from Alpha Vantage (3+ years)."""
    api_key = os.environ['ALPHA_VANTAGE_API_KEY']
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_MONTHLY_ADJUSTED"
        f"&symbol={symbol}&apikey={api_key}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    if 'Monthly Adjusted Time Series' not in data:
        raise ValueError(f"No monthly data for {symbol}: {list(data.keys())}")

    series = data['Monthly Adjusted Time Series']
    records = []
    for date_str, values in series.items():
        date = datetime.strptime(date_str, '%Y-%m-%d')
        if datetime(2022, 12, 1) <= date <= datetime.now():
            records.append({
                'date': date_str,
                'close': float(values['5. adjusted close']),
                'volume': int(values['6. volume']),
            })

    records.sort(key=lambda r: r['date'])
    logger.info(f"{symbol}: fetched {len(records)} monthly records")
    return records


def format_records(symbol, records):
    """Add symbol + extracted_at to each record. Return NDJSON string."""
    extracted_at = datetime.now().isoformat()
    enriched = []
    for r in records:
        enriched.append({
            'symbol': symbol,
            'date': r['date'],
            'close': r['close'],
            'volume': r['volume'],
            'extracted_at': extracted_at,
        })
    return '\n'.join(json.dumps(r) for r in enriched)


def write_to_s3(symbol, body):
    """Write NDJSON body to s3://bucket/historical_prices/symbol={symbol}/monthly.json."""
    s3, bucket = _s3_client()
    key = f"historical_prices/symbol={symbol}/monthly.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
    logger.info(f"Written to s3://{bucket}/{key}")
    return bucket, key


def register_partition(symbol, bucket):
    """Register symbol partition with Athena so the data is immediately queryable."""
    athena = boto3.client(
        'athena',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )
    location = f"s3://{bucket}/historical_prices/symbol={symbol}/"
    athena.start_query_execution(
        QueryString=(
            f"ALTER TABLE historical_prices ADD IF NOT EXISTS "
            f"PARTITION (symbol='{symbol}') LOCATION '{location}'"
        ),
        QueryExecutionContext={'Database': GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )
    logger.info(f"Registered Athena partition symbol={symbol}")


def run_backfill():
    """Backfill all symbols. Fetches, formats, writes to S3, registers partitions."""
    symbols = list(STOCKS.keys())
    logger.info(f"Starting historical backfill for {len(symbols)} symbols")

    succeeded = []
    failed = []

    for i, symbol in enumerate(symbols):
        try:
            records = fetch_monthly_prices(symbol)
            body = format_records(symbol, records)
            bucket, key = write_to_s3(symbol, body)
            register_partition(symbol, bucket)
            succeeded.append(symbol)
            logger.info(f"[{i+1}/{len(symbols)}] {symbol} complete")

            if i < len(symbols) - 1:
                sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logger.error(f"[{i+1}/{len(symbols)}] {symbol} FAILED: {str(e)}")
            failed.append(symbol)
            continue

    logger.info(f"Backfill complete. Succeeded: {succeeded}. Failed: {failed}")
    if failed:
        logger.warning(f"Re-run for failed symbols: {failed}")


if __name__ == '__main__':
    run_backfill()
