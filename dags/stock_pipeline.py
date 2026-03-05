from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import os
from time import sleep
from config import STOCK_SYMBOLS, RATE_LIMIT_DELAY
from utils import _s3_client, _athena_client, get_date_str, s3_read_json, s3_write_json, s3_write_parquet, register_athena_partition
from data_quality import validate_stock_data, log_data_stats

# Setup logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 2),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'stock_pipeline',
    default_args=default_args,
    description='Extract stock prices and load to S3',
    schedule_interval='0 17 * * 1-5',  # 5 PM daily, Mon-Fri (after market close)
    catchup=False,
)


# Task 1: Extract stock data
def extract_stock_data():
    api_key = os.environ['ALPHA_VANTAGE_API_KEY']
    symbols = STOCK_SYMBOLS

    all_data = []

    for symbol in symbols:
        try:
            logger.info(f"Fetching data for {symbol}")
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"

            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Validate response
            if 'Global Quote' not in data or not data['Global Quote']:
                logger.warning(f"No data returned for {symbol}")
                continue

            quote = data['Global Quote']

            # Extract key fields
            stock_data = {
                'symbol': quote.get('01. symbol', symbol),
                'price': quote.get('05. price', '0'),
                'volume': quote.get('06. volume', '0'),
                'trading_day': quote.get('07. latest trading day', ''),
                'previous_close': quote.get('08. previous close', '0'),
                'change': quote.get('09. change', '0'),
                'change_percent': quote.get('10. change percent', '0%'),
            }

            all_data.append(stock_data)
            logger.info(f"Successfully extracted {symbol}: ${stock_data['price']}")

            # API rate limit: 5 calls per minute for free tier
            sleep(RATE_LIMIT_DELAY)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {symbol}: {str(e)}")
            continue

        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {str(e)}")
            continue

    if not all_data:
        raise Exception("No stock data extracted for any symbol")

    # Write raw data to S3 temp location (shared across workers)
    s3, bucket = _s3_client()
    date_str = get_date_str()
    tmp_key = f"tmp/stocks/raw/{date_str}.json"
    s3_write_json(s3, bucket, tmp_key, all_data)
    logger.info(f"Raw data written to s3://{bucket}/{tmp_key}")

    logger.info(f"Extracted data for {len(all_data)} stocks")
    return all_data


# Task 2: Transform stock data
def transform_stock_data():
    try:
        # Read raw data from S3 temp location
        s3, bucket = _s3_client()
        date_str = get_date_str()
        raw_key = f"tmp/stocks/raw/{date_str}.json"
        raw_data = s3_read_json(s3, bucket, raw_key)

        transformed = []

        for stock in raw_data:
            # Convert strings to numbers
            price = float(stock['price'])
            previous_close = float(stock['previous_close'])
            change = float(stock['change'])
            volume = int(stock['volume'])

            # Calculate additional metrics
            change_percent_num = (change / previous_close * 100) if previous_close > 0 else 0

            transformed_stock = {
                'symbol': stock['symbol'],
                'price': price,
                'previous_close': previous_close,
                'change': change,
                'change_percent': round(change_percent_num, 2),
                'volume': volume,
                'trading_day': stock['trading_day'],
                'timestamp': datetime.now().isoformat(),
            }

            transformed.append(transformed_stock)
            logger.info(f"Transformed {stock['symbol']}: ${price} ({change_percent_num:+.2f}%)")

        # DATA QUALITY CHECK
        try:
            validate_stock_data(transformed)
            log_data_stats(transformed, "stock_data")
        except Exception as e:
            logger.error(f"Data quality check failed: {str(e)}")
            raise

        # Write transformed data to S3 temp location
        transformed_key = f"tmp/stocks/transformed/{date_str}.json"
        s3_write_json(s3, bucket, transformed_key, transformed)
        logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

        logger.info(f"Successfully transformed {len(transformed)} stocks")
        return transformed

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


# Task 3: Load to S3
def load_to_s3():
    try:
        s3, bucket = _s3_client()

        # Read transformed data from S3 temp location
        date_str = get_date_str()
        transformed_key = f"tmp/stocks/transformed/{date_str}.json"
        data = s3_read_json(s3, bucket, transformed_key)

        # Upload final data to S3 as NDJSON
        timestamp_str = get_date_str('timestamp')
        key = f"stocks/date={date_str}/{timestamp_str}.parquet"
        s3_write_parquet(s3, bucket, key, data)
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")

        # Verify upload
        response = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Upload verified. File size: {response['ContentLength']} bytes")

        # Register partition with Athena so it's queryable immediately
        athena = _athena_client()
        location = f"s3://{bucket}/stocks/date={date_str}/"
        register_athena_partition(athena, 'stocks', 'date', date_str, location)
        logger.info(f"Registered Athena partition date={date_str} for stocks")

        return f"s3://{bucket}/{key}"

    except Exception as e:
        logger.error(f"Upload to S3 failed: {str(e)}")
        raise


# Define tasks
extract_task = PythonOperator(
    task_id='extract_stock_data',
    python_callable=extract_stock_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_stock_data',
    python_callable=transform_stock_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

# Set dependencies
extract_task >> transform_task >> load_task
