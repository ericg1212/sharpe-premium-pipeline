from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from time import sleep
from config import CRYPTO_SYMBOLS
from utils import (
    _s3_client, _athena_client, get_date_str,
    s3_read_json, s3_write_json, s3_write_parquet, register_athena_partition
)
from data_quality import log_data_stats

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 2),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_pipeline',
    default_args=default_args,
    description='Extract crypto prices and load to S3',
    schedule_interval='0 */6 * * *',  # Every 6 hours (crypto trades 24/7)
    catchup=False,
)


def extract_crypto_data():
    symbols = CRYPTO_SYMBOLS

    all_data = []
    max_retries = 3

    for symbol in symbols:
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching {symbol} (attempt {attempt + 1})")
                url = f"https://api.coinbase.com/v2/prices/{symbol}/spot"

                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                if 'data' not in data or 'amount' not in data['data']:
                    raise ValueError(f"Invalid response for {symbol}")

                price_data = data['data']
                base = symbol.split('-')[0]

                crypto_data = {
                    'symbol': base,
                    'price': price_data['amount'],
                    'currency': price_data['currency'],
                    'timestamp': datetime.now().isoformat(),
                }

                all_data.append(crypto_data)
                logger.info(f"Successfully extracted {base}: ${float(price_data['amount']):,.2f}")
                break

            except requests.exceptions.RequestException as e:
                logger.warning(f"{symbol} request failed on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    sleep(5)
                    continue
                else:
                    logger.error(f"Failed to fetch {symbol} after all retries")

            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {str(e)}")
                break

    if not all_data:
        raise Exception("No crypto data extracted")

    s3, bucket = _s3_client()
    date_str = get_date_str()
    tmp_key = f"tmp/crypto/raw/{date_str}.json"
    s3_write_json(s3, bucket, tmp_key, all_data)
    logger.info(f"Raw data written to s3://{bucket}/{tmp_key}")

    logger.info(f"Extracted data for {len(all_data)} cryptocurrencies")
    return all_data


def transform_crypto_data():
    try:
        s3, bucket = _s3_client()
        date_str = get_date_str()
        raw_key = f"tmp/crypto/raw/{date_str}.json"
        raw_data = s3_read_json(s3, bucket, raw_key)

        transformed = []

        for crypto in raw_data:
            price = float(crypto['price'])

            transformed_crypto = {
                'symbol': crypto['symbol'],
                'price': price,
                'currency': crypto['currency'],
                'timestamp': crypto['timestamp'],
                'extracted_at': datetime.now().isoformat(),
            }

            transformed.append(transformed_crypto)
            logger.info(f"Transformed {crypto['symbol']}: ${price:,.2f}")

        try:
            for crypto in transformed:
                if crypto['price'] <= 0:
                    raise ValueError(f"{crypto['symbol']}: Invalid price ${crypto['price']}")

            log_data_stats(transformed, "crypto_data")

        except Exception as e:
            logger.error(f"Data quality check failed: {str(e)}")
            raise

        transformed_key = f"tmp/crypto/transformed/{date_str}.json"
        s3_write_json(s3, bucket, transformed_key, transformed)
        logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

        logger.info(f"Successfully transformed {len(transformed)} cryptocurrencies")
        return transformed

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


def load_to_s3():
    try:
        s3, bucket = _s3_client()

        date_str = get_date_str()
        transformed_key = f"tmp/crypto/transformed/{date_str}.json"
        data = s3_read_json(s3, bucket, transformed_key)

        timestamp_str = get_date_str('timestamp')
        key = f"crypto/date={date_str}/{timestamp_str}.parquet"
        s3_write_parquet(s3, bucket, key, data)
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")

        response = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Upload verified. File size: {response['ContentLength']} bytes")

        athena = _athena_client()
        location = f"s3://{bucket}/crypto/date={date_str}/"
        register_athena_partition(athena, 'crypto', 'date', date_str, location)
        logger.info(f"Registered Athena partition date={date_str} for crypto")

        return f"s3://{bucket}/{key}"

    except Exception as e:
        logger.error(f"Upload to S3 failed: {str(e)}")
        raise


extract_task = PythonOperator(
    task_id='extract_crypto_data',
    python_callable=extract_crypto_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_crypto_data',
    python_callable=transform_crypto_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> transform_task >> load_task
