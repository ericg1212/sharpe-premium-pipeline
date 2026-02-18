from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import logging
import os
from time import sleep
from config import CRYPTO_SYMBOLS, S3_BUCKET
from data_quality import validate_stock_data, log_data_stats

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
    
    with open('/tmp/crypto_raw.json', 'w') as f:
        json.dump(all_data, f)
    
    logger.info(f"Extracted data for {len(all_data)} cryptocurrencies")
    return all_data

def transform_crypto_data():
    try:
        with open('/tmp/crypto_raw.json', 'r') as f:
            raw_data = json.load(f)
        
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
        
        with open('/tmp/crypto_transformed.json', 'w') as f:
            f.write('\n'.join([json.dumps(record) for record in transformed]))
        
        logger.info(f"Successfully transformed {len(transformed)} cryptocurrencies")
        return transformed
        
    except FileNotFoundError:
        logger.error("Raw crypto data file not found")
        raise
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise

def load_to_s3():
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        bucket = os.environ.get('S3_BUCKET', S3_BUCKET)

        with open('/tmp/crypto_transformed.json', 'r') as f:
            data = f.read()
        
        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        key = f"crypto/{date_str}/{timestamp_str}.json"
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='application/json'
        )
        
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        
        response = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Upload verified. File size: {response['ContentLength']} bytes")
        
        return f"s3://{bucket}/{key}"
        
    except FileNotFoundError:
        logger.error("Transformed crypto data file not found")
        raise
        
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
