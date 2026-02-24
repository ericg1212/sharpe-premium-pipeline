from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
import json
import os
from config import S3_BUCKET

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 2),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'pipeline_monitor',
    default_args=default_args,
    description='Monitor pipeline health and data quality',
    schedule_interval='0 18 * * *',
    catchup=False,
)

def get_s3_client():
    """Create S3 client using environment variables"""
    return boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    )

def check_weather_pipeline():
    """Check if weather data was loaded today"""
    try:
        s3 = get_s3_client()
        bucket = os.environ.get('S3_BUCKET', S3_BUCKET)
        
        today = datetime.now().strftime('%Y-%m-%d')
        prefix = f"weather/{today}/"
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"⚠️ No weather data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}
        
        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        file_key = latest_file['Key']
        
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        data = json.loads(obj['Body'].read())
        
        if data.get('temperature') and data.get('city'):
            logger.info(f"✅ Weather pipeline healthy: {data['city']} - {data['temperature']}°F")
            return {
                'status': 'OK',
                'city': data['city'],
                'temperature': data['temperature'],
                'last_update': str(latest_file['LastModified'])
            }
        else:
            logger.error("❌ Weather data incomplete")
            return {'status': 'ERROR', 'message': 'Incomplete data'}
            
    except Exception as e:
        logger.error(f"❌ Weather pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}

def check_stock_pipeline():
    """Check if stock data was loaded today"""
    try:
        s3 = get_s3_client()
        bucket = os.environ.get('S3_BUCKET', S3_BUCKET)

        today = datetime.now().strftime('%Y-%m-%d')
        prefix = f"stocks/{today}/"
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"⚠️ No stock data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}
        
        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        file_key = latest_file['Key']
        
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        lines = obj['Body'].read().decode('utf-8').strip().split('\n')
        data = [json.loads(line) for line in lines]
        
        if len(data) == 10:
            symbols = [s['symbol'] for s in data]
            prices = {s['symbol']: s['price'] for s in data}
            logger.info(f"✅ Stock pipeline healthy: {symbols} - Prices: {prices}")
            return {
                'status': 'OK',
                'stocks': symbols,
                'prices': prices,
                'last_update': str(latest_file['LastModified'])
            }
        else:
            logger.error(f"❌ Expected 10 stocks, got {len(data)}")
            return {'status': 'ERROR', 'message': f'Expected 10 stocks, got {len(data)}'}
            
    except Exception as e:
        logger.error(f"❌ Stock pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}

def check_crypto_pipeline():
    """Check if crypto data was loaded today"""
    try:
        s3 = get_s3_client()
        bucket = os.environ.get('S3_BUCKET', S3_BUCKET)

        today = datetime.now().strftime('%Y-%m-%d')
        prefix = f"crypto/{today}/"
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"⚠️ No crypto data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}
        
        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        file_key = latest_file['Key']
        
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        lines = obj['Body'].read().decode('utf-8').strip().split('\n')
        data = [json.loads(line) for line in lines]
        
        if len(data) == 3:
            symbols = [c['symbol'] for c in data]
            prices = {c['symbol']: f"${c['price']:,.2f}" for c in data}
            logger.info(f"✅ Crypto pipeline healthy: {symbols} - Prices: {prices}")
            return {
                'status': 'OK',
                'cryptos': symbols,
                'prices': prices,
                'last_update': str(latest_file['LastModified'])
            }
        else:
            logger.error(f"❌ Expected 3 cryptos, got {len(data)}")
            return {'status': 'ERROR', 'message': f'Expected 3 cryptos, got {len(data)}'}
            
    except Exception as e:
        logger.error(f"❌ Crypto pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}

def generate_health_report(**context):
    """Generate overall health report"""
    ti = context['ti']
    
    weather_status = ti.xcom_pull(task_ids='check_weather_pipeline')
    stock_status = ti.xcom_pull(task_ids='check_stock_pipeline')
    crypto_status = ti.xcom_pull(task_ids='check_crypto_pipeline')
    
    logger.info("=" * 50)
    logger.info("📊 PIPELINE HEALTH REPORT")
    logger.info("=" * 50)
    
    logger.info(f"Weather Pipeline: {weather_status.get('status', 'UNKNOWN')}")
    if weather_status.get('status') == 'OK':
        logger.info(f"  ├─ City: {weather_status.get('city')}")
        logger.info(f"  └─ Temp: {weather_status.get('temperature')}°F")
    else:
        logger.info(f"  └─ Issue: {weather_status.get('message')}")
    
    logger.info(f"Stock Pipeline: {stock_status.get('status', 'UNKNOWN')}")
    if stock_status.get('status') == 'OK':
        logger.info(f"  ├─ Stocks: {stock_status.get('stocks')}")
        logger.info(f"  └─ Prices: {stock_status.get('prices')}")
    else:
        logger.info(f"  └─ Issue: {stock_status.get('message')}")
    
    logger.info(f"Crypto Pipeline: {crypto_status.get('status', 'UNKNOWN')}")
    if crypto_status.get('status') == 'OK':
        logger.info(f"  ├─ Cryptos: {crypto_status.get('cryptos')}")
        logger.info(f"  └─ Prices: {crypto_status.get('prices')}")
    else:
        logger.info(f"  └─ Issue: {crypto_status.get('message')}")
    
    logger.info("=" * 50)
    
    if (weather_status.get('status') == 'OK'
            and stock_status.get('status') == 'OK'
            and crypto_status.get('status') == 'OK'):
        logger.info("✅ All pipelines healthy!")
        return 'HEALTHY'
    else:
        logger.warning("⚠️ Some pipelines need attention")
        return 'NEEDS_ATTENTION'

check_weather_task = PythonOperator(
    task_id='check_weather_pipeline',
    python_callable=check_weather_pipeline,
    dag=dag,
)

check_stock_task = PythonOperator(
    task_id='check_stock_pipeline',
    python_callable=check_stock_pipeline,
    dag=dag,
)

check_crypto_task = PythonOperator(
    task_id='check_crypto_pipeline',
    python_callable=check_crypto_pipeline,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    provide_context=True,
    dag=dag,
)

[check_weather_task, check_stock_task, check_crypto_task] >> report_task