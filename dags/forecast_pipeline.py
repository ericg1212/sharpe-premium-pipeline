from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import logging
import os
from config import WEATHER_CITY, GLUE_DATABASE, ATHENA_WORKGROUP
from utils import _s3_client
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
    'forecast_pipeline',
    default_args=default_args,
    description='Extract 5-day weather forecast and load to S3',
    schedule_interval='0 6 * * *',
    catchup=False,
)


def extract_forecast():
    api_key = os.environ['OPENWEATHER_API_KEY']
    city = WEATHER_CITY
    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?q={city}&appid={api_key}&units=imperial"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    if 'list' not in data:
        raise ValueError(f"Unexpected API response: {list(data.keys())}")

    # Write raw data to S3 temp location (shared across workers)
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')
    tmp_key = f"tmp/forecast/raw/{date_str}.json"
    s3.put_object(Bucket=bucket, Key=tmp_key, Body=json.dumps(data), ContentType='application/json')
    logger.info(f"Raw data written to s3://{bucket}/{tmp_key}")

    logger.info(f"Extracted {len(data['list'])} forecast periods for {city}")
    return len(data['list'])


def transform_forecast():
    # Read raw data from S3 temp location
    s3, bucket = _s3_client()
    date_str = datetime.now().strftime('%Y-%m-%d')
    raw_key = f"tmp/forecast/raw/{date_str}.json"
    raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
    data = json.loads(raw_obj['Body'].read())

    city = data.get('city', {}).get('name', WEATHER_CITY)
    records = []

    for period in data['list']:
        record = {
            'city': city,
            'forecast_time': period['dt_txt'],
            'temperature': period['main']['temp'],
            'feels_like': period['main']['feels_like'],
            'humidity': period['main']['humidity'],
            'description': period['weather'][0]['description'],
            'wind_speed': period.get('wind', {}).get('speed', 0),
            'timestamp': datetime.now().isoformat(),
        }
        records.append(record)

    # Write transformed NDJSON to S3 temp location
    transformed_key = f"tmp/forecast/transformed/{date_str}.json"
    ndjson_body = '\n'.join(json.dumps(r) for r in records)
    s3.put_object(Bucket=bucket, Key=transformed_key, Body=ndjson_body, ContentType='application/json')
    logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

    log_data_stats(records, "forecast_data")
    logger.info(f"Transformed {len(records)} forecast periods")
    return len(records)


def load_to_s3():
    s3, bucket = _s3_client()

    # Read transformed NDJSON from S3 temp location
    date_str = datetime.now().strftime('%Y-%m-%d')
    transformed_key = f"tmp/forecast/transformed/{date_str}.json"
    transformed_obj = s3.get_object(Bucket=bucket, Key=transformed_key)
    data = transformed_obj['Body'].read()

    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    key = f"forecast/date={date_str}/{timestamp_str}.json"

    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType='application/json')

    response = s3.head_object(Bucket=bucket, Key=key)
    logger.info(f"Uploaded to s3://{bucket}/{key} ({response['ContentLength']} bytes)")

    athena = boto3.client(
        'athena',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )
    location = f"s3://{bucket}/forecast/date={date_str}/"
    athena.start_query_execution(
        QueryString=(
            f"ALTER TABLE forecast ADD IF NOT EXISTS "
            f"PARTITION (date='{date_str}') LOCATION '{location}'"
        ),
        QueryExecutionContext={'Database': GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )
    logger.info(f"Registered Athena partition date={date_str} for forecast")

    return f"s3://{bucket}/{key}"


extract_task = PythonOperator(
    task_id='extract_forecast',
    python_callable=extract_forecast,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_forecast',
    python_callable=transform_forecast,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> transform_task >> load_task
