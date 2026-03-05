from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import os
from config import WEATHER_CITY
from utils import _s3_client, _athena_client, get_date_str, s3_read_json, s3_write_json, s3_write_parquet, register_athena_partition
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

    s3, bucket = _s3_client()
    date_str = get_date_str()
    tmp_key = f"tmp/forecast/raw/{date_str}.json"
    s3_write_json(s3, bucket, tmp_key, data)
    logger.info(f"Raw data written to s3://{bucket}/{tmp_key}")

    logger.info(f"Extracted {len(data['list'])} forecast periods for {city}")
    return len(data['list'])


def transform_forecast():
    s3, bucket = _s3_client()
    date_str = get_date_str()
    raw_key = f"tmp/forecast/raw/{date_str}.json"
    data = s3_read_json(s3, bucket, raw_key)

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

    transformed_key = f"tmp/forecast/transformed/{date_str}.json"
    s3_write_json(s3, bucket, transformed_key, records)
    logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

    log_data_stats(records, "forecast_data")
    logger.info(f"Transformed {len(records)} forecast periods")
    return len(records)


def load_to_s3():
    s3, bucket = _s3_client()

    date_str = get_date_str()
    transformed_key = f"tmp/forecast/transformed/{date_str}.json"
    data = s3_read_json(s3, bucket, transformed_key)

    timestamp_str = get_date_str('timestamp')
    key = f"forecast/date={date_str}/{timestamp_str}.parquet"
    s3_write_parquet(s3, bucket, key, data)

    response = s3.head_object(Bucket=bucket, Key=key)
    logger.info(f"Uploaded to s3://{bucket}/{key} ({response['ContentLength']} bytes)")

    athena = _athena_client()
    location = f"s3://{bucket}/forecast/date={date_str}/"
    register_athena_partition(athena, 'forecast', 'date', date_str, location)
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
