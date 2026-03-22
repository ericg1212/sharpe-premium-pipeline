from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import os
from time import sleep
from config import WEATHER_CITY
from utils import (
    _s3_client, _athena_client, get_date_str,
    s3_read_json, s3_write_json, s3_write_parquet, register_athena_partition
)
from data_quality import validate_weather_data, log_data_stats

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
    'weather_pipeline',
    default_args=default_args,
    description='Extract weather data and load to S3 with error handling',
    schedule_interval='0 9 * * *',
    catchup=False,
)


# Task 1: Extract weather data with retry logic
def extract_weather():
    api_key = os.environ['OPENWEATHER_API_KEY']
    city = WEATHER_CITY
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=imperial"

    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} to fetch weather data")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Validate response has required fields
            required_fields = ['main', 'weather', 'wind', 'name']
            if not all(field in data for field in required_fields):
                raise ValueError("Missing required fields in API response")

            s3, bucket = _s3_client()
            date_str = get_date_str()
            tmp_key = f"tmp/weather/raw/{date_str}.json"
            s3_write_json(s3, bucket, tmp_key, data)
            logger.info(f"Raw data written to s3://{bucket}/{tmp_key}")

            logger.info(f"Successfully extracted weather data for {city}")
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
                continue
            else:
                raise Exception("API timeout after all retries")

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
                continue
            else:
                raise Exception(f"API request failed after all retries: {str(e)}")

        except ValueError as e:
            logger.error(f"Invalid data format: {str(e)}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise


# Task 2: Transform data with validation
def transform_weather():
    try:
        s3, bucket = _s3_client()
        date_str = get_date_str()
        raw_key = f"tmp/weather/raw/{date_str}.json"
        data = s3_read_json(s3, bucket, raw_key)

        # Validate data before transformation
        if not data.get('main') or not data.get('weather'):
            raise ValueError("Invalid weather data structure")

        # Extract key fields with defaults
        transformed = {
            'city': data.get('name', 'Unknown'),
            'temperature': data['main'].get('temp', 0),
            'feels_like': data['main'].get('feels_like', 0),
            'humidity': data['main'].get('humidity', 0),
            'description': data['weather'][0].get('description', 'unknown') if data.get('weather') else 'unknown',
            'wind_speed': data.get('wind', {}).get('speed', 0),
            'timestamp': datetime.now().isoformat(),
        }

        # DATA QUALITY CHECK
        try:
            validate_weather_data(transformed)
            log_data_stats(transformed, "weather_data")
        except Exception as e:
            logger.error(f"Data quality check failed: {str(e)}")
            raise

        transformed_key = f"tmp/weather/transformed/{date_str}.json"
        s3_write_json(s3, bucket, transformed_key, transformed)
        logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

        logger.info(f"Successfully transformed weather data: {transformed}")
        return transformed

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


# Task 3: Load to S3 with error handling
def load_to_s3():
    try:
        s3, bucket = _s3_client()

        date_str = get_date_str()
        transformed_key = f"tmp/weather/transformed/{date_str}.json"
        data = s3_read_json(s3, bucket, transformed_key)

        timestamp_str = get_date_str('timestamp')
        key = f"weather/date={date_str}/{timestamp_str}.parquet"
        s3_write_parquet(s3, bucket, key, [data])  # wrap single record in list for Parquet
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")

        # Verify upload
        response = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Upload verified. File size: {response['ContentLength']} bytes")

        athena = _athena_client()
        location = f"s3://{bucket}/weather/date={date_str}/"
        register_athena_partition(athena, 'weather', 'date', date_str, location)
        logger.info(f"Registered Athena partition date={date_str} for weather")

        return f"s3://{bucket}/{key}"

    except Exception as e:
        logger.error(f"Upload to S3 failed: {str(e)}")
        raise


# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather',
    python_callable=extract_weather,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather',
    python_callable=transform_weather,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag,
)

# Set dependencies
extract_task >> transform_task >> load_task
