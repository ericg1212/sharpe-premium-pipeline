from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import logging
import os
from time import sleep
from config import WEATHER_CITY, GLUE_DATABASE, ATHENA_WORKGROUP
from utils import _s3_client
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

            # Write raw data to S3 temp location (shared across workers)
            s3, bucket = _s3_client()
            date_str = datetime.now().strftime('%Y-%m-%d')
            tmp_key = f"tmp/weather/raw/{date_str}.json"
            s3.put_object(Bucket=bucket, Key=tmp_key, Body=json.dumps(data), ContentType='application/json')
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
        # Read raw data from S3 temp location
        s3, bucket = _s3_client()
        date_str = datetime.now().strftime('%Y-%m-%d')
        raw_key = f"tmp/weather/raw/{date_str}.json"
        raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
        data = json.loads(raw_obj['Body'].read())

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

        # Write transformed data to S3 temp location
        transformed_key = f"tmp/weather/transformed/{date_str}.json"
        s3.put_object(
            Bucket=bucket,
            Key=transformed_key,
            Body=json.dumps(transformed),
            ContentType='application/json',
        )
        logger.info(f"Transformed data written to s3://{bucket}/{transformed_key}")

        logger.info(f"Successfully transformed weather data: {transformed}")
        return transformed

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in raw data: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


# Task 3: Load to S3 with error handling
def load_to_s3():
    try:
        s3, bucket = _s3_client()

        # Read transformed data from S3 temp location
        date_str = datetime.now().strftime('%Y-%m-%d')
        transformed_key = f"tmp/weather/transformed/{date_str}.json"
        transformed_obj = s3.get_object(Bucket=bucket, Key=transformed_key)
        data = json.loads(transformed_obj['Body'].read())

        # Upload to S3
        timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        key = f"weather/date={date_str}/{timestamp_str}.json"

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )

        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")

        # Verify upload
        response = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Upload verified. File size: {response['ContentLength']} bytes")

        # Register partition with Athena so it's queryable immediately
        athena = boto3.client(
            'athena',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        location = f"s3://{bucket}/weather/date={date_str}/"
        athena.start_query_execution(
            QueryString=(
                f"ALTER TABLE weather ADD IF NOT EXISTS "
                f"PARTITION (date='{date_str}') LOCATION '{location}'"
            ),
            QueryExecutionContext={'Database': GLUE_DATABASE},
            WorkGroup=ATHENA_WORKGROUP,
        )
        logger.info(f"Registered Athena partition date={date_str} for weather")

        return f"s3://{bucket}/{key}"

    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"S3 upload failed: {str(e)}")
        raise

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
