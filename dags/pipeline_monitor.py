from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import io
import logging
import pyarrow.parquet as pq
from utils import _s3_client, get_date_str

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


def _read_parquet_from_s3(s3, bucket, key):
    """Download a Parquet file from S3 and return as list of dicts."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))
    return table.to_pylist()


def check_weather_pipeline():
    """Check if weather data was loaded today."""
    try:
        s3, bucket = _s3_client()
        today = get_date_str()
        prefix = f"weather/date={today}/"

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"No weather data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}

        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        records = _read_parquet_from_s3(s3, bucket, latest_file['Key'])
        data = records[0]  # weather is one record per file

        if data.get('temperature') and data.get('city'):
            logger.info(f"Weather pipeline healthy: {data['city']} - {data['temperature']}F")
            return {
                'status': 'OK',
                'city': data['city'],
                'temperature': data['temperature'],
                'last_update': str(latest_file['LastModified']),
            }
        else:
            logger.error("Weather data incomplete")
            return {'status': 'ERROR', 'message': 'Incomplete data'}

    except Exception as e:
        logger.error(f"Weather pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_stock_pipeline():
    """Check if stock data was loaded today."""
    try:
        s3, bucket = _s3_client()
        today = get_date_str()
        prefix = f"stocks/date={today}/"

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"No stock data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}

        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        data = _read_parquet_from_s3(s3, bucket, latest_file['Key'])

        if len(data) == 10:
            symbols = [s['symbol'] for s in data]
            prices = {s['symbol']: s['price'] for s in data}
            logger.info(f"Stock pipeline healthy: {symbols}")
            return {
                'status': 'OK',
                'stocks': symbols,
                'prices': prices,
                'last_update': str(latest_file['LastModified']),
            }
        else:
            logger.error(f"Expected 10 stocks, got {len(data)}")
            return {'status': 'ERROR', 'message': f'Expected 10 stocks, got {len(data)}'}

    except Exception as e:
        logger.error(f"Stock pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_crypto_pipeline():
    """Check if crypto data was loaded today."""
    try:
        s3, bucket = _s3_client()
        today = get_date_str()
        prefix = f"crypto/date={today}/"

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"No crypto data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}

        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        data = _read_parquet_from_s3(s3, bucket, latest_file['Key'])

        if len(data) == 3:
            symbols = [c['symbol'] for c in data]
            prices = {c['symbol']: f"${c['price']:,.2f}" for c in data}
            logger.info(f"Crypto pipeline healthy: {symbols}")
            return {
                'status': 'OK',
                'cryptos': symbols,
                'prices': prices,
                'last_update': str(latest_file['LastModified']),
            }
        else:
            logger.error(f"Expected 3 cryptos, got {len(data)}")
            return {'status': 'ERROR', 'message': f'Expected 3 cryptos, got {len(data)}'}

    except Exception as e:
        logger.error(f"Crypto pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_edgar_pipeline():
    """Check if EDGAR fundamentals data was loaded within the last 120 days (quarterly cadence)."""
    try:
        s3, bucket = _s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='fundamentals/')

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning("No EDGAR fundamentals data found")
            return {'status': 'WARNING', 'message': 'No fundamentals data found'}

        latest = max(response['Contents'], key=lambda x: x['LastModified'])
        days_old = (datetime.now(latest['LastModified'].tzinfo) - latest['LastModified']).days

        if days_old > 120:  # quarterly = ~90 days + 30-day buffer
            logger.warning(f"EDGAR data is {days_old} days old — expected <= 120")
            return {'status': 'WARNING', 'message': f'Data is {days_old} days old (expected <=120)'}

        partitions = len(response['Contents'])
        logger.info(f"EDGAR pipeline healthy: {partitions} partition files, latest {days_old}d ago")
        return {
            'status': 'OK',
            'partitions': partitions,
            'days_since_update': days_old,
            'last_update': str(latest['LastModified']),
        }

    except Exception as e:
        logger.error(f"EDGAR pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_fred_pipeline():
    """Check if FRED macro data was loaded within the last 35 days (monthly + 2-week lag buffer)."""
    try:
        s3, bucket = _s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='macro_indicators/')

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning("No FRED macro data found")
            return {'status': 'WARNING', 'message': 'No macro_indicators data found'}

        latest = max(response['Contents'], key=lambda x: x['LastModified'])
        days_old = (datetime.now(latest['LastModified'].tzinfo) - latest['LastModified']).days

        if days_old > 35:  # monthly + 2-week FRED lag + buffer
            logger.warning(f"FRED data is {days_old} days old — expected <= 35")
            return {'status': 'WARNING', 'message': f'Data is {days_old} days old (expected <=35)'}

        partitions = len(response['Contents'])
        logger.info(f"FRED pipeline healthy: {partitions} partition files, latest {days_old}d ago")
        return {
            'status': 'OK',
            'partitions': partitions,
            'days_since_update': days_old,
            'last_update': str(latest['LastModified']),
        }

    except Exception as e:
        logger.error(f"FRED pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def generate_health_report(**context):
    """Generate overall health report for all 5 finance pipelines."""
    ti = context['ti']

    weather_status = ti.xcom_pull(task_ids='check_weather_pipeline')
    stock_status = ti.xcom_pull(task_ids='check_stock_pipeline')
    crypto_status = ti.xcom_pull(task_ids='check_crypto_pipeline')
    edgar_status = ti.xcom_pull(task_ids='check_edgar_pipeline')
    fred_status = ti.xcom_pull(task_ids='check_fred_pipeline')

    logger.info("=" * 50)
    logger.info("PIPELINE HEALTH REPORT")
    logger.info("=" * 50)

    logger.info(f"Weather Pipeline: {weather_status.get('status', 'UNKNOWN')}")
    if weather_status.get('status') == 'OK':
        logger.info(f"  |- City: {weather_status.get('city')}")
        logger.info(f"  |- Temp: {weather_status.get('temperature')}F")
    else:
        logger.info(f"  |- Issue: {weather_status.get('message')}")

    logger.info(f"Stock Pipeline: {stock_status.get('status', 'UNKNOWN')}")
    if stock_status.get('status') == 'OK':
        logger.info(f"  |- Stocks: {stock_status.get('stocks')}")
        logger.info(f"  |- Prices: {stock_status.get('prices')}")
    else:
        logger.info(f"  |- Issue: {stock_status.get('message')}")

    logger.info(f"Crypto Pipeline: {crypto_status.get('status', 'UNKNOWN')}")
    if crypto_status.get('status') == 'OK':
        logger.info(f"  |- Cryptos: {crypto_status.get('cryptos')}")
        logger.info(f"  |- Prices: {crypto_status.get('prices')}")
    else:
        logger.info(f"  |- Issue: {crypto_status.get('message')}")

    logger.info(f"EDGAR Pipeline: {edgar_status.get('status', 'UNKNOWN')}")
    if edgar_status.get('status') == 'OK':
        logger.info(f"  |- Partitions: {edgar_status.get('partitions')}")
        logger.info(f"  |- Days since update: {edgar_status.get('days_since_update')}")
    else:
        logger.info(f"  |- Issue: {edgar_status.get('message')}")

    logger.info(f"FRED Pipeline: {fred_status.get('status', 'UNKNOWN')}")
    if fred_status.get('status') == 'OK':
        logger.info(f"  |- Partitions: {fred_status.get('partitions')}")
        logger.info(f"  |- Days since update: {fred_status.get('days_since_update')}")
    else:
        logger.info(f"  |- Issue: {fred_status.get('message')}")

    logger.info("=" * 50)

    all_ok = all(
        s.get('status') == 'OK'
        for s in [weather_status, stock_status, crypto_status, edgar_status, fred_status]
    )
    if all_ok:
        logger.info("All pipelines healthy!")
        return 'HEALTHY'
    else:
        logger.warning("Some pipelines need attention")
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

check_edgar_task = PythonOperator(
    task_id='check_edgar_pipeline',
    python_callable=check_edgar_pipeline,
    dag=dag,
)

check_fred_task = PythonOperator(
    task_id='check_fred_pipeline',
    python_callable=check_fred_pipeline,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    provide_context=True,
    dag=dag,
)

[check_weather_task, check_stock_task, check_crypto_task,
 check_edgar_task, check_fred_task] >> report_task
