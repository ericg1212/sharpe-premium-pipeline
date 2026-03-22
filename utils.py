import boto3
import io
import json
import logging
import os
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from config import S3_BUCKET, GLUE_DATABASE, ATHENA_WORKGROUP

logger = logging.getLogger(__name__)


def _s3_client():
    """Return (s3_client, bucket_name) tuple."""
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )
    bucket = os.environ.get('S3_BUCKET', S3_BUCKET)
    return client, bucket


def _athena_client():
    """Return an Athena client."""
    return boto3.client(
        'athena',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )


def get_date_str(fmt='date'):
    """Return a formatted date string.

    fmt='date'      → 'YYYY-MM-DD'
    fmt='timestamp' → 'YYYY-MM-DD_HH-MM-SS'
    """
    if fmt == 'timestamp':
        return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return datetime.now().strftime('%Y-%m-%d')


def s3_read_json(s3, bucket, key):
    """Read and parse a JSON file from S3. Returns the parsed object."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read())


def s3_write_json(s3, bucket, key, data):
    """Write data as a JSON file to S3."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json',
    )


def s3_write_ndjson(s3, bucket, key, records):
    """Write a list of dicts to S3 as NDJSON (one JSON object per line)."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body='\n'.join(json.dumps(record) for record in records),
        ContentType='application/json',
    )


def s3_write_parquet(s3, bucket, key, records):
    """Write a list of dicts to S3 as Snappy-compressed Parquet."""
    table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.read(),
        ContentType='application/octet-stream',
    )


# Use run_etl() for new pipelines; existing pipelines pre-date this pattern.
def run_etl(name, extract_fn, transform_fn, load_fn, logger):
    logger.info(f"{name}: extract start")
    try:
        raw = extract_fn()
    except Exception as e:
        logger.error(f"{name}: extract failed: {e}")
        raise
    logger.info(f"{name}: extract success")

    logger.info(f"{name}: transform start")
    try:
        transformed = transform_fn(raw)
    except Exception as e:
        logger.error(f"{name}: transform failed: {e}")
        raise
    logger.info(f"{name}: transform success")

    logger.info(f"{name}: load start")
    try:
        result = load_fn(transformed)
    except Exception as e:
        logger.error(f"{name}: load failed: {e}")
        raise
    logger.info(f"{name}: load success")

    return result


def register_athena_partition(athena, table, partition_key, partition_value, s3_location):
    """Register a single date-style partition with Athena.

    Args:
        athena:           Athena client (from _athena_client())
        table:            Glue table name (e.g. 'stocks')
        partition_key:    Partition column name (e.g. 'date')
        partition_value:  Partition value (e.g. '2026-03-05')
        s3_location:      Full S3 prefix (e.g. 's3://bucket/stocks/date=2026-03-05/')
    """
    response = athena.start_query_execution(
        QueryString=(
            f"ALTER TABLE {table} ADD IF NOT EXISTS "
            f"PARTITION ({partition_key}='{partition_value}') LOCATION '{s3_location}'"
        ),
        QueryExecutionContext={'Database': GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )
    logger.info(
        f"Athena query {response['QueryExecutionId']} submitted: "
        f"{table} {partition_key}={partition_value}"
    )
