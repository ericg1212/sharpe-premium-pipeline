import boto3
import os

from config import S3_BUCKET


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
