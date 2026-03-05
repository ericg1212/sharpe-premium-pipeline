"""Tests for shared utils helpers."""

import json
import pytest
from unittest.mock import MagicMock, call
from moto import mock_aws
import boto3
import io
import pyarrow.parquet as pq
from utils import _s3_client, _athena_client, get_date_str, s3_read_json, s3_write_json, s3_write_ndjson, s3_write_parquet, register_athena_partition


class TestS3Client:
    """_s3_client() returns a working (client, bucket) tuple."""

    @mock_aws
    def test_returns_two_tuple(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        result = _s3_client()
        assert isinstance(result, tuple)
        assert len(result) == 2

    @mock_aws
    def test_bucket_comes_from_env_var(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'my-custom-bucket')
        _, bucket = _s3_client()
        assert bucket == 'my-custom-bucket'

    @mock_aws
    def test_bucket_falls_back_to_config_when_env_not_set(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.delenv('S3_BUCKET', raising=False)
        from config import S3_BUCKET
        _, bucket = _s3_client()
        assert bucket == S3_BUCKET

    @mock_aws
    def test_default_region_is_us_east_1(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        client, _ = _s3_client()
        assert client.meta.region_name == 'us-east-1'

    @mock_aws
    def test_custom_region_is_used(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-west-2')
        client, _ = _s3_client()
        assert client.meta.region_name == 'us-west-2'

    @mock_aws
    def test_missing_access_key_raises_key_error(self, monkeypatch):
        monkeypatch.delenv('AWS_ACCESS_KEY_ID', raising=False)
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        with pytest.raises(KeyError):
            _s3_client()


class TestAthenaClient:
    """_athena_client() returns a usable Athena boto3 client."""

    @mock_aws
    def test_returns_athena_client(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        client = _athena_client()
        assert client.meta.service_model.service_name == 'athena'

    @mock_aws
    def test_default_region_is_us_east_1(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        client = _athena_client()
        assert client.meta.region_name == 'us-east-1'

    @mock_aws
    def test_custom_region_is_used(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'eu-west-1')
        client = _athena_client()
        assert client.meta.region_name == 'eu-west-1'


class TestGetDateStr:
    """get_date_str() returns correctly formatted date strings."""

    def test_default_returns_date_format(self):
        result = get_date_str()
        # Should match YYYY-MM-DD
        import re
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', result)

    def test_date_format_explicit(self):
        import re
        result = get_date_str('date')
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', result)

    def test_timestamp_format(self):
        import re
        result = get_date_str('timestamp')
        # Should match YYYY-MM-DD_HH-MM-SS
        assert re.match(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$', result)

    def test_timestamp_longer_than_date(self):
        assert len(get_date_str('timestamp')) > len(get_date_str('date'))


class TestS3ReadJson:
    """s3_read_json() reads and parses a JSON file from S3."""

    @mock_aws
    def test_reads_dict(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.put_object(Bucket='test-bucket', Key='data.json', Body=json.dumps({'key': 'value'}))
        result = s3_read_json(s3, 'test-bucket', 'data.json')
        assert result == {'key': 'value'}

    @mock_aws
    def test_reads_list(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        data = [{'a': 1}, {'b': 2}]
        s3.put_object(Bucket='test-bucket', Key='list.json', Body=json.dumps(data))
        result = s3_read_json(s3, 'test-bucket', 'list.json')
        assert result == data

    @mock_aws
    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        with pytest.raises(Exception):
            s3_read_json(s3, 'test-bucket', 'nonexistent.json')


class TestS3WriteJson:
    """s3_write_json() writes a Python object as JSON to S3."""

    @mock_aws
    def test_writes_dict(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3_write_json(s3, 'test-bucket', 'out.json', {'x': 42})
        body = s3.get_object(Bucket='test-bucket', Key='out.json')['Body'].read()
        assert json.loads(body) == {'x': 42}

    @mock_aws
    def test_writes_list(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        data = [1, 2, 3]
        s3_write_json(s3, 'test-bucket', 'list.json', data)
        body = s3.get_object(Bucket='test-bucket', Key='list.json')['Body'].read()
        assert json.loads(body) == data

    @mock_aws
    def test_content_type_is_json(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3_write_json(s3, 'test-bucket', 'out.json', {})
        head = s3.head_object(Bucket='test-bucket', Key='out.json')
        assert head['ContentType'] == 'application/json'


class TestS3WriteNdjson:
    """s3_write_ndjson() writes a list of dicts as NDJSON to S3."""

    @mock_aws
    def test_each_line_is_valid_json(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        records = [{'a': 1}, {'b': 2}, {'c': 3}]
        s3_write_ndjson(s3, 'test-bucket', 'out.ndjson', records)
        body = s3.get_object(Bucket='test-bucket', Key='out.ndjson')['Body'].read().decode()
        lines = body.strip().split('\n')
        assert len(lines) == 3
        parsed = [json.loads(line) for line in lines]
        assert parsed == records

    @mock_aws
    def test_single_record(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3_write_ndjson(s3, 'test-bucket', 'single.ndjson', [{'only': 'one'}])
        body = s3.get_object(Bucket='test-bucket', Key='single.ndjson')['Body'].read().decode()
        assert json.loads(body) == {'only': 'one'}

    @mock_aws
    def test_content_type_is_json(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3_write_ndjson(s3, 'test-bucket', 'out.ndjson', [{'x': 1}])
        head = s3.head_object(Bucket='test-bucket', Key='out.ndjson')
        assert head['ContentType'] == 'application/json'


class TestS3WriteParquet:
    """s3_write_parquet() writes a list of dicts as Parquet to S3."""

    @mock_aws
    def test_written_file_is_valid_parquet(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        records = [{'symbol': 'BTC', 'price': 95000.0}, {'symbol': 'ETH', 'price': 3400.0}]
        s3_write_parquet(s3, 'test-bucket', 'crypto/data.parquet', records)
        body = s3.get_object(Bucket='test-bucket', Key='crypto/data.parquet')['Body'].read()
        result = pq.read_table(io.BytesIO(body)).to_pylist()
        assert result == records

    @mock_aws
    def test_record_count_matches_input(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        records = [{'x': i} for i in range(5)]
        s3_write_parquet(s3, 'test-bucket', 'out.parquet', records)
        body = s3.get_object(Bucket='test-bucket', Key='out.parquet')['Body'].read()
        result = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(result) == 5

    @mock_aws
    def test_single_record_list(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3_write_parquet(s3, 'test-bucket', 'single.parquet', [{'city': 'Brooklyn', 'temp': 45.2}])
        body = s3.get_object(Bucket='test-bucket', Key='single.parquet')['Body'].read()
        result = pq.read_table(io.BytesIO(body)).to_pylist()
        assert result[0]['city'] == 'Brooklyn'


class TestRegisterAthenaPartition:
    """register_athena_partition() calls Athena with correct ALTER TABLE SQL."""

    def test_calls_start_query_execution(self):
        athena = MagicMock()
        register_athena_partition(athena, 'stocks', 'date', '2026-03-05', 's3://bucket/stocks/date=2026-03-05/')
        athena.start_query_execution.assert_called_once()

    def test_sql_contains_table_name(self):
        athena = MagicMock()
        register_athena_partition(athena, 'stocks', 'date', '2026-03-05', 's3://bucket/stocks/date=2026-03-05/')
        sql = athena.start_query_execution.call_args[1]['QueryString']
        assert 'stocks' in sql

    def test_sql_contains_partition_value(self):
        athena = MagicMock()
        register_athena_partition(athena, 'weather', 'date', '2026-01-01', 's3://bucket/weather/date=2026-01-01/')
        sql = athena.start_query_execution.call_args[1]['QueryString']
        assert '2026-01-01' in sql

    def test_sql_contains_location(self):
        athena = MagicMock()
        location = 's3://my-bucket/forecast/date=2026-03-05/'
        register_athena_partition(athena, 'forecast', 'date', '2026-03-05', location)
        sql = athena.start_query_execution.call_args[1]['QueryString']
        assert location in sql

    def test_sql_uses_add_if_not_exists(self):
        athena = MagicMock()
        register_athena_partition(athena, 'crypto', 'date', '2026-03-05', 's3://b/crypto/date=2026-03-05/')
        sql = athena.start_query_execution.call_args[1]['QueryString']
        assert 'ADD IF NOT EXISTS' in sql
