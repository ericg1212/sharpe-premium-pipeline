"""Tests for fred_pipeline: transform_fred_data and load_to_s3."""

import io
import json
import pytest
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from fred_pipeline.fred_pipeline import transform_fred_data, load_to_s3

# ── Sample FRED raw data ───────────────────────────────────────────────────────

# Mimics the payload written to S3 tmp/fred/raw/ by extract_fred_data().
# Includes one "." (missing) observation to verify it is filtered out.
RAW_FRED = {
    'GS10': {
        'series_id': 'GS10',
        'description': '10-Year Treasury Constant Maturity Rate',
        'units': 'percent',
        'observations': [
            {'date': '2023-01-01', 'value': '3.88'},
            {'date': '2023-02-01', 'value': '3.92'},
            {'date': '2023-03-01', 'value': '.'},   # missing — must be dropped
        ],
    },
    'UNRATE': {
        'series_id': 'UNRATE',
        'description': 'Unemployment Rate',
        'units': 'percent',
        'observations': [
            {'date': '2023-01-01', 'value': '3.4'},
            {'date': '2023-02-01', 'value': '3.6'},
        ],
    },
}

# Pre-built transformed records for seeding load_to_s3() tests
TRANSFORMED_RECORDS = [
    {'series_id': 'GS10', 'description': '10-Year Treasury Constant Maturity Rate',
     'units': 'percent', 'date': '2023-01-01', 'year': 2023, 'month': 1,
     'value': 3.88, 'extracted_at': '2026-02-26T12:00:00'},
    {'series_id': 'GS10', 'description': '10-Year Treasury Constant Maturity Rate',
     'units': 'percent', 'date': '2023-02-01', 'year': 2023, 'month': 2,
     'value': 3.92, 'extracted_at': '2026-02-26T12:00:00'},
    {'series_id': 'UNRATE', 'description': 'Unemployment Rate',
     'units': 'percent', 'date': '2023-01-01', 'year': 2023, 'month': 1,
     'value': 3.4, 'extracted_at': '2026-02-26T12:00:00'},
]


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_fred(s3_client):
    """Seed RAW_FRED into mocked S3 at the tmp/fred/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/fred/raw/{date_str}.json',
        Body=json.dumps(RAW_FRED),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_fred(s3_client):
    """Seed TRANSFORMED_RECORDS into mocked S3 at the tmp/fred/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/fred/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_RECORDS),
    )
    yield s3_client


# ── transform_fred_data tests ──────────────────────────────────────────────────

class TestTransformFredData:
    """Tests for transform_fred_data() — reads raw from S3, writes transformed to S3."""

    def test_drops_missing_dot_observations(self, s3_raw_fred):
        """FRED '.' values must be excluded from output."""
        transform_fred_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_fred.get_object(
            Bucket='test-bucket', Key=f'tmp/fred/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read())
        values = [r['value'] for r in records]
        assert '.' not in values

    def test_value_converted_to_float(self, s3_raw_fred):
        transform_fred_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_fred.get_object(
            Bucket='test-bucket', Key=f'tmp/fred/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read())
        for r in records:
            assert isinstance(r['value'], float)

    def test_year_and_month_extracted_from_date(self, s3_raw_fred):
        transform_fred_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_fred.get_object(
            Bucket='test-bucket', Key=f'tmp/fred/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read())
        gs10 = next(r for r in records if r['series_id'] == 'GS10' and r['date'] == '2023-02-01')

        assert gs10['year'] == 2023
        assert gs10['month'] == 2

    def test_output_record_count_excludes_missing(self, s3_raw_fred):
        """GS10 has 2 valid + 1 missing, UNRATE has 2 valid → 4 total records."""
        transform_fred_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_fred.get_object(
            Bucket='test-bucket', Key=f'tmp/fred/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read())
        assert len(records) == 4

    def test_output_records_contain_required_fields(self, s3_raw_fred):
        transform_fred_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_fred.get_object(
            Bucket='test-bucket', Key=f'tmp/fred/transformed/{date_str}.json'
        )
        record = json.loads(obj['Body'].read())[0]

        for field in ['series_id', 'description', 'units', 'date',
                      'year', 'month', 'value', 'extracted_at']:
            assert field in record

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key seeded → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_fred_data()


# ── load_to_s3 tests ───────────────────────────────────────────────────────────

class TestLoadFredToS3:
    """Tests for load_to_s3() — partitions records by (series_id, year)."""

    def test_writes_to_macro_indicators_prefix(self, s3_transformed_fred):
        load_to_s3()

        objects = s3_transformed_fred.list_objects_v2(
            Bucket='test-bucket', Prefix='macro_indicators/'
        )
        assert objects['KeyCount'] >= 1

    def test_partition_key_contains_series_and_year(self, s3_transformed_fred):
        """Keys follow macro_indicators/series={id}/year={year}/data.parquet pattern."""
        load_to_s3()

        objects = s3_transformed_fred.list_objects_v2(
            Bucket='test-bucket', Prefix='macro_indicators/'
        )
        keys = [obj['Key'] for obj in objects['Contents']]
        assert any('series=GS10' in k and 'year=2023' in k for k in keys)
        assert any('series=UNRATE' in k and 'year=2023' in k for k in keys)

    def test_one_file_per_series_year_partition(self, s3_transformed_fred):
        """GS10/2023 and UNRATE/2023 → 2 separate S3 files."""
        load_to_s3()

        objects = s3_transformed_fred.list_objects_v2(
            Bucket='test-bucket', Prefix='macro_indicators/'
        )
        assert len(objects['Contents']) == 2

    def test_output_is_parquet_with_required_fields(self, s3_transformed_fred):
        """S3 file is valid Parquet containing series_id and value columns."""
        load_to_s3()

        objects = s3_transformed_fred.list_objects_v2(
            Bucket='test-bucket', Prefix='macro_indicators/'
        )
        key = objects['Contents'][0]['Key']
        body = s3_transformed_fred.get_object(
            Bucket='test-bucket', Key=key
        )['Body'].read()

        records = pq.read_table(io.BytesIO(body)).to_pylist()
        for record in records:
            assert 'series_id' in record
            assert 'value' in record

    def test_raises_on_missing_transformed_data(self, s3_client):
        """No transformed key seeded → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            load_to_s3()
