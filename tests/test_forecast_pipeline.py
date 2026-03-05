"""Tests for forecast_pipeline DAG: transform and load functions."""

import io
import json
import pytest
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from forecast_pipeline.forecast_pipeline import transform_forecast, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_FORECAST = {
    'city': {'name': 'Brooklyn'},
    'list': [
        {
            'dt_txt': '2026-02-19 06:00:00',
            'main': {'temp': 38.5, 'feels_like': 32.1, 'humidity': 88},
            'weather': [{'description': 'light snow'}],
            'wind': {'speed': 8.2},
        },
        {
            'dt_txt': '2026-02-19 09:00:00',
            'main': {'temp': 40.1, 'feels_like': 34.0, 'humidity': 82},
            'weather': [{'description': 'overcast clouds'}],
            'wind': {'speed': 6.5},
        },
    ],
}

TRANSFORMED_FORECAST = [
    {
        'city': 'Brooklyn',
        'forecast_time': '2026-02-19 06:00:00',
        'temperature': 38.5,
        'feels_like': 32.1,
        'humidity': 88,
        'description': 'light snow',
        'wind_speed': 8.2,
        'timestamp': '2026-02-19T06:00:00',
    }
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_forecast(s3_client):
    """Seed RAW_FORECAST into mocked S3 at the tmp/forecast/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/forecast/raw/{date_str}.json',
        Body=json.dumps(RAW_FORECAST),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_forecast(s3_client):
    """Seed JSON array forecast data into mocked S3 at the tmp/forecast/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/forecast/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_FORECAST),
    )
    yield s3_client


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformForecast:
    """Tests for transform_forecast()."""

    def test_returns_count_of_periods(self, s3_raw_forecast):
        count = transform_forecast()
        assert count == 2

    def test_output_written_as_json_array_to_s3(self, s3_raw_forecast):
        transform_forecast()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_forecast.get_object(
            Bucket='test-bucket', Key=f'tmp/forecast/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read().decode())
        assert isinstance(records, list)
        assert len(records) == 2
        for record in records:
            assert 'city' in record
            assert 'forecast_time' in record
            assert 'temperature' in record

    def test_extracts_all_required_fields(self, s3_raw_forecast):
        transform_forecast()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_forecast.get_object(
            Bucket='test-bucket', Key=f'tmp/forecast/transformed/{date_str}.json'
        )
        record = json.loads(obj['Body'].read().decode())[0]

        assert record['city'] == 'Brooklyn'
        assert record['forecast_time'] == '2026-02-19 06:00:00'
        assert record['temperature'] == 38.5
        assert record['feels_like'] == 32.1
        assert record['humidity'] == 88
        assert record['description'] == 'light snow'
        assert record['wind_speed'] == 8.2
        assert 'timestamp' in record

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_forecast()


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadForecastToS3:
    """Tests for load_to_s3() in the forecast pipeline."""

    def test_uploads_to_forecast_prefix(self, s3_transformed_forecast):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/forecast/')

    def test_object_exists_in_s3_after_upload(self, s3_transformed_forecast):
        load_to_s3()

        objects = s3_transformed_forecast.list_objects_v2(Bucket='test-bucket', Prefix='forecast/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_forecast_records(self, s3_transformed_forecast):
        load_to_s3()

        objects = s3_transformed_forecast.list_objects_v2(Bucket='test-bucket', Prefix='forecast/')
        key = objects['Contents'][0]['Key']
        body = s3_transformed_forecast.get_object(Bucket='test-bucket', Key=key)['Body'].read()
        records = pq.read_table(io.BytesIO(body)).to_pylist()
        record = records[0]

        assert record['city'] == 'Brooklyn'
        assert 'forecast_time' in record
        assert 'temperature' in record
