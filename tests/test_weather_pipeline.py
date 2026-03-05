"""Tests for weather_pipeline DAG: transform and load functions."""

import io
import json
import pytest
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from weather_pipeline.weather_pipeline import transform_weather, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

# Raw OpenWeatherMap API response (as saved by extract_weather)
RAW_WEATHER = {
    'name': 'Brooklyn',
    'main': {'temp': 45.2, 'feels_like': 40.1, 'humidity': 65},
    'weather': [{'description': 'light rain', 'main': 'Rain'}],
    'wind': {'speed': 12.5},
}

TRANSFORMED_WEATHER = {
    'city': 'Brooklyn',
    'temperature': 45.2,
    'feels_like': 40.1,
    'humidity': 65,
    'description': 'light rain',
    'wind_speed': 12.5,
    'timestamp': '2025-12-31T12:00:00',
}


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_weather(s3_client):
    """Seed RAW_WEATHER into mocked S3 at the tmp/weather/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/weather/raw/{date_str}.json',
        Body=json.dumps(RAW_WEATHER),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_weather(s3_client):
    """Seed TRANSFORMED_WEATHER into mocked S3 at the tmp/weather/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/weather/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_WEATHER),
    )
    yield s3_client


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformWeather:
    """Tests for transform_weather()."""

    def test_extracts_all_required_fields(self, s3_raw_weather):
        result = transform_weather()

        assert result['city'] == 'Brooklyn'
        assert result['temperature'] == 45.2
        assert result['feels_like'] == 40.1
        assert result['humidity'] == 65
        assert result['description'] == 'light rain'
        assert result['wind_speed'] == 12.5
        assert 'timestamp' in result

    def test_description_field_not_weather(self, s3_raw_weather):
        """Regression: key must be 'description' to match the Glue schema column."""
        result = transform_weather()

        assert 'description' in result
        assert 'weather' not in result

    def test_output_written_to_s3(self, s3_raw_weather):
        transform_weather()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_weather.get_object(
            Bucket='test-bucket', Key=f'tmp/weather/transformed/{date_str}.json'
        )
        saved = json.loads(obj['Body'].read())
        assert saved['city'] == 'Brooklyn'

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_weather()

    def test_raises_on_missing_main_field(self, s3_client):
        """API response missing 'main' block should raise ValueError."""
        date_str = datetime.now().strftime('%Y-%m-%d')
        incomplete = {'name': 'Brooklyn', 'weather': [{'description': 'clear'}], 'wind': {'speed': 5}}
        s3_client.put_object(
            Bucket='test-bucket',
            Key=f'tmp/weather/raw/{date_str}.json',
            Body=json.dumps(incomplete),
        )

        with pytest.raises((ValueError, KeyError)):
            transform_weather()


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadWeatherToS3:
    """Tests for load_to_s3() in the weather pipeline."""

    def test_uploads_to_weather_prefix_not_raw(self, s3_transformed_weather):
        """Key must use 'weather/' prefix, not the old 'raw/' prefix."""
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/weather/')
        assert 'raw/' not in result

    def test_object_exists_in_s3_after_upload(self, s3_transformed_weather):
        load_to_s3()

        objects = s3_transformed_weather.list_objects_v2(Bucket='test-bucket', Prefix='weather/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_city(self, s3_transformed_weather):
        load_to_s3()

        objects = s3_transformed_weather.list_objects_v2(Bucket='test-bucket', Prefix='weather/')
        key = objects['Contents'][0]['Key']
        body = s3_transformed_weather.get_object(Bucket='test-bucket', Key=key)['Body'].read()
        record = pq.read_table(io.BytesIO(body)).to_pylist()[0]

        assert record['city'] == 'Brooklyn'
        assert record['description'] == 'light rain'
