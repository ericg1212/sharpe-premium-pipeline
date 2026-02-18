"""Tests for weather_pipeline DAG: transform and load functions."""

import json
import os
import pytest

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
def raw_weather_file():
    """Write RAW_WEATHER to /tmp/weather_raw.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/weather_raw.json', 'w') as f:
        json.dump(RAW_WEATHER, f)
    yield
    for path in ['/tmp/weather_raw.json', '/tmp/weather_transformed.json']:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@pytest.fixture
def transformed_weather_file():
    """Write TRANSFORMED_WEATHER to /tmp/weather_transformed.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/weather_transformed.json', 'w') as f:
        json.dump(TRANSFORMED_WEATHER, f)
    yield
    try:
        os.remove('/tmp/weather_transformed.json')
    except FileNotFoundError:
        pass


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformWeather:
    """Tests for transform_weather()."""

    def test_extracts_all_required_fields(self, raw_weather_file):
        result = transform_weather()

        assert result['city'] == 'Brooklyn'
        assert result['temperature'] == 45.2
        assert result['feels_like'] == 40.1
        assert result['humidity'] == 65
        assert result['description'] == 'light rain'
        assert result['wind_speed'] == 12.5
        assert 'timestamp' in result

    def test_description_field_not_weather(self, raw_weather_file):
        """Regression: key must be 'description' to match the Glue schema column."""
        result = transform_weather()

        assert 'description' in result
        assert 'weather' not in result

    def test_output_written_to_tmp_file(self, raw_weather_file):
        transform_weather()

        assert os.path.exists('/tmp/weather_transformed.json')
        with open('/tmp/weather_transformed.json') as f:
            saved = json.load(f)
        assert saved['city'] == 'Brooklyn'

    def test_raises_file_not_found_without_raw_data(self):
        try:
            os.remove('/tmp/weather_raw.json')
        except FileNotFoundError:
            pass

        with pytest.raises(FileNotFoundError):
            transform_weather()

    def test_raises_on_missing_main_field(self):
        """API response missing 'main' block should raise ValueError."""
        os.makedirs('/tmp', exist_ok=True)
        incomplete = {'name': 'Brooklyn', 'weather': [{'description': 'clear'}], 'wind': {'speed': 5}}
        with open('/tmp/weather_raw.json', 'w') as f:
            json.dump(incomplete, f)

        try:
            with pytest.raises((ValueError, KeyError)):
                transform_weather()
        finally:
            try:
                os.remove('/tmp/weather_raw.json')
            except FileNotFoundError:
                pass


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadWeatherToS3:
    """Tests for load_to_s3() in the weather pipeline."""

    def test_uploads_to_weather_prefix_not_raw(self, s3_client, transformed_weather_file):
        """Key must use 'weather/' prefix, not the old 'raw/' prefix."""
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/weather/')
        assert 'raw/' not in result

    def test_object_exists_in_s3_after_upload(self, s3_client, transformed_weather_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='weather/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_city(self, s3_client, transformed_weather_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='weather/')
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read().decode()
        record = json.loads(body)

        assert record['city'] == 'Brooklyn'
        assert record['description'] == 'light rain'
