"""Tests for stock_pipeline DAG: transform and load functions."""

import json
import os
import pytest

from stock_pipeline.stock_pipeline import transform_stock_data, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_STOCKS = [
    {
        'symbol': 'NVDA',
        'price': '875.40',
        'previous_close': '860.00',
        'change': '15.40',
        'change_percent': '1.79%',
        'volume': '45000000',
        'trading_day': '2025-12-31',
    },
    {
        'symbol': 'META',
        'price': '620.00',
        'previous_close': '615.00',
        'change': '5.00',
        'change_percent': '0.81%',
        'volume': '18000000',
        'trading_day': '2025-12-31',
    },
]

TRANSFORMED_STOCKS = [
    {
        'symbol': 'NVDA', 'price': 875.40, 'previous_close': 860.00, 'change': 15.40,
        'change_percent': 1.79, 'volume': 45000000, 'trading_day': '2025-12-31',
        'extracted_at': '2025-12-31T12:00:00',
    },
    {
        'symbol': 'META', 'price': 620.00, 'previous_close': 615.00, 'change': 5.00,
        'change_percent': 0.81, 'volume': 18000000, 'trading_day': '2025-12-31',
        'extracted_at': '2025-12-31T12:00:00',
    },
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_stocks_file():
    """Write RAW_STOCKS to /tmp/stocks_raw.json; clean up after the test."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/stocks_raw.json', 'w') as f:
        json.dump(RAW_STOCKS, f)
    yield
    for path in ['/tmp/stocks_raw.json', '/tmp/stocks_transformed.json']:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@pytest.fixture
def transformed_stocks_file():
    """Write TRANSFORMED_STOCKS to /tmp/stocks_transformed.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/stocks_transformed.json', 'w') as f:
        json.dump(TRANSFORMED_STOCKS, f)
    yield
    try:
        os.remove('/tmp/stocks_transformed.json')
    except FileNotFoundError:
        pass


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformStockData:
    """Tests for transform_stock_data()."""

    def test_converts_string_fields_to_numeric_types(self, raw_stocks_file):
        result = transform_stock_data()

        assert isinstance(result[0]['price'], float)
        assert isinstance(result[0]['volume'], int)
        assert isinstance(result[0]['change'], float)
        assert isinstance(result[0]['change_percent'], float)

    def test_calculates_change_percent_from_change_and_close(self, raw_stocks_file):
        result = transform_stock_data()

        nvda = next(r for r in result if r['symbol'] == 'NVDA')
        # change=15.40, previous_close=860.00 → 15.40 / 860.00 * 100 ≈ 1.79
        expected = round(15.40 / 860.00 * 100, 2)
        assert abs(nvda['change_percent'] - expected) < 0.01

    def test_all_input_stocks_are_in_output(self, raw_stocks_file):
        result = transform_stock_data()

        assert len(result) == 2
        assert {r['symbol'] for r in result} == {'NVDA', 'META'}

    def test_output_written_to_tmp_file(self, raw_stocks_file):
        transform_stock_data()

        assert os.path.exists('/tmp/stocks_transformed.json')
        with open('/tmp/stocks_transformed.json') as f:
            saved = json.load(f)
        assert len(saved) == 2

    def test_raises_file_not_found_without_raw_data(self):
        try:
            os.remove('/tmp/stocks_raw.json')
        except FileNotFoundError:
            pass

        with pytest.raises(FileNotFoundError):
            transform_stock_data()

    def test_data_quality_rejects_negative_price(self):
        os.makedirs('/tmp', exist_ok=True)
        bad = [{'symbol': 'BAD', 'price': '-5.00', 'previous_close': '100.00',
                'change': '-105.00', 'change_percent': '-105%',
                'volume': '1000', 'trading_day': '2025-12-31'}]
        with open('/tmp/stocks_raw.json', 'w') as f:
            json.dump(bad, f)

        try:
            with pytest.raises(Exception, match="Invalid price"):
                transform_stock_data()
        finally:
            for path in ['/tmp/stocks_raw.json', '/tmp/stocks_transformed.json']:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadStockToS3:
    """Tests for load_to_s3() in the stock pipeline."""

    def test_uploads_to_stocks_prefix(self, s3_client, transformed_stocks_file):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/stocks/')

    def test_object_exists_in_s3_after_upload(self, s3_client, transformed_stocks_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='stocks/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_is_ndjson(self, s3_client, transformed_stocks_file):
        """Each line of the S3 payload should be valid JSON with a symbol field."""
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='stocks/')
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read().decode()

        lines = body.strip().split('\n')
        assert len(lines) == 2
        for line in lines:
            record = json.loads(line)
            assert 'symbol' in record
            assert 'price' in record
