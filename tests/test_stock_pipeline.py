"""Tests for stock_pipeline DAG: transform and load functions."""

import io
import json
import pytest
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

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
        'timestamp': '2025-12-31T12:00:00',
    },
    {
        'symbol': 'META', 'price': 620.00, 'previous_close': 615.00, 'change': 5.00,
        'change_percent': 0.81, 'volume': 18000000, 'trading_day': '2025-12-31',
        'timestamp': '2025-12-31T12:00:00',
    },
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_stocks(s3_client):
    """Seed RAW_STOCKS into mocked S3 at the tmp/stocks/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/stocks/raw/{date_str}.json',
        Body=json.dumps(RAW_STOCKS),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_stocks(s3_client):
    """Seed TRANSFORMED_STOCKS into mocked S3 at the tmp/stocks/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/stocks/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_STOCKS),
    )
    yield s3_client


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformStockData:
    """Tests for transform_stock_data()."""

    def test_converts_string_fields_to_numeric_types(self, s3_raw_stocks):
        result = transform_stock_data()

        assert isinstance(result[0]['price'], float)
        assert isinstance(result[0]['volume'], int)
        assert isinstance(result[0]['change'], float)
        assert isinstance(result[0]['change_percent'], float)

    def test_calculates_change_percent_from_change_and_close(self, s3_raw_stocks):
        result = transform_stock_data()

        nvda = next(r for r in result if r['symbol'] == 'NVDA')
        # change=15.40, previous_close=860.00 → 15.40 / 860.00 * 100 ≈ 1.79
        expected = round(15.40 / 860.00 * 100, 2)
        assert abs(nvda['change_percent'] - expected) < 0.01

    def test_all_input_stocks_are_in_output(self, s3_raw_stocks):
        result = transform_stock_data()

        assert len(result) == 2
        assert {r['symbol'] for r in result} == {'NVDA', 'META'}

    def test_output_written_to_s3(self, s3_raw_stocks):
        transform_stock_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_stocks.get_object(
            Bucket='test-bucket', Key=f'tmp/stocks/transformed/{date_str}.json'
        )
        saved = json.loads(obj['Body'].read())
        assert len(saved) == 2

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_stock_data()

    def test_data_quality_rejects_negative_price(self, s3_client):
        date_str = datetime.now().strftime('%Y-%m-%d')
        bad = [{'symbol': 'BAD', 'price': '-5.00', 'previous_close': '100.00',
                'change': '-105.00', 'change_percent': '-105%',
                'volume': '1000', 'trading_day': '2025-12-31'}]
        s3_client.put_object(
            Bucket='test-bucket',
            Key=f'tmp/stocks/raw/{date_str}.json',
            Body=json.dumps(bad),
        )

        with pytest.raises(Exception, match="Invalid price"):
            transform_stock_data()


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadStockToS3:
    """Tests for load_to_s3() in the stock pipeline."""

    def test_uploads_to_stocks_prefix(self, s3_transformed_stocks):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/stocks/')

    def test_object_exists_in_s3_after_upload(self, s3_transformed_stocks):
        load_to_s3()

        objects = s3_transformed_stocks.list_objects_v2(Bucket='test-bucket', Prefix='stocks/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_is_parquet(self, s3_transformed_stocks):
        """S3 payload should be valid Parquet with symbol and price columns."""
        load_to_s3()

        objects = s3_transformed_stocks.list_objects_v2(Bucket='test-bucket', Prefix='stocks/')
        key = objects['Contents'][0]['Key']
        body = s3_transformed_stocks.get_object(Bucket='test-bucket', Key=key)['Body'].read()

        records = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(records) == 2
        for record in records:
            assert 'symbol' in record
            assert 'price' in record
