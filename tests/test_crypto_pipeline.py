"""Tests for crypto_pipeline DAG: transform and load functions."""

import io
import json
import pytest
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from crypto_pipeline.crypto_pipeline import transform_crypto_data, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_CRYPTOS = [
    {'symbol': 'BTC', 'price': '95000.50', 'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
    {'symbol': 'ETH', 'price': '3400.25',  'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
    {'symbol': 'SOL', 'price': '185.75',   'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
]

TRANSFORMED_CRYPTOS = [
    {'symbol': 'BTC', 'price': 95000.50, 'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
     'extracted_at': '2025-12-31T12:00:00'},
    {'symbol': 'ETH', 'price': 3400.25,  'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
     'extracted_at': '2025-12-31T12:00:00'},
    {'symbol': 'SOL', 'price': 185.75,   'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
     'extracted_at': '2025-12-31T12:00:00'},
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_crypto(s3_client):
    """Seed RAW_CRYPTOS into mocked S3 at the tmp/crypto/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/crypto/raw/{date_str}.json',
        Body=json.dumps(RAW_CRYPTOS),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_crypto(s3_client):
    """Seed JSON array crypto data into mocked S3 at the tmp/crypto/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/crypto/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_CRYPTOS),
    )
    yield s3_client


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformCryptoData:
    """Tests for transform_crypto_data()."""

    def test_converts_price_string_to_float(self, s3_raw_crypto):
        result = transform_crypto_data()

        assert isinstance(result[0]['price'], float)
        assert abs(result[0]['price'] - 95000.50) < 0.01

    def test_all_three_symbols_are_transformed(self, s3_raw_crypto):
        result = transform_crypto_data()

        assert len(result) == 3
        assert {r['symbol'] for r in result} == {'BTC', 'ETH', 'SOL'}

    def test_output_written_as_json_array_to_s3(self, s3_raw_crypto):
        """Transform writes a JSON array to the S3 tmp key."""
        transform_crypto_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_crypto.get_object(
            Bucket='test-bucket', Key=f'tmp/crypto/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read().decode())
        assert isinstance(records, list)
        assert len(records) == 3
        for record in records:
            assert 'symbol' in record
            assert 'price' in record

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_crypto_data()

    def test_data_quality_rejects_zero_price(self, s3_client):
        date_str = datetime.now().strftime('%Y-%m-%d')
        bad = [{'symbol': 'BTC', 'price': '0', 'currency': 'USD',
                'timestamp': '2025-12-31T12:00:00'}]
        s3_client.put_object(
            Bucket='test-bucket',
            Key=f'tmp/crypto/raw/{date_str}.json',
            Body=json.dumps(bad),
        )

        with pytest.raises(Exception):
            transform_crypto_data()


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadCryptoToS3:
    """Tests for load_to_s3() in the crypto pipeline."""

    def test_uploads_to_crypto_prefix(self, s3_transformed_crypto):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/crypto/')

    def test_object_exists_in_s3_after_upload(self, s3_transformed_crypto):
        load_to_s3()

        objects = s3_transformed_crypto.list_objects_v2(Bucket='test-bucket', Prefix='crypto/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_all_symbols(self, s3_transformed_crypto):
        load_to_s3()

        objects = s3_transformed_crypto.list_objects_v2(Bucket='test-bucket', Prefix='crypto/')
        key = objects['Contents'][0]['Key']
        body = s3_transformed_crypto.get_object(Bucket='test-bucket', Key=key)['Body'].read()

        records = pq.read_table(io.BytesIO(body)).to_pylist()
        symbols = {r['symbol'] for r in records}
        assert symbols == {'BTC', 'ETH', 'SOL'}
