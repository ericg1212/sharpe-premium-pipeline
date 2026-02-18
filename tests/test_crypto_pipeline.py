"""Tests for crypto_pipeline DAG: transform and load functions."""

import json
import os
import pytest

from crypto_pipeline.crypto_pipeline import transform_crypto_data, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_CRYPTOS = [
    {'symbol': 'BTC', 'price': '95000.50', 'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
    {'symbol': 'ETH', 'price': '3400.25',  'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
    {'symbol': 'SOL', 'price': '185.75',   'currency': 'USD', 'timestamp': '2025-12-31T12:00:00'},
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_crypto_file():
    """Write RAW_CRYPTOS to /tmp/crypto_raw.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/crypto_raw.json', 'w') as f:
        json.dump(RAW_CRYPTOS, f)
    yield
    for path in ['/tmp/crypto_raw.json', '/tmp/crypto_transformed.json']:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@pytest.fixture
def transformed_crypto_file():
    """Write NDJSON crypto data to /tmp/crypto_transformed.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    records = [
        {'symbol': 'BTC', 'price': 95000.50, 'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
         'extracted_at': '2025-12-31T12:00:00'},
        {'symbol': 'ETH', 'price': 3400.25,  'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
         'extracted_at': '2025-12-31T12:00:00'},
        {'symbol': 'SOL', 'price': 185.75,   'currency': 'USD', 'timestamp': '2025-12-31T12:00:00',
         'extracted_at': '2025-12-31T12:00:00'},
    ]
    # crypto transform writes NDJSON (one JSON object per line)
    with open('/tmp/crypto_transformed.json', 'w') as f:
        f.write('\n'.join(json.dumps(r) for r in records))
    yield
    try:
        os.remove('/tmp/crypto_transformed.json')
    except FileNotFoundError:
        pass


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformCryptoData:
    """Tests for transform_crypto_data()."""

    def test_converts_price_string_to_float(self, raw_crypto_file):
        result = transform_crypto_data()

        assert isinstance(result[0]['price'], float)
        assert abs(result[0]['price'] - 95000.50) < 0.01

    def test_all_three_symbols_are_transformed(self, raw_crypto_file):
        result = transform_crypto_data()

        assert len(result) == 3
        assert {r['symbol'] for r in result} == {'BTC', 'ETH', 'SOL'}

    def test_output_written_as_ndjson(self, raw_crypto_file):
        """Transform writes one JSON object per line (NDJSON)."""
        transform_crypto_data()

        with open('/tmp/crypto_transformed.json') as f:
            lines = f.read().strip().split('\n')
        assert len(lines) == 3
        for line in lines:
            record = json.loads(line)
            assert 'symbol' in record
            assert 'price' in record

    def test_raises_file_not_found_without_raw_data(self):
        try:
            os.remove('/tmp/crypto_raw.json')
        except FileNotFoundError:
            pass

        with pytest.raises(FileNotFoundError):
            transform_crypto_data()

    def test_data_quality_rejects_zero_price(self):
        os.makedirs('/tmp', exist_ok=True)
        bad = [{'symbol': 'BTC', 'price': '0', 'currency': 'USD',
                'timestamp': '2025-12-31T12:00:00'}]
        with open('/tmp/crypto_raw.json', 'w') as f:
            json.dump(bad, f)

        try:
            with pytest.raises(Exception):
                transform_crypto_data()
        finally:
            for path in ['/tmp/crypto_raw.json', '/tmp/crypto_transformed.json']:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadCryptoToS3:
    """Tests for load_to_s3() in the crypto pipeline."""

    def test_uploads_to_crypto_prefix(self, s3_client, transformed_crypto_file):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/crypto/')

    def test_object_exists_in_s3_after_upload(self, s3_client, transformed_crypto_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='crypto/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_all_symbols(self, s3_client, transformed_crypto_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='crypto/')
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read().decode()

        lines = body.strip().split('\n')
        symbols = {json.loads(line)['symbol'] for line in lines}
        assert symbols == {'BTC', 'ETH', 'SOL'}
