"""Tests for historical_backfill: format_records, write_to_s3, register_partition."""

import json

from stock_pipeline.historical_backfill import format_records, write_to_s3, register_partition

# ── Sample data ────────────────────────────────────────────────────────────────

SAMPLE_RECORDS = [
    {'date': '2023-01-31', 'close': 100.0, 'volume': 1000000},
    {'date': '2023-02-28', 'close': 105.5, 'volume': 1250000},
    {'date': '2023-03-31', 'close': 98.25, 'volume': 980000},
]


# ── format_records tests ───────────────────────────────────────────────────────

class TestFormatRecords:
    """Tests for format_records() — pure function, no S3 needed."""

    def test_adds_symbol_to_every_record(self):
        ndjson = format_records('NVDA', SAMPLE_RECORDS)

        for line in ndjson.strip().split('\n'):
            assert json.loads(line)['symbol'] == 'NVDA'

    def test_adds_extracted_at_to_every_record(self):
        ndjson = format_records('NVDA', SAMPLE_RECORDS)

        for line in ndjson.strip().split('\n'):
            assert 'extracted_at' in json.loads(line)

    def test_output_is_valid_ndjson(self):
        ndjson = format_records('NVDA', SAMPLE_RECORDS)

        lines = ndjson.strip().split('\n')
        assert len(lines) == len(SAMPLE_RECORDS)
        for line in lines:
            json.loads(line)  # raises if invalid JSON

    def test_preserves_close_and_volume(self):
        ndjson = format_records('NVDA', SAMPLE_RECORDS)

        records = [json.loads(line) for line in ndjson.strip().split('\n')]
        assert records[0]['close'] == 100.0
        assert records[0]['volume'] == 1000000
        assert records[1]['close'] == 105.5

    def test_different_symbols_produce_different_output(self):
        nvda = format_records('NVDA', SAMPLE_RECORDS)
        meta = format_records('META', SAMPLE_RECORDS)

        assert json.loads(nvda.split('\n')[0])['symbol'] == 'NVDA'
        assert json.loads(meta.split('\n')[0])['symbol'] == 'META'


# ── write_to_s3 tests ──────────────────────────────────────────────────────────

class TestWriteToS3:
    """Tests for write_to_s3() — writes NDJSON to mocked S3."""

    def test_writes_to_correct_partition_key(self, s3_client):
        bucket, key = write_to_s3('NVDA', 'body content')

        assert key == 'historical_prices/symbol=NVDA/monthly.json'

    def test_returns_correct_bucket_name(self, s3_client):
        bucket, key = write_to_s3('NVDA', 'body content')

        assert bucket == 'test-bucket'

    def test_object_exists_after_write(self, s3_client):
        write_to_s3('NVDA', 'body content')

        obj = s3_client.get_object(
            Bucket='test-bucket', Key='historical_prices/symbol=NVDA/monthly.json'
        )
        assert obj['Body'].read().decode() == 'body content'

    def test_different_symbols_write_to_different_keys(self, s3_client):
        write_to_s3('NVDA', 'nvda data')
        write_to_s3('META', 'meta data')

        objects = s3_client.list_objects_v2(
            Bucket='test-bucket', Prefix='historical_prices/'
        )
        keys = [obj['Key'] for obj in objects['Contents']]
        assert any('symbol=NVDA' in k for k in keys)
        assert any('symbol=META' in k for k in keys)


# ── register_partition tests ───────────────────────────────────────────────────

class TestRegisterPartition:
    """Tests for register_partition() — fires Athena query via mocked AWS."""

    def test_does_not_raise_for_valid_symbol(self, s3_client):
        """Athena workgroup exists in the fixture — should complete without error."""
        register_partition('NVDA', 'test-bucket')

    def test_does_not_raise_for_multiple_symbols(self, s3_client):
        for symbol in ['NVDA', 'META', 'MSFT']:
            register_partition(symbol, 'test-bucket')
