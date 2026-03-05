"""Tests for historical_backfill: format_records, write_to_s3, register_partition."""

import io
import pytest
import pyarrow.parquet as pq

from stock_pipeline.historical_backfill import format_records, write_to_s3, register_partition

# ── Sample data ────────────────────────────────────────────────────────────────

SAMPLE_RECORDS = [
    {'date': '2023-01-31', 'close': 100.0, 'volume': 1000000},
    {'date': '2023-02-28', 'close': 105.5, 'volume': 1250000},
    {'date': '2023-03-31', 'close': 98.25, 'volume': 980000},
]


# ── format_records tests ───────────────────────────────────────────────────────

class TestFormatRecords:
    """Tests for format_records() — returns list of dicts, no I/O."""

    def test_adds_symbol_to_every_record(self):
        records = format_records('NVDA', SAMPLE_RECORDS)

        for record in records:
            assert record['symbol'] == 'NVDA'

    def test_adds_extracted_at_to_every_record(self):
        records = format_records('NVDA', SAMPLE_RECORDS)

        for record in records:
            assert 'extracted_at' in record

    def test_output_is_list_of_dicts(self):
        records = format_records('NVDA', SAMPLE_RECORDS)

        assert isinstance(records, list)
        assert len(records) == len(SAMPLE_RECORDS)
        for record in records:
            assert isinstance(record, dict)

    def test_preserves_close_and_volume(self):
        records = format_records('NVDA', SAMPLE_RECORDS)

        assert records[0]['close'] == 100.0
        assert records[0]['volume'] == 1000000
        assert records[1]['close'] == 105.5

    def test_different_symbols_produce_different_output(self):
        nvda = format_records('NVDA', SAMPLE_RECORDS)
        meta = format_records('META', SAMPLE_RECORDS)

        assert nvda[0]['symbol'] == 'NVDA'
        assert meta[0]['symbol'] == 'META'


# ── write_to_s3 tests ──────────────────────────────────────────────────────────

class TestWriteToS3:
    """Tests for write_to_s3() — writes Parquet to mocked S3."""

    def test_writes_to_correct_partition_key(self, s3_client):
        records = format_records('NVDA', SAMPLE_RECORDS)
        bucket, key = write_to_s3('NVDA', records)

        assert key == 'historical_prices/symbol=NVDA/monthly.parquet'

    def test_returns_correct_bucket_name(self, s3_client):
        records = format_records('NVDA', SAMPLE_RECORDS)
        bucket, key = write_to_s3('NVDA', records)

        assert bucket == 'test-bucket'

    def test_object_exists_after_write(self, s3_client):
        records = format_records('NVDA', SAMPLE_RECORDS)
        write_to_s3('NVDA', records)

        body = s3_client.get_object(
            Bucket='test-bucket', Key='historical_prices/symbol=NVDA/monthly.parquet'
        )['Body'].read()
        result = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(result) == 3

    def test_different_symbols_write_to_different_keys(self, s3_client):
        write_to_s3('NVDA', format_records('NVDA', SAMPLE_RECORDS))
        write_to_s3('META', format_records('META', SAMPLE_RECORDS))

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
