"""Tests for edgar_pipeline: _extract_annual_records, transform_fundamentals, load_to_s3."""

import io
import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, call
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from edgar_pipeline.edgar_pipeline import (
    extract_edgar_data, _extract_annual_records, transform_fundamentals, load_to_s3
)

# ── Sample EDGAR raw data ──────────────────────────────────────────────────────

# Mimics the 'facts' dict from the EDGAR Company Facts API for META.
# Includes one quarterly 10-Q record to verify it is filtered out.
META_FACTS = {
    'us-gaap': {
        'PaymentsToAcquirePropertyPlantAndEquipment': {
            'units': {
                'USD': [
                    {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 32000000000,
                     'end': '2022-12-31', 'filed': '2023-02-01'},
                    {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 37000000000,
                     'end': '2023-12-31', 'filed': '2024-02-01'},
                    # Quarterly record — must NOT appear in output
                    {'fp': 'Q3', 'form': '10-Q', 'fy': 2023, 'val': 8000000000,
                     'end': '2023-09-30', 'filed': '2023-11-01'},
                ]
            }
        },
        'RevenueFromContractWithCustomerExcludingAssessedTax': {
            'units': {
                'USD': [
                    {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 116609000000,
                     'end': '2022-12-31', 'filed': '2023-02-01'},
                    {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 134902000000,
                     'end': '2023-12-31', 'filed': '2024-02-01'},
                ]
            }
        },
    }
}

# Payload written to S3 tmp/edgar/raw/ by extract_edgar_data()
RAW_EDGAR = {
    'META': {
        'cik': '0001326801',
        'entity_name': 'Meta Platforms Inc',
        'facts': META_FACTS,
    }
}

# Pre-built transformed records for seeding load_to_s3() tests
TRANSFORMED_RECORDS = [
    {
        'cik': '0001326801', 'symbol': 'META', 'year': 2022,
        'capex_usd': 32000000000, 'revenue_usd': 116609000000,
        'revenue_tag': 'RevenueFromContractWithCustomerExcludingAssessedTax',
        'period_end': '2022-12-31', 'filed': '2023-02-01',
        'extracted_at': '2026-02-26T12:00:00',
    },
    {
        'cik': '0001326801', 'symbol': 'META', 'year': 2023,
        'capex_usd': 37000000000, 'revenue_usd': 134902000000,
        'revenue_tag': 'RevenueFromContractWithCustomerExcludingAssessedTax',
        'period_end': '2023-12-31', 'filed': '2024-02-01',
        'extracted_at': '2026-02-26T12:00:00',
    },
]


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_raw_edgar(s3_client):
    """Seed RAW_EDGAR into mocked S3 at the tmp/edgar/raw/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/edgar/raw/{date_str}.json',
        Body=json.dumps(RAW_EDGAR),
    )
    yield s3_client


@pytest.fixture
def s3_transformed_edgar(s3_client):
    """Seed TRANSFORMED_RECORDS into mocked S3 at the tmp/edgar/transformed/ key."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_client.put_object(
        Bucket='test-bucket',
        Key=f'tmp/edgar/transformed/{date_str}.json',
        Body=json.dumps(TRANSFORMED_RECORDS),
    )
    yield s3_client


# ── _extract_annual_records tests ──────────────────────────────────────────────

class TestExtractAnnualRecords:
    """Tests for _extract_annual_records() — pure function, no S3 needed."""

    def test_returns_only_fy_10k_records(self):
        records = _extract_annual_records(
            META_FACTS, 'PaymentsToAcquirePropertyPlantAndEquipment'
        )

        assert len(records) == 2
        for r in records:
            assert r['fp'] == 'FY'
            assert r['form'] == '10-K'

    def test_excludes_quarterly_10q_records(self):
        records = _extract_annual_records(
            META_FACTS, 'PaymentsToAcquirePropertyPlantAndEquipment'
        )

        forms = {r['form'] for r in records}
        assert '10-Q' not in forms

    def test_returns_empty_list_for_missing_tag(self):
        records = _extract_annual_records(META_FACTS, 'NonExistentXbrlTag')

        assert records == []

    def test_returns_empty_list_for_empty_facts(self):
        records = _extract_annual_records({}, 'PaymentsToAcquirePropertyPlantAndEquipment')

        assert records == []

    def test_extracts_correct_values_by_year(self):
        records = _extract_annual_records(
            META_FACTS, 'PaymentsToAcquirePropertyPlantAndEquipment'
        )

        values = {r['fy']: r['val'] for r in records}
        assert values[2022] == 32000000000
        assert values[2023] == 37000000000


# ── transform_fundamentals tests ───────────────────────────────────────────────

class TestTransformFundamentals:
    """Tests for transform_fundamentals() — reads raw from S3, writes transformed to S3."""

    def test_produces_one_record_per_fiscal_year(self, s3_raw_edgar):
        transform_fundamentals()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_edgar.get_object(
            Bucket='test-bucket', Key=f'tmp/edgar/transformed/{date_str}.json'
        )
        records = json.loads(obj['Body'].read())
        assert len(records) == 2

    def test_output_records_contain_required_fields(self, s3_raw_edgar):
        transform_fundamentals()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_edgar.get_object(
            Bucket='test-bucket', Key=f'tmp/edgar/transformed/{date_str}.json'
        )
        record = json.loads(obj['Body'].read())[0]

        for field in ['cik', 'symbol', 'year', 'capex_usd', 'revenue_usd',
                      'period_end', 'extracted_at']:
            assert field in record

    def test_capex_and_revenue_values_are_correct(self, s3_raw_edgar):
        transform_fundamentals()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_raw_edgar.get_object(
            Bucket='test-bucket', Key=f'tmp/edgar/transformed/{date_str}.json'
        )
        by_year = {r['year']: r for r in json.loads(obj['Body'].read())}

        assert by_year[2022]['capex_usd'] == 32000000000
        assert by_year[2022]['revenue_usd'] == 116609000000

    def test_skips_years_before_2020(self, s3_client):
        """Records with fy < 2020 must be excluded regardless of filing type."""
        facts_with_old_year = {
            'us-gaap': {
                'PaymentsToAcquirePropertyPlantAndEquipment': {
                    'units': {
                        'USD': [
                            {'fp': 'FY', 'form': '10-K', 'fy': 2018, 'val': 10000000000,
                             'end': '2018-12-31', 'filed': '2019-02-01'},
                            {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 32000000000,
                             'end': '2022-12-31', 'filed': '2023-02-01'},
                        ]
                    }
                },
                'RevenueFromContractWithCustomerExcludingAssessedTax': {
                    'units': {
                        'USD': [
                            {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 116609000000,
                             'end': '2022-12-31', 'filed': '2023-02-01'},
                        ]
                    }
                },
            }
        }
        raw = {'META': {'cik': '0001326801', 'entity_name': 'Meta Platforms Inc',
                        'facts': facts_with_old_year}}
        date_str = datetime.now().strftime('%Y-%m-%d')
        s3_client.put_object(
            Bucket='test-bucket',
            Key=f'tmp/edgar/raw/{date_str}.json',
            Body=json.dumps(raw),
        )

        transform_fundamentals()

        obj = s3_client.get_object(
            Bucket='test-bucket', Key=f'tmp/edgar/transformed/{date_str}.json'
        )
        years = [r['year'] for r in json.loads(obj['Body'].read())]
        assert 2018 not in years
        assert 2022 in years

    def test_falls_back_through_revenue_tags(self, s3_client):
        """Uses 'Revenues' tag when primary revenue tag is absent."""
        facts_alt_revenue = {
            'us-gaap': {
                'PaymentsToAcquirePropertyPlantAndEquipment': {
                    'units': {
                        'USD': [
                            {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 32000000000,
                             'end': '2022-12-31', 'filed': '2023-02-01'},
                        ]
                    }
                },
                'Revenues': {  # second tag in REVENUE_TAGS fallback list
                    'units': {
                        'USD': [
                            {'fp': 'FY', 'form': '10-K', 'fy': 2022, 'val': 999000000000,
                             'end': '2022-12-31', 'filed': '2023-02-01'},
                        ]
                    }
                },
            }
        }
        raw = {'META': {'cik': '0001326801', 'entity_name': 'Meta Platforms Inc',
                        'facts': facts_alt_revenue}}
        date_str = datetime.now().strftime('%Y-%m-%d')
        s3_client.put_object(
            Bucket='test-bucket',
            Key=f'tmp/edgar/raw/{date_str}.json',
            Body=json.dumps(raw),
        )

        transform_fundamentals()

        obj = s3_client.get_object(
            Bucket='test-bucket', Key=f'tmp/edgar/transformed/{date_str}.json'
        )
        record = json.loads(obj['Body'].read())[0]
        assert record['revenue_usd'] == 999000000000
        assert record['revenue_tag'] == 'Revenues'

    def test_raises_on_missing_raw_data(self, s3_client):
        """No raw key seeded in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            transform_fundamentals()


# ── load_to_s3 tests ───────────────────────────────────────────────────────────

class TestLoadEdgarToS3:
    """Tests for load_to_s3() — partitions records by (cik, year) and writes to S3."""

    def test_writes_to_correct_partition_prefix(self, s3_transformed_edgar):
        load_to_s3()

        objects = s3_transformed_edgar.list_objects_v2(
            Bucket='test-bucket', Prefix='fundamentals/cik=0001326801/'
        )
        assert objects['KeyCount'] >= 1

    def test_one_file_per_year_partition(self, s3_transformed_edgar):
        """Two years of META data → two separate S3 files under fundamentals/."""
        load_to_s3()

        objects = s3_transformed_edgar.list_objects_v2(
            Bucket='test-bucket', Prefix='fundamentals/'
        )
        assert len(objects['Contents']) == 2

    def test_output_is_parquet_with_required_fields(self, s3_transformed_edgar):
        """S3 file is valid Parquet containing symbol and capex_usd columns."""
        load_to_s3()

        objects = s3_transformed_edgar.list_objects_v2(
            Bucket='test-bucket', Prefix='fundamentals/'
        )
        key = objects['Contents'][0]['Key']
        body = s3_transformed_edgar.get_object(
            Bucket='test-bucket', Key=key
        )['Body'].read()

        records = pq.read_table(io.BytesIO(body)).to_pylist()
        for record in records:
            assert 'symbol' in record
            assert 'capex_usd' in record

    def test_partition_key_contains_cik_and_year(self, s3_transformed_edgar):
        """S3 keys follow fundamentals/cik={cik}/year={year}/data.parquet pattern."""
        load_to_s3()

        objects = s3_transformed_edgar.list_objects_v2(
            Bucket='test-bucket', Prefix='fundamentals/'
        )
        keys = [obj['Key'] for obj in objects['Contents']]
        assert any('cik=0001326801' in k and 'year=2022' in k for k in keys)
        assert any('cik=0001326801' in k and 'year=2023' in k for k in keys)

    def test_raises_on_missing_transformed_data(self, s3_client):
        """No transformed key seeded in S3 → ClientError (NoSuchKey)."""
        with pytest.raises(ClientError):
            load_to_s3()


# ── Extract tests ───────────────────────────────────────────────────────────────

def _make_edgar_response(cik, include_capex=True):
    facts = {'us-gaap': {}}
    if include_capex:
        facts['us-gaap']['PaymentsToAcquirePropertyPlantAndEquipment'] = {
            'units': {
                'USD': [
                    {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 37000000000,
                     'end': '2023-12-31', 'filed': '2024-02-01'},
                ]
            }
        }
    facts['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax'] = {
        'units': {
            'USD': [
                {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 134902000000,
                 'end': '2023-12-31', 'filed': '2024-02-01'},
            ]
        }
    }
    return {
        'entityName': f'Entity {cik}',
        'facts': facts,
    }


class TestExtractEdgarData:
    """Tests for extract_edgar_data() — HTTP-level mocking only."""

    def test_extract_edgar_returns_all_companies(self, s3_client, monkeypatch):
        from config import EDGAR_CIKS

        def side_effect(url, headers, timeout):
            cik = url.split('CIK')[1].replace('.json', '')
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json.return_value = _make_edgar_response(cik)
            return mock_response

        with patch('edgar_pipeline.edgar_pipeline.requests.get', side_effect=side_effect), \
             patch('edgar_pipeline.edgar_pipeline.sleep'):
            extract_edgar_data()

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_client.get_object(Bucket='test-bucket', Key=f'tmp/edgar/raw/{date_str}.json')
        result = json.loads(obj['Body'].read())

        assert set(result.keys()) == set(EDGAR_CIKS.keys())

    def test_extract_edgar_missing_tag_handled(self, s3_client, monkeypatch):
        """Company missing capex tag: extract succeeds, transform will skip it."""
        from config import EDGAR_CIKS

        symbols = list(EDGAR_CIKS.items())
        skip_symbol = symbols[0][0]

        def side_effect(url, headers, timeout):
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            cik = url.split('CIK')[1].replace('.json', '')
            symbol_for_cik = next(
                (s for s, c in EDGAR_CIKS.items() if c == cik), None
            )
            include_capex = symbol_for_cik != skip_symbol
            mock_response.json.return_value = _make_edgar_response(cik, include_capex=include_capex)
            return mock_response

        with patch('edgar_pipeline.edgar_pipeline.requests.get', side_effect=side_effect), \
             patch('edgar_pipeline.edgar_pipeline.sleep'):
            extract_edgar_data()  # must not raise

        date_str = datetime.now().strftime('%Y-%m-%d')
        obj = s3_client.get_object(Bucket='test-bucket', Key=f'tmp/edgar/raw/{date_str}.json')
        result = json.loads(obj['Body'].read())
        assert skip_symbol in result  # raw data still stored; transform skips missing capex

    def test_extract_edgar_rate_limit_sleep(self, s3_client, monkeypatch):
        from config import EDGAR_CIKS

        def side_effect(url, headers, timeout):
            cik = url.split('CIK')[1].replace('.json', '')
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json.return_value = _make_edgar_response(cik)
            return mock_response

        with patch('edgar_pipeline.edgar_pipeline.requests.get', side_effect=side_effect), \
             patch('edgar_pipeline.edgar_pipeline.sleep') as mock_sleep:
            extract_edgar_data()

        # sleep called once between each company: n-1 times for n companies
        expected_calls = len(EDGAR_CIKS) - 1
        assert mock_sleep.call_count == expected_calls
