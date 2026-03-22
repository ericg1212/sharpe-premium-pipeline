"""Integration tests: full extract→transform→load chain per pipeline using moto S3 + mocked HTTP."""

import io
import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

import pyarrow.parquet as pq

from stock_pipeline.stock_pipeline import (
    extract_stock_data, transform_stock_data, load_to_s3 as stock_load,
)
from fred_pipeline.fred_pipeline import (
    extract_fred_data, transform_fred_data, load_to_s3 as fred_load,
)
from edgar_pipeline.edgar_pipeline import (
    extract_edgar_data, transform_fundamentals, load_to_s3 as edgar_load,
)
from forecast_pipeline.forecast_pipeline import (
    extract_forecast, transform_forecast, load_to_s3 as forecast_load,
)


def _alpha_vantage_response(symbol):
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        'Global Quote': {
            '01. symbol': symbol,
            '05. price': '150.00',
            '06. volume': '10000000',
            '07. latest trading day': '2026-03-22',
            '08. previous close': '148.00',
            '09. change': '2.00',
            '10. change percent': '1.35%',
        }
    }
    return mock


def _fred_api_response(series_id):
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        'observations': [
            {'date': '2024-01-01', 'value': '4.25'},
            {'date': '2024-02-01', 'value': '4.30'},
        ]
    }
    return mock


META_FACTS = {
    'us-gaap': {
        'PaymentsToAcquirePropertyPlantAndEquipment': {
            'units': {
                'USD': [
                    {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 37000000000,
                     'end': '2023-12-31', 'filed': '2024-02-01'},
                ]
            }
        },
        'RevenueFromContractWithCustomerExcludingAssessedTax': {
            'units': {
                'USD': [
                    {'fp': 'FY', 'form': '10-K', 'fy': 2023, 'val': 134902000000,
                     'end': '2023-12-31', 'filed': '2024-02-01'},
                ]
            }
        },
    }
}


def _edgar_api_response(symbol):
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        'entityName': symbol,
        'facts': META_FACTS,
    }
    return mock


OPENWEATHER_RESPONSE = {
    'city': {'name': 'Brooklyn'},
    'list': [
        {
            'dt_txt': '2026-03-22 06:00:00',
            'main': {'temp': 55.0, 'feels_like': 50.0, 'humidity': 70},
            'weather': [{'description': 'clear sky'}],
            'wind': {'speed': 5.0},
        }
    ],
}


def _openweather_response():
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = OPENWEATHER_RESPONSE
    return mock


class TestStockPipelineFullChain:

    def test_stock_pipeline_full_chain(self, s3_client, monkeypatch):
        monkeypatch.setenv('ALPHA_VANTAGE_API_KEY', 'test-key')

        def av_side_effect(url, timeout):
            symbol = url.split('symbol=')[1].split('&')[0]
            return _alpha_vantage_response(symbol)

        with patch('stock_pipeline.stock_pipeline.requests.get', side_effect=av_side_effect), \
             patch('stock_pipeline.stock_pipeline.sleep'):
            extract_stock_data()

        transform_stock_data()
        result = stock_load()

        assert result.startswith('s3://test-bucket/stocks/')

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='stocks/date=')
        assert objects['KeyCount'] >= 1
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read()
        records = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(records) > 0
        assert 'symbol' in records[0]
        assert 'price' in records[0]


class TestFredPipelineFullChain:

    def test_fred_pipeline_full_chain(self, s3_client, monkeypatch):
        monkeypatch.setenv('FRED_API_KEY', 'test-key')

        def fred_side_effect(url, params, timeout):
            return _fred_api_response(params['series_id'])

        with patch('fred_pipeline.fred_pipeline.requests.get', side_effect=fred_side_effect):
            extract_fred_data()

        transform_fred_data()
        fred_load()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='macro_indicators/')
        assert objects['KeyCount'] >= 1
        gs10_keys = [o['Key'] for o in objects['Contents'] if 'series=GS10' in o['Key']]
        assert len(gs10_keys) >= 1

        body = s3_client.get_object(Bucket='test-bucket', Key=gs10_keys[0])['Body'].read()
        records = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(records) > 0
        assert 'date' in records[0]
        assert 'value' in records[0]
        assert 'series_id' in records[0]


class TestEdgarPipelineFullChain:

    def test_edgar_pipeline_full_chain(self, s3_client, monkeypatch):
        from config import EDGAR_CIKS

        def edgar_side_effect(url, headers, timeout):
            for symbol in EDGAR_CIKS:
                cik = EDGAR_CIKS[symbol].lstrip('0')
                if cik in url:
                    return _edgar_api_response(symbol)
            return _edgar_api_response('META')

        with patch('edgar_pipeline.edgar_pipeline.requests.get', side_effect=edgar_side_effect), \
             patch('edgar_pipeline.edgar_pipeline.sleep'):
            extract_edgar_data()

        transform_fundamentals()
        edgar_load()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='fundamentals/')
        assert objects['KeyCount'] >= 1
        key = objects['Contents'][0]['Key']
        assert 'cik=' in key
        assert 'year=' in key

        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read()
        records = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(records) > 0
        assert 'symbol' in records[0]
        assert 'year' in records[0]
        assert 'capex_usd' in records[0]
        assert 'revenue_usd' in records[0]


class TestForecastPipelineFullChain:

    def test_forecast_pipeline_full_chain(self, s3_client, monkeypatch):
        monkeypatch.setenv('OPENWEATHER_API_KEY', 'test-key')

        with patch('forecast_pipeline.forecast_pipeline.requests.get',
                   return_value=_openweather_response()):
            extract_forecast()

        transform_forecast()
        result = forecast_load()

        assert result.startswith('s3://test-bucket/forecast/')

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='forecast/date=')
        assert objects['KeyCount'] >= 1
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read()
        records = pq.read_table(io.BytesIO(body)).to_pylist()
        assert len(records) > 0
        assert 'city' in records[0]
        assert 'temperature' in records[0]
