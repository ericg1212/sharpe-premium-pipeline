"""Tests for historical_backtest.py — covering calculate_sharpe paths,
run_backtest (mocked S3 + API), and the category/premium helpers."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime

from stock_pipeline.historical_backtest import calculate_sharpe, get_dynamic_risk_free_rate
from config import RISK_FREE_RATE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_prices(n=36, seed=0, start='2022-12-31', monthly_ret=0.02, vol=0.05):
    np.random.seed(seed)
    dates = pd.date_range(start=start, periods=n + 1, freq='ME')
    returns = np.random.normal(monthly_ret, vol, n)
    prices = [100.0]
    for r in returns:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame({'date': dates, 'close': prices})


# ---------------------------------------------------------------------------
# TestGetDynamicRiskFreeRate
# ---------------------------------------------------------------------------

class TestGetDynamicRiskFreeRate:

    def test_returns_mean_of_loaded_values(self):
        """When S3 returns FEDFUNDS data, result should equal mean / 100."""
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq

        records = [{'value': 4.5}, {'value': 5.5}]
        table = pa.Table.from_pylist(records, schema=pa.schema([pa.field('value', pa.float64())]))
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        parquet_bytes = buf.read()

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=parquet_bytes))}

        rate = get_dynamic_risk_free_rate(mock_s3, 'test-bucket')
        expected = (4.5 + 5.5) / 2 / 100
        assert abs(rate - expected) < 1e-6

    def test_falls_back_to_config_when_s3_empty(self):
        """When S3 raises for all years, should return config RISK_FREE_RATE."""
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")

        rate = get_dynamic_risk_free_rate(mock_s3, 'test-bucket')
        assert rate == RISK_FREE_RATE


# ---------------------------------------------------------------------------
# TestCalculateSharpeExtended
# ---------------------------------------------------------------------------

class TestCalculateSharpeExtended:

    def test_known_sharpe_value(self):
        """Verify arithmetic: uniform 3% monthly return, 4.5% rf → positive Sharpe."""
        dates = pd.date_range(start='2022-12-31', periods=37, freq='ME')
        prices = [100 * (1.03 ** i) for i in range(37)]
        df = pd.DataFrame({'date': dates, 'close': prices})

        result = calculate_sharpe(df, 'NVDA', rf_rate=0.045)

        assert result is not None
        assert result['sharpe_ratio'] > 0
        # annualized return should be (1.03)^12 - 1 ≈ 42.6%
        expected_ann = round((1.03 ** 12 - 1) * 100, 2)
        assert abs(result['annualized_return'] - expected_ann) < 0.05

    def test_negative_returns_produce_negative_sharpe(self):
        """Declining price series should yield negative annualized return and Sharpe."""
        dates = pd.date_range(start='2022-12-31', periods=37, freq='ME')
        prices = [100 * (0.97 ** i) for i in range(37)]
        df = pd.DataFrame({'date': dates, 'close': prices})

        result = calculate_sharpe(df, 'CRM', rf_rate=0.045)
        assert result is not None
        assert result['annualized_return'] < 0
        assert result['sharpe_ratio'] < 0

    def test_months_analyzed_correct(self, sample_monthly_prices):
        """months_analyzed should equal number of pct_change rows (n-1)."""
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')
        assert result['months_analyzed'] == len(sample_monthly_prices) - 1

    def test_rolling_sharpe_shape(self, sample_monthly_prices):
        """Rolling Sharpe list length should be months_analyzed - window + 1."""
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')
        n_months = result['months_analyzed']
        window = 12
        expected_len = n_months - window + 1
        assert len(result['rolling_sharpe']) == expected_len

    def test_all_rolling_sharpe_have_required_keys(self, sample_monthly_prices):
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')
        for entry in result['rolling_sharpe']:
            assert 'date' in entry
            assert 'rolling_sharpe_12m' in entry

    def test_meta_capex_fields_present(self, sample_monthly_prices):
        result = calculate_sharpe(sample_monthly_prices, 'META')
        assert result['capex_2025_B'] == 72.2
        assert result['est_ai_spend_2026_B'] == round(125.0 * 95 / 100, 1)

    def test_insufficient_data_returns_none(self, short_price_series):
        result = calculate_sharpe(short_price_series, 'NVDA')
        assert result is None

    def test_ai_strategy_field_present(self, sample_monthly_prices):
        result = calculate_sharpe(sample_monthly_prices, 'GOOGL')
        assert result['ai_strategy'] == 'Proprietary (Gemini, TPUs)'

    def test_max_drawdown_nonpositive(self, sample_monthly_prices):
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')
        assert result['max_drawdown'] <= 0.0

    def test_category_field_correct(self, sample_monthly_prices):
        result = calculate_sharpe(sample_monthly_prices, 'MSFT')
        assert result['category'] == 'AI Integrator'


# ---------------------------------------------------------------------------
# TestRunBacktest (mocked S3 + Alpha Vantage)
# ---------------------------------------------------------------------------

class TestRunBacktest:

    def _build_alpha_vantage_response(self, symbol, n_months=36):
        """Build a minimal Alpha Vantage Monthly Adjusted response dict."""
        start = datetime(2022, 12, 1)
        series = {}
        price = 100.0
        for i in range(n_months + 1):
            month = pd.Timestamp(start) + pd.DateOffset(months=i)
            date_str = month.strftime('%Y-%m-%d')
            price *= (1 + np.random.normal(0.02, 0.05))
            series[date_str] = {
                '5. adjusted close': str(round(price, 4)),
                '1. open': str(round(price, 4)),
            }
        return {'Monthly Adjusted Time Series': series}

    @patch('stock_pipeline.historical_backtest.sleep', return_value=None)
    @patch('stock_pipeline.historical_backtest._s3_client')
    @patch('stock_pipeline.historical_backtest.requests.get')
    def test_run_backtest_returns_dataframe(self, mock_get, mock_s3_client, mock_sleep):
        from stock_pipeline.historical_backtest import run_backtest

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")
        mock_s3_client.return_value = (mock_s3, 'test-bucket')

        np.random.seed(42)

        def fake_api(url, timeout=30):
            symbol = url.split('symbol=')[1].split('&')[0]
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            resp.json.return_value = self._build_alpha_vantage_response(symbol)
            return resp

        mock_get.side_effect = fake_api

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'testkey'}):  # pragma: allowlist secret
            df = run_backtest()

        assert df is not None
        assert len(df) == 10
        required_cols = {'symbol', 'category', 'annualized_return', 'annualized_volatility',
                         'sharpe_ratio', 'max_drawdown', 'months_analyzed'}
        assert required_cols.issubset(set(df.columns))

    @patch('stock_pipeline.historical_backtest.sleep', return_value=None)
    @patch('stock_pipeline.historical_backtest._s3_client')
    @patch('stock_pipeline.historical_backtest.requests.get')
    def test_run_backtest_skips_failed_symbols(self, mock_get, mock_s3_client, mock_sleep):
        from stock_pipeline.historical_backtest import run_backtest

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")
        mock_s3_client.return_value = (mock_s3, 'test-bucket')

        call_count = [0]

        def fake_api_partial(url, timeout=30):
            call_count[0] += 1
            symbol = url.split('symbol=')[1].split('&')[0]
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if symbol in ('CRM', 'ORCL'):
                resp.json.return_value = {}  # missing key → calculate_sharpe returns None
            else:
                np.random.seed(call_count[0])
                resp.json.return_value = self._build_alpha_vantage_response(symbol)
            return resp

        mock_get.side_effect = fake_api_partial

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'testkey'}):  # pragma: allowlist secret
            df = run_backtest()

        # CRM and ORCL return no monthly data key → ValueError raised → caught → skipped
        assert len(df) <= 10
        assert len(df) >= 1

    @patch('stock_pipeline.historical_backtest.sleep', return_value=None)
    @patch('stock_pipeline.historical_backtest._s3_client')
    @patch('stock_pipeline.historical_backtest.requests.get')
    def test_run_backtest_raises_when_no_results(self, mock_get, mock_s3_client, mock_sleep):
        from stock_pipeline.historical_backtest import run_backtest

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")
        mock_s3_client.return_value = (mock_s3, 'test-bucket')

        mock_get.side_effect = Exception("API down")

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'testkey'}):  # pragma: allowlist secret
            with pytest.raises(Exception, match="No backtest results"):
                run_backtest()

    @patch('stock_pipeline.historical_backtest.sleep', return_value=None)
    @patch('stock_pipeline.historical_backtest._s3_client')
    @patch('stock_pipeline.historical_backtest.requests.get')
    def test_run_backtest_builder_premium_logged(self, mock_get, mock_s3_client, mock_sleep):
        """run_backtest should complete without error when builders and integrators both present."""
        from stock_pipeline.historical_backtest import run_backtest

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")
        mock_s3_client.return_value = (mock_s3, 'test-bucket')

        np.random.seed(7)
        call_count = [0]

        def fake_api(url, timeout=30):
            call_count[0] += 1
            symbol = url.split('symbol=')[1].split('&')[0]
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            np.random.seed(call_count[0])
            resp.json.return_value = self._build_alpha_vantage_response(symbol)
            return resp

        mock_get.side_effect = fake_api

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'testkey'}):  # pragma: allowlist secret
            df = run_backtest()

        builders = df[df['category'] == 'AI Builder']
        integrators = df[df['category'] == 'AI Integrator']
        assert len(builders) == 2
        assert len(integrators) == 2
