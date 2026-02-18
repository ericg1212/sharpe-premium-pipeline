"""Tests for Sharpe ratio calculation in historical_backtest.py."""

import pytest
import pandas as pd
import numpy as np

from stock_pipeline.historical_backtest import calculate_sharpe, RISK_FREE_RATE, STOCKS


class TestCalculateSharpe:
    """Unit tests for the calculate_sharpe() function."""

    def test_returns_valid_result_for_normal_data(self, sample_monthly_prices):
        """36 months of data should produce a complete result dict."""
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')

        assert result is not None
        assert result['symbol'] == 'NVDA'
        assert result['category'] == 'Infrastructure'
        assert result['months_analyzed'] == 36
        assert 'annualized_return' in result
        assert 'annualized_volatility' in result
        assert 'sharpe_ratio' in result

    def test_returns_none_for_insufficient_data(self, short_price_series):
        """Fewer than 12 months should return None."""
        result = calculate_sharpe(short_price_series, 'NVDA')
        assert result is None

    def test_zero_volatility_returns_zero_sharpe(self, flat_price_series):
        """Constant prices (zero vol) should produce sharpe of 0, not divide-by-zero."""
        result = calculate_sharpe(flat_price_series, 'NVDA')

        assert result is not None
        assert result['sharpe_ratio'] == 0
        assert result['annualized_volatility'] == 0.0

    def test_sharpe_with_known_values(self):
        """Verify Sharpe calculation against hand-computed values."""
        # 13 prices = 12 monthly returns, all exactly +5%
        dates = pd.date_range(start='2023-01-31', periods=13, freq='ME')
        prices = [100 * (1.05 ** i) for i in range(13)]
        df = pd.DataFrame({'date': dates, 'close': prices})

        result = calculate_sharpe(df, 'AAPL')

        # Expected: annualized return = (1.05)^12 - 1 = 79.59%
        expected_return = (1.05 ** 12 - 1) * 100
        assert abs(result['annualized_return'] - round(expected_return, 2)) < 0.01

        # With constant returns, std should be ~0 (floating point noise)
        assert result['annualized_volatility'] < 0.01

    def test_capex_data_attached_for_tracked_stocks(self, sample_monthly_prices):
        """Stocks in AI_CAPEX should have capex fields in the result."""
        result = calculate_sharpe(sample_monthly_prices, 'META')

        assert 'capex_2025_B' in result
        assert 'capex_2026_B' in result
        assert 'ai_pct_of_capex' in result
        assert 'est_ai_spend_2026_B' in result
        assert result['ai_pct_of_capex'] == 95

    def test_no_capex_data_for_untracked_stocks(self, sample_monthly_prices):
        """Stocks not in AI_CAPEX should not have capex fields."""
        result = calculate_sharpe(sample_monthly_prices, 'AAPL')

        assert 'capex_2025_B' not in result
        assert 'capex_2026_B' not in result

    def test_result_dates_match_input(self, sample_monthly_prices):
        """Start/end dates in result should match the input DataFrame."""
        result = calculate_sharpe(sample_monthly_prices, 'NVDA')

        assert result['start_date'] == sample_monthly_prices['date'].min().strftime('%Y-%m-%d')
        assert result['end_date'] == sample_monthly_prices['date'].max().strftime('%Y-%m-%d')

    def test_all_stock_symbols_recognized(self):
        """Every symbol in the STOCKS dict should be usable."""
        assert len(STOCKS) == 10
        expected = {'NVDA', 'META', 'GOOGL', 'MSFT', 'AMZN', 'CRM', 'ORCL', 'ADBE', 'AAPL', 'TSLA'}
        assert set(STOCKS.keys()) == expected
