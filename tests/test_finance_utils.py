"""Tests for finance_utils: calculate_annualized_return, calculate_max_drawdown,
calculate_beta, calculate_rolling_sharpe."""

import pytest
from stock_pipeline.finance_utils import (
    calculate_annualized_return,
    calculate_max_drawdown,
    calculate_beta,
    calculate_rolling_sharpe,
)

# ── calculate_annualized_return ────────────────────────────────────────────────

class TestCalculateAnnualizedReturn:
    """Tests for calculate_annualized_return()."""

    def test_returns_none_for_fewer_than_12_months(self):
        result = calculate_annualized_return([0.01] * 11)
        assert result is None

    def test_zero_returns_give_zero_annualized(self):
        result = calculate_annualized_return([0.0] * 12)
        assert abs(result) < 1e-9

    def test_flat_monthly_return_compounds_correctly(self):
        # 1% per month for 12 months → (1.01)^12 - 1 ≈ 12.68%
        result = calculate_annualized_return([0.01] * 12)
        expected = (1.01 ** 12) - 1
        assert abs(result - expected) < 0.0001

    def test_accepts_more_than_12_months(self):
        result = calculate_annualized_return([0.005] * 24)
        assert result is not None
        assert result > 0


# ── calculate_max_drawdown ─────────────────────────────────────────────────────

class TestCalculateMaxDrawdown:
    """Tests for calculate_max_drawdown()."""

    def test_monotonically_rising_prices_return_zero(self):
        result = calculate_max_drawdown([100, 110, 120, 130])
        assert result == 0.0

    def test_single_price_returns_zero(self):
        result = calculate_max_drawdown([100])
        assert result == 0.0

    def test_known_drawdown_is_correct(self):
        # Peak 100, trough 65 → drawdown = (65-100)/100 = -0.35
        prices = [100, 90, 65, 80, 95]
        result = calculate_max_drawdown(prices)
        assert abs(result - (-0.35)) < 0.001

    def test_result_is_negative_or_zero(self):
        prices = [100, 90, 80, 70, 60]
        result = calculate_max_drawdown(prices)
        assert result <= 0.0


# ── calculate_beta ─────────────────────────────────────────────────────────────

class TestCalculateBeta:
    """Tests for calculate_beta()."""

    def test_returns_none_for_fewer_than_12_observations(self):
        result = calculate_beta([0.01] * 11, [0.01] * 11)
        assert result is None

    def test_returns_none_for_mismatched_lengths(self):
        result = calculate_beta([0.01] * 12, [0.01] * 13)
        assert result is None

    def test_identical_returns_give_beta_of_one(self):
        returns = [0.01, -0.02, 0.03, -0.01, 0.02, 0.01,
                   -0.03, 0.02, 0.01, -0.01, 0.03, 0.01]
        result = calculate_beta(returns, returns)
        assert abs(result - 1.0) < 1e-9

    def test_returns_float_for_valid_inputs(self):
        stock = [0.01, -0.02, 0.03, -0.01, 0.02, 0.01,
                 -0.03, 0.02, 0.01, -0.01, 0.03, 0.01]
        market = [0.005, -0.01, 0.015, -0.005, 0.01, 0.005,
                  -0.015, 0.01, 0.005, -0.005, 0.015, 0.005]
        result = calculate_beta(stock, market)
        assert isinstance(result, float)


# ── calculate_rolling_sharpe ───────────────────────────────────────────────────

class TestCalculateRollingSharpe:
    """Tests for calculate_rolling_sharpe()."""

    def test_returns_empty_list_when_fewer_than_window_observations(self):
        result = calculate_rolling_sharpe([0.01] * 11, risk_free_rate=0.04)
        assert result == []

    def test_exactly_window_size_returns_one_tuple(self):
        result = calculate_rolling_sharpe([0.01] * 12, risk_free_rate=0.04)
        assert len(result) == 1

    def test_result_is_list_of_tuples(self):
        result = calculate_rolling_sharpe([0.01] * 24, risk_free_rate=0.04)
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, tuple)
            assert len(item) == 2

    def test_zero_excess_returns_give_zero_sharpe(self):
        # Monthly return equals monthly risk-free → excess = 0 → Sharpe = 0
        monthly_rf = 0.04 / 12
        returns = [monthly_rf] * 12
        result = calculate_rolling_sharpe(returns, risk_free_rate=0.04)
        assert abs(result[0][1]) < 1e-9
