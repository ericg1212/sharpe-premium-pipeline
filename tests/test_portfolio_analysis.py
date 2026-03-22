"""Tests for portfolio analysis functions in stock_pipeline/portfolio_analysis.py."""

import os
import csv
import tempfile
import pytest
from unittest.mock import patch

from stock_pipeline.portfolio_analysis import (
    build_vs_rent_analysis,
    capex_efficiency_analysis,
    value_chain_summary,
    rolling_sharpe_analysis,
    save_analysis,
    AI_CAPEX,
)


class TestBuildVsRentAnalysis:
    """Tests for build_vs_rent_analysis()."""

    def test_returns_correct_premium(self, sample_backtest_results):
        """Premium should be (builder_sharpe - integrator_sharpe) / |integrator_sharpe| * 100."""
        result = build_vs_rent_analysis(sample_backtest_results)

        # Builders: META=2.369, GOOGL=1.979 -> avg=2.174
        # Integrators: MSFT=1.512, AMZN=1.232 -> avg=1.372
        expected_b = (2.369 + 1.979) / 2
        expected_i = (1.512 + 1.232) / 2
        expected_premium = (expected_b - expected_i) / abs(expected_i) * 100

        assert abs(result['builder_sharpe'] - expected_b) < 0.001
        assert abs(result['integrator_sharpe'] - expected_i) < 0.001
        assert abs(result['premium_pct'] - round(expected_premium, 1)) < 0.1

    def test_returns_return_and_vol_metrics(self, sample_backtest_results):
        """Result should include return and volatility comparisons."""
        result = build_vs_rent_analysis(sample_backtest_results)

        assert 'builder_return' in result
        assert 'integrator_return' in result
        assert 'builder_vol' in result
        assert 'integrator_vol' in result

    def test_returns_none_without_builders(self, sample_backtest_results):
        """Missing AI Builder category should return None."""
        filtered = [r for r in sample_backtest_results if r['category'] != 'AI Builder']
        result = build_vs_rent_analysis(filtered)
        assert result is None

    def test_returns_none_without_integrators(self, sample_backtest_results):
        """Missing AI Integrator category should return None."""
        filtered = [r for r in sample_backtest_results if r['category'] != 'AI Integrator']
        result = build_vs_rent_analysis(filtered)
        assert result is None


class TestCapexEfficiencyAnalysis:
    """Tests for capex_efficiency_analysis()."""

    def test_returns_rows_for_tracked_stocks(self, sample_backtest_results):
        """Should return a row for each stock that has AI_CAPEX data."""
        rows = capex_efficiency_analysis(sample_backtest_results)

        tracked_symbols = {r['symbol'] for r in rows}
        # From sample data, META, GOOGL, MSFT, AMZN are in AI_CAPEX
        expected = {'META', 'GOOGL', 'MSFT', 'AMZN'}
        assert tracked_symbols == expected

    def test_efficiency_calculation(self, sample_backtest_results):
        """Sharpe per $B should be sharpe / ai_spend * 100."""
        rows = capex_efficiency_analysis(sample_backtest_results)

        meta_row = next(r for r in rows if r['symbol'] == 'META')
        # META: capex_2026=125, ai_pct=95 -> ai_spend=118.75
        expected_spend = 125.0 * 95 / 100
        expected_eff = 2.369 / expected_spend * 100
        assert abs(meta_row['ai_spend_2026'] - round(expected_spend, 1)) < 0.1
        assert abs(meta_row['sharpe_per_B'] - round(expected_eff, 4)) < 0.001

    def test_ignores_stocks_without_capex(self, sample_backtest_results):
        """Stocks like CRM/NVDA without AI_CAPEX data should be excluded."""
        rows = capex_efficiency_analysis(sample_backtest_results)
        symbols = {r['symbol'] for r in rows}
        assert 'NVDA' not in symbols
        assert 'CRM' not in symbols

    def test_all_capex_stocks_have_required_fields(self):
        """Every entry in AI_CAPEX should have the required fields."""
        for sym, data in AI_CAPEX.items():
            assert 'capex_2025_B' in data, f"{sym} missing capex_2025_B"
            assert 'capex_2026_B' in data, f"{sym} missing capex_2026_B"
            assert 'ai_pct' in data, f"{sym} missing ai_pct"


class TestValueChainSummary:
    """Tests for value_chain_summary()."""

    def test_returns_summary_rows(self, sample_backtest_results):
        """Should return one row per category present in the data."""
        rows = value_chain_summary(sample_backtest_results)
        categories = {r['category'] for r in rows}
        assert 'AI Builder' in categories
        assert 'AI Integrator' in categories
        assert 'Infrastructure' in categories

    def test_averages_are_correct(self, sample_backtest_results):
        """Category averages should match manual calculation."""
        rows = value_chain_summary(sample_backtest_results)
        builder_row = next(r for r in rows if r['category'] == 'AI Builder')

        expected_sharpe = round((2.369 + 1.979) / 2, 3)
        assert builder_row['avg_sharpe'] == expected_sharpe
        assert builder_row['stock_count'] == 2


class TestRollingSharpeCsv:
    """Tests for rolling_sharpe_analysis()."""

    def test_flattens_rolling_series_into_rows(self):
        results = [
            {
                'symbol': 'META', 'category': 'AI Builder',
                'rolling_sharpe': [
                    {'date': '2024-01-31', 'rolling_sharpe_12m': 2.1},
                    {'date': '2024-02-29', 'rolling_sharpe_12m': 2.3},
                ],
            },
            {
                'symbol': 'MSFT', 'category': 'AI Integrator',
                'rolling_sharpe': [
                    {'date': '2024-01-31', 'rolling_sharpe_12m': 1.5},
                ],
            },
        ]
        rows = rolling_sharpe_analysis(results)

        assert len(rows) == 3
        assert all('symbol' in r and 'date' in r and 'rolling_sharpe_12m' in r for r in rows)
        assert rows[0]['symbol'] == 'META'
        assert rows[2]['symbol'] == 'MSFT'

    def test_handles_missing_rolling_sharpe_key(self):
        """Results without a rolling_sharpe key should produce no rows."""
        results = [{'symbol': 'CRM', 'category': 'Legacy Tech'}]
        rows = rolling_sharpe_analysis(results)
        assert rows == []

    def test_category_included_in_each_row(self):
        results = [
            {
                'symbol': 'GOOGL', 'category': 'AI Builder',
                'rolling_sharpe': [{'date': '2024-06-30', 'rolling_sharpe_12m': 1.8}],
            }
        ]
        rows = rolling_sharpe_analysis(results)
        assert rows[0]['category'] == 'AI Builder'


class TestSaveAnalysis:
    """Tests for save_analysis() — file write paths."""

    def _minimal_results(self):
        return [
            {
                'symbol': 'META', 'category': 'AI Builder',
                'ai_strategy': 'Proprietary (Llama, MTIA chips)',
                'annualized_return': 45.2, 'annualized_volatility': 28.1,
                'sharpe_ratio': 2.369, 'max_drawdown': -18.5,
                'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31',
                'capex_2025_B': 72.2, 'capex_2026_B': 125.0,
                'ai_pct_of_capex': 95, 'est_ai_spend_2026_B': 118.8,
                'rolling_sharpe': [{'date': '2024-01-31', 'rolling_sharpe_12m': 2.1}],
            },
            {
                'symbol': 'MSFT', 'category': 'AI Integrator',
                'ai_strategy': 'Partnership (OpenAI)',
                'annualized_return': 28.1, 'annualized_volatility': 22.5,
                'sharpe_ratio': 1.512, 'max_drawdown': -12.3,
                'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31',
                'capex_2025_B': 80.0, 'capex_2026_B': 145.0,
                'ai_pct_of_capex': 60, 'est_ai_spend_2026_B': 87.0,
                'rolling_sharpe': [{'date': '2024-01-31', 'rolling_sharpe_12m': 1.4}],
            },
        ]

    def test_save_creates_csv_files(self, sample_backtest_results):
        with tempfile.TemporaryDirectory() as tmpdir:
            results = self._minimal_results()
            build_rent = build_vs_rent_analysis(results)
            capex_rows = capex_efficiency_analysis(results)
            chain_summary = value_chain_summary(results)
            rolling_rows = rolling_sharpe_analysis(results)

            with patch('stock_pipeline.portfolio_analysis.os.path.dirname', return_value=tmpdir):
                save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows)

            assert os.path.exists(os.path.join(tmpdir, 'backtest_results.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'powerbi_master.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'category_summary.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'build_vs_rent.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'capex_efficiency.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'value_chain_summary.csv'))
            assert os.path.exists(os.path.join(tmpdir, 'rolling_sharpe.csv'))

    def test_powerbi_master_has_build_or_rent_column(self):
        results = self._minimal_results()
        build_rent = build_vs_rent_analysis(results)
        capex_rows = capex_efficiency_analysis(results)
        chain_summary = value_chain_summary(results)
        rolling_rows = rolling_sharpe_analysis(results)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('stock_pipeline.portfolio_analysis.os.path.dirname', return_value=tmpdir):
                save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows)

            pbi_path = os.path.join(tmpdir, 'powerbi_master.csv')
            with open(pbi_path) as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2
            assert rows[0]['build_or_rent'] == 'Build'
            assert rows[1]['build_or_rent'] == 'Rent'

    def test_category_summary_averages(self):
        results = self._minimal_results()
        build_rent = build_vs_rent_analysis(results)
        capex_rows = capex_efficiency_analysis(results)
        chain_summary = value_chain_summary(results)
        rolling_rows = rolling_sharpe_analysis(results)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('stock_pipeline.portfolio_analysis.os.path.dirname', return_value=tmpdir):
                save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows)

            cat_path = os.path.join(tmpdir, 'category_summary.csv')
            with open(cat_path) as f:
                reader = csv.DictReader(f)
                rows = {r['category']: r for r in reader}

            assert 'AI Builder' in rows
            assert float(rows['AI Builder']['avg_sharpe']) == pytest.approx(2.369, abs=0.001)

    def test_rolling_sharpe_csv_not_written_when_empty(self):
        results = [
            {
                'symbol': 'META', 'category': 'AI Builder',
                'ai_strategy': 'X',
                'annualized_return': 45.2, 'annualized_volatility': 28.1,
                'sharpe_ratio': 2.369, 'max_drawdown': -18.5,
                'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31',
                'rolling_sharpe': [],
            },
            {
                'symbol': 'MSFT', 'category': 'AI Integrator',
                'ai_strategy': 'Y',
                'annualized_return': 28.1, 'annualized_volatility': 22.5,
                'sharpe_ratio': 1.512, 'max_drawdown': -12.3,
                'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31',
                'rolling_sharpe': [],
            },
        ]
        build_rent = build_vs_rent_analysis(results)
        capex_rows = capex_efficiency_analysis(results)
        chain_summary = value_chain_summary(results)
        rolling_rows = rolling_sharpe_analysis(results)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('stock_pipeline.portfolio_analysis.os.path.dirname', return_value=tmpdir):
                save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows)

            assert not os.path.exists(os.path.join(tmpdir, 'rolling_sharpe.csv'))
