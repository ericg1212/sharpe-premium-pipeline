"""Tests for portfolio analysis functions in stock_pipeline/portfolio_analysis.py."""

from stock_pipeline.portfolio_analysis import (
    build_vs_rent_analysis,
    capex_efficiency_analysis,
    value_chain_summary,
    rolling_sharpe_analysis,
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
