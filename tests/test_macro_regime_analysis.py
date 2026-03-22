"""Tests for pure functions in stock_pipeline/macro_regime_analysis.py."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import re
import math
import json
import pytest
import numpy as np
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws
from unittest.mock import patch

from stock_pipeline.macro_regime_analysis import (
    classify_regimes,
    compute_trailing_sharpe,
    aggregate_by_regime,
    load_fred_data,
    build_monthly_sharpe,
    BUILDER_STOCKS,
    INTEGRATOR_STOCKS,
    ALL_SYMBOLS,
)
from config import FRED_SERIES


def _build_fred_df(n_months=24, gs10_start=3.0, gs10_end=6.0,
                   cpi_yoy_pct=2.0, unrate=4.5):
    dates = pd.date_range(start='2022-01-01', periods=n_months, freq='MS')

    gs10_values = np.linspace(gs10_start, gs10_end, n_months)

    # Build CPI values that produce the target YoY %: CPI(t) = CPI(t-12) * (1 + pct/100)
    cpi_base = 280.0
    cpi_values = []
    for i in range(n_months):
        if i < 12:
            cpi_values.append(cpi_base * (1 + i * 0.002))
        else:
            cpi_values.append(cpi_values[i - 12] * (1 + cpi_yoy_pct / 100))

    unrate_values = [unrate] * n_months
    fedfunds_values = [4.5] * n_months

    rows = []
    for i, dt in enumerate(dates):
        rows += [
            {'date': dt, 'value': gs10_values[i], 'series_id': 'GS10'},
            {'date': dt, 'value': cpi_values[i], 'series_id': 'CPIAUCSL'},
            {'date': dt, 'value': unrate_values[i], 'series_id': 'UNRATE'},
            {'date': dt, 'value': fedfunds_values[i], 'series_id': 'FEDFUNDS'},
        ]

    return pd.DataFrame(rows)


class TestClassifyRegimes:

    def test_rate_regime_rising_when_above_rolling_mean(self):
        df = _build_fred_df(n_months=24, gs10_start=2.0, gs10_end=8.0)
        result = classify_regimes(df)
        # Later months have GS10 above the rolling mean → expect 'rising'
        late = result.tail(6)
        assert (late['rate_regime'] == 'rising').all()

    def test_inflation_regime_high_when_yoy_above_4pct(self):
        df = _build_fred_df(n_months=24, cpi_yoy_pct=6.0)
        result = classify_regimes(df)
        # After the first 12 months (needed for YoY), all should be 'high'
        late = result.tail(6)
        assert (late['inflation_regime'] == 'high').all()

    def test_unemployment_regime_elevated_when_above_threshold(self):
        df = _build_fred_df(n_months=24, unrate=7.0)
        result = classify_regimes(df)
        assert (result['unemployment_regime'] == 'elevated').all()

    def test_combined_regime_string_format(self):
        df = _build_fred_df(n_months=24)
        result = classify_regimes(df)
        pattern = re.compile(r'^(rising|falling|unknown)_(high|normal|unknown)_(elevated|normal|unknown)$')
        for val in result['combined_regime']:
            assert pattern.match(val), f"Unexpected combined_regime: {val}"


class TestComputeTrailingSharpe:

    def test_trailing_sharpe_returns_nan_for_insufficient_data(self):
        returns = pd.Series([0.01, 0.02, -0.01, 0.015, 0.005, 0.02])
        rf_monthly = 0.045 / 12
        sharpes = compute_trailing_sharpe(returns, rf_monthly, window=12)
        # All positions should be NaN since length < window
        assert all(math.isnan(v) for v in sharpes)

    def test_trailing_sharpe_returns_float_for_full_window(self):
        np.random.seed(0)
        returns = pd.Series(np.random.normal(0.015, 0.05, 24))
        rf_monthly = 0.045 / 12
        sharpes = compute_trailing_sharpe(returns, rf_monthly, window=12)
        non_nan = [v for v in sharpes if not math.isnan(v)]
        assert len(non_nan) > 0
        assert all(isinstance(v, float) for v in non_nan)

    def test_trailing_sharpe_positive_for_uptrending_returns(self):
        returns = pd.Series(np.full(24, 0.03))
        rf_monthly = 0.045 / 12
        # Constant positive returns above rf → Sharpe should be positive where vol > 0
        # But constant returns → vol = 0 → sharpe = 0.0 (not nan, not negative)
        sharpes = compute_trailing_sharpe(returns, rf_monthly, window=12)
        non_nan = [v for v in sharpes if not math.isnan(v)]
        assert all(v >= 0.0 for v in non_nan)


class TestAggregateByRegime:

    def _build_monthly_df(self):
        regimes = ['rising_high_elevated', 'falling_normal_normal', 'rising_high_elevated',
                   'falling_normal_normal', 'rising_normal_normal']
        builder_sharpes = [2.0, 1.0, 2.5, 0.8, 1.8]
        integrator_sharpes = [1.0, 0.5, 1.5, 0.4, 0.9]

        dates = pd.date_range('2023-01-31', periods=5, freq='ME')
        df = pd.DataFrame({
            'month': dates,
            'combined_regime': regimes,
            'avg_builder_sharpe': builder_sharpes,
            'avg_integrator_sharpe': integrator_sharpes,
            'builder_premium': [b - i for b, i in zip(builder_sharpes, integrator_sharpes)],
        })
        return df

    def test_aggregate_sorts_by_premium_descending(self):
        df = self._build_monthly_df()
        result = aggregate_by_regime(df)
        premiums = result['builder_premium'].tolist()
        assert premiums == sorted(premiums, reverse=True)

    def test_aggregate_handles_nan_rows(self):
        df = self._build_monthly_df()
        # Inject NaN rows
        df.loc[0, 'avg_builder_sharpe'] = float('nan')
        df.loc[1, 'avg_integrator_sharpe'] = float('nan')
        result = aggregate_by_regime(df)
        # NaN rows dropped — only non-NaN rows aggregated
        total_months = result['n_months'].sum()
        assert total_months == 3


# ---------------------------------------------------------------------------
# Helpers for S3 tests
# ---------------------------------------------------------------------------

def _parquet_bytes(records, schema=None):
    if schema:
        table = pa.Table.from_pylist(records, schema=schema)
    else:
        table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


def _setup_fred_parquet(s3_client, bucket, series_id, year, records):
    key = f"macro_indicators/series={series_id}/year={year}/data.parquet"
    schema = pa.schema([
        pa.field('series_id', pa.string()),
        pa.field('date', pa.string()),
        pa.field('value', pa.float64()),
    ])
    body = _parquet_bytes(records, schema=schema)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


# ---------------------------------------------------------------------------
# TestLoadFredData
# ---------------------------------------------------------------------------

class TestLoadFredData:

    @mock_aws
    def test_load_fred_data_returns_combined_dataframe(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')

        s3 = boto3.client('s3', region_name='us-east-1')
        bucket = 'test-fred-bucket'
        s3.create_bucket(Bucket=bucket)

        _setup_fred_parquet(s3, bucket, 'GS10', 2023, [
            {'series_id': 'GS10', 'date': '2023-01-01', 'value': 3.8},
            {'series_id': 'GS10', 'date': '2023-02-01', 'value': 3.9},
        ])
        _setup_fred_parquet(s3, bucket, 'CPIAUCSL', 2023, [
            {'series_id': 'CPIAUCSL', 'date': '2023-01-01', 'value': 301.0},
            {'series_id': 'CPIAUCSL', 'date': '2023-02-01', 'value': 302.0},
        ])

        result = load_fred_data(s3, bucket)

        assert isinstance(result, pd.DataFrame)
        series_in_result = set(result['series_id'].unique())
        assert 'GS10' in series_in_result
        assert 'CPIAUCSL' in series_in_result

    @mock_aws
    def test_load_fred_data_skips_missing_series(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')

        s3 = boto3.client('s3', region_name='us-east-1')
        bucket = 'test-fred-partial'
        s3.create_bucket(Bucket=bucket)

        # Only write GS10 — the other 3 FRED series have no data
        _setup_fred_parquet(s3, bucket, 'GS10', 2023, [
            {'series_id': 'GS10', 'date': '2023-01-01', 'value': 3.8},
        ])

        result = load_fred_data(s3, bucket)

        assert isinstance(result, pd.DataFrame)
        assert set(result['series_id'].unique()) == {'GS10'}
        assert len(result) == 1

    @mock_aws
    def test_load_fred_data_raises_when_all_missing(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')

        s3 = boto3.client('s3', region_name='us-east-1')
        bucket = 'test-fred-empty'
        s3.create_bucket(Bucket=bucket)
        # Write nothing — all series partitions missing

        with pytest.raises(Exception, match="run fred_pipeline"):
            load_fred_data(s3, bucket)


# ---------------------------------------------------------------------------
# TestBuildMonthlySharpe
# ---------------------------------------------------------------------------

def _make_price_df(n_months=36, seed=0, start='2020-01-31', monthly_ret=0.02, vol=0.04):
    np.random.seed(seed)
    dates = pd.date_range(start=start, periods=n_months + 1, freq='ME')
    returns = np.random.normal(monthly_ret, vol, n_months)
    prices = [100.0]
    for r in returns:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame({'date': dates, 'close': prices})


def _make_regime_df(months):
    return pd.DataFrame({
        'month': months,
        'rate_regime': 'rising',
        'inflation_regime': 'normal',
        'unemployment_regime': 'normal',
        'combined_regime': 'rising_normal_normal',
        'fedfunds': 4.5,
    })


class TestBuildMonthlySharpe:

    def test_build_monthly_sharpe_produces_premium_column(self):
        prices = {sym: _make_price_df(36, seed=i) for i, sym in enumerate(ALL_SYMBOLS)}

        all_months = sorted(
            set.union(*[set(df['date'].dt.to_period('M').dt.to_timestamp('M')) for df in prices.values()])
        )
        regime_df = _make_regime_df(all_months)

        result = build_monthly_sharpe(prices, regime_df)

        assert 'builder_premium' in result.columns
        valid_premium = result['builder_premium'].dropna()
        assert len(valid_premium) > 0
        assert not valid_premium.isna().all()

    def test_build_monthly_sharpe_skips_symbols_with_insufficient_data(self):
        # Only 6 months per symbol — all trailing Sharpe values should be NaN
        prices = {sym: _make_price_df(6, seed=i) for i, sym in enumerate(ALL_SYMBOLS)}

        all_months = sorted(
            set.union(*[set(df['date'].dt.to_period('M').dt.to_timestamp('M')) for df in prices.values()])
        )
        regime_df = _make_regime_df(all_months)

        result = build_monthly_sharpe(prices, regime_df)

        for sym in ALL_SYMBOLS:
            col = f'{sym}_sharpe'
            if col in result.columns:
                assert result[col].isna().all(), f"{sym} should have all-NaN sharpe with 6 months"
