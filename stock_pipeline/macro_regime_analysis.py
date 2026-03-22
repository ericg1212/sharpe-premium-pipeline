"""
Macro Regime Analysis: Does the AI Sharpe premium hold across different macro regimes?

Loads FRED macro indicators (GS10, CPIAUCSL, UNRATE, FEDFUNDS) from S3 and
monthly stock prices for AI Builders (META, GOOGL) and AI Integrators (MSFT, AMZN).
Classifies each month into a macro regime and computes the trailing 12-month Sharpe
for each stock, then aggregates the builder premium by regime.

S3 inputs:
  macro_indicators/series={series_id}/year={year}/data.parquet
  historical_prices/symbol={symbol}/monthly.json

S3 outputs:
  analysis/regime_analysis/regime_analysis.parquet
  analysis/regime_analysis/regime_summary.parquet

Local outputs:
  stock_pipeline/regime_analysis.csv
  stock_pipeline/regime_summary.csv
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import json
import logging
import warnings
import requests
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from config import FRED_SERIES, RISK_FREE_RATE, RATE_LIMIT_DELAY
from utils import _s3_client, s3_write_parquet

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BUILDER_STOCKS = ['META', 'GOOGL']
INTEGRATOR_STOCKS = ['MSFT', 'AMZN']
ALL_SYMBOLS = BUILDER_STOCKS + INTEGRATOR_STOCKS

ANALYSIS_START = '2020-01-01'


def _read_parquet_from_s3(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))
    return table.to_pylist()


def load_fred_data(s3, bucket):
    series_ids = list(FRED_SERIES.keys())
    frames = []

    for series_id in series_ids:
        series_records = []
        # Scan years 2020–current
        for year in range(2020, datetime.now().year + 1):
            key = f"macro_indicators/series={series_id}/year={year}/data.parquet"
            try:
                records = _read_parquet_from_s3(s3, bucket, key)
                series_records.extend(records)
            except Exception:
                # Partition missing — normal for future years or sparse data
                pass

        if not series_records:
            logger.warning(f"No S3 data found for FRED series {series_id} — skipping")
            continue

        df = pd.DataFrame(series_records)
        df['date'] = pd.to_datetime(df['date'])
        df = df[['date', 'value', 'series_id']].sort_values('date').reset_index(drop=True)
        frames.append(df)
        logger.info(f"{series_id}: loaded {len(df)} records from S3")

    if not frames:
        raise Exception("No FRED data loaded from S3 — run fred_pipeline first")

    combined = pd.concat(frames, ignore_index=True)
    return combined


def load_stock_prices(s3, bucket):
    prices = {}
    api_key = os.environ['ALPHA_VANTAGE_API_KEY']

    for symbol in ALL_SYMBOLS:
        # Try S3 cache first
        key = f"historical_prices/symbol={symbol}/monthly.json"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj['Body'].read())
            if 'Monthly Adjusted Time Series' in data:
                series = data['Monthly Adjusted Time Series']
                records = []
                for date_str, values in series.items():
                    date = pd.to_datetime(date_str)
                    if date >= pd.Timestamp(ANALYSIS_START):
                        records.append({
                            'date': date,
                            'close': float(values['5. adjusted close']),
                        })
                if records:
                    df = pd.DataFrame(records).sort_values('date').reset_index(drop=True)
                    prices[symbol] = df
                    logger.info(f"{symbol}: loaded {len(df)} months from S3 cache")
                    continue
        except Exception:
            pass

        # Fall back to Alpha Vantage API
        logger.info(f"{symbol}: fetching from Alpha Vantage")
        url = (
            f"https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_MONTHLY_ADJUSTED"
            f"&symbol={symbol}&apikey={api_key}"
        )
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'Monthly Adjusted Time Series' not in data:
                logger.warning(f"{symbol}: no monthly data from Alpha Vantage — skipping")
                continue

            series = data['Monthly Adjusted Time Series']
            records = []
            for date_str, values in series.items():
                date = pd.to_datetime(date_str)
                if date >= pd.Timestamp(ANALYSIS_START):
                    records.append({
                        'date': date,
                        'close': float(values['5. adjusted close']),
                    })

            if not records:
                logger.warning(f"{symbol}: no records after {ANALYSIS_START}")
                continue

            df = pd.DataFrame(records).sort_values('date').reset_index(drop=True)
            prices[symbol] = df
            logger.info(f"{symbol}: fetched {len(df)} months from API")

        except Exception as e:
            logger.warning(f"{symbol}: fetch failed ({e}) — skipping")

    return prices


def classify_regimes(fred_df):
    pivot = fred_df.pivot_table(index='date', columns='series_id', values='value', aggfunc='last')
    pivot = pivot.sort_index()

    result = pd.DataFrame(index=pivot.index)

    if 'GS10' in pivot.columns:
        gs10 = pivot['GS10']
        rolling_mean = gs10.rolling(window=12, min_periods=6).mean()
        result['rate_regime'] = np.where(gs10 > rolling_mean, 'rising', 'falling')
    else:
        result['rate_regime'] = 'unknown'

    if 'CPIAUCSL' in pivot.columns:
        cpi = pivot['CPIAUCSL']
        # YoY change: (current / 12-months-ago) - 1, expressed as percent
        cpi_yoy = cpi.pct_change(periods=12) * 100
        result['inflation_regime'] = np.where(cpi_yoy > 4.0, 'high', 'normal')
    else:
        result['inflation_regime'] = 'unknown'

    if 'UNRATE' in pivot.columns:
        unrate = pivot['UNRATE']
        result['unemployment_regime'] = np.where(unrate > 5.5, 'elevated', 'normal')
    else:
        result['unemployment_regime'] = 'unknown'

    if 'FEDFUNDS' in pivot.columns:
        result['fedfunds'] = pivot['FEDFUNDS']
    else:
        result['fedfunds'] = RISK_FREE_RATE * 100  # fallback: constant

    result['combined_regime'] = (
        result['rate_regime'] + '_' +
        result['inflation_regime'] + '_' +
        result['unemployment_regime']
    )

    result = result.reset_index().rename(columns={'date': 'month'})
    # Normalize month to month-end for alignment with stock price dates
    result['month'] = result['month'].dt.to_period('M').dt.to_timestamp('M')
    return result


def compute_monthly_returns(prices_df):
    df = prices_df.sort_values('date').copy()
    df['monthly_return'] = df['close'].pct_change()
    df['month'] = df['date'].dt.to_period('M').dt.to_timestamp('M')
    return df[['month', 'monthly_return']].dropna()


def compute_trailing_sharpe(returns_series, rf_monthly, window=12):
    """Compute trailing window-month Sharpe at each period. Returns Series indexed by position."""
    returns = returns_series.values
    sharpes = [np.nan] * len(returns)

    for end in range(window, len(returns) + 1):
        window_ret = returns[end - window:end]
        excess = window_ret - rf_monthly
        vol = np.std(excess, ddof=1)
        if vol > 0:
            sharpes[end - 1] = float(np.mean(excess) / vol * np.sqrt(12))
        else:
            sharpes[end - 1] = 0.0

    return sharpes


def build_monthly_sharpe(prices, regime_df):
    monthly_data = {}

    for symbol in ALL_SYMBOLS:
        if symbol not in prices:
            continue
        returns_df = compute_monthly_returns(prices[symbol])
        monthly_data[symbol] = returns_df.set_index('month')['monthly_return']

    if not monthly_data:
        raise Exception("No stock return data available")

    # Common months across all available symbols and regime data
    regime_months = set(regime_df['month'])
    all_months = sorted(set.union(*[set(s.index) for s in monthly_data.values()]) & regime_months)

    rows = []
    for month in all_months:
        regime_row = regime_df[regime_df['month'] == month]
        if regime_row.empty:
            continue
        regime_row = regime_row.iloc[0]

        rf_annual = regime_row['fedfunds'] / 100
        rf_monthly = rf_annual / 12

        row = {
            'month': month,
            'rate_regime': regime_row['rate_regime'],
            'inflation_regime': regime_row['inflation_regime'],
            'unemployment_regime': regime_row['unemployment_regime'],
            'combined_regime': regime_row['combined_regime'],
            'fedfunds': regime_row['fedfunds'],
        }

        for symbol in ALL_SYMBOLS:
            if symbol not in monthly_data:
                row[f'{symbol}_sharpe'] = np.nan
                continue
            s = monthly_data[symbol]
            # Get all returns up to this month
            historical = s[s.index <= month].dropna()
            if len(historical) < 12:
                row[f'{symbol}_sharpe'] = np.nan
                continue
            window_ret = historical.values[-12:]
            excess = window_ret - rf_monthly
            vol = np.std(excess, ddof=1)
            if vol > 0:
                row[f'{symbol}_sharpe'] = float(np.mean(excess) / vol * np.sqrt(12))
            else:
                row[f'{symbol}_sharpe'] = 0.0

        rows.append(row)

    monthly_df = pd.DataFrame(rows)

    # Builder and integrator averages
    builder_cols = [f'{s}_sharpe' for s in BUILDER_STOCKS if f'{s}_sharpe' in monthly_df.columns]
    integrator_cols = [f'{s}_sharpe' for s in INTEGRATOR_STOCKS if f'{s}_sharpe' in monthly_df.columns]

    monthly_df['avg_builder_sharpe'] = monthly_df[builder_cols].mean(axis=1)
    monthly_df['avg_integrator_sharpe'] = monthly_df[integrator_cols].mean(axis=1)
    monthly_df['builder_premium'] = monthly_df['avg_builder_sharpe'] - monthly_df['avg_integrator_sharpe']

    return monthly_df


def aggregate_by_regime(monthly_df):
    agg = (
        monthly_df
        .dropna(subset=['avg_builder_sharpe', 'avg_integrator_sharpe'])
        .groupby('combined_regime')
        .agg(
            avg_builder_sharpe=('avg_builder_sharpe', 'mean'),
            avg_integrator_sharpe=('avg_integrator_sharpe', 'mean'),
            builder_premium=('builder_premium', 'mean'),
            n_months=('month', 'count'),
        )
        .round(3)
        .sort_values('builder_premium', ascending=False)
        .reset_index()
    )
    return agg


def print_summary(regime_summary, monthly_df):
    logger.info("=" * 70)
    logger.info("MACRO REGIME ANALYSIS: AI Builder Premium by Regime")
    logger.info("=" * 70)
    logger.info(f"  Months analyzed: {len(monthly_df)}")
    logger.info(f"  Regime combinations observed: {len(regime_summary)}")
    logger.info("")
    logger.info(f"  {'Regime':<35} {'Builder':>8} {'Integrator':>10} {'Premium':>8} {'Months':>7}")
    logger.info("  " + "-" * 68)
    for _, row in regime_summary.iterrows():
        logger.info(
            f"  {row['combined_regime']:<35} "
            f"{row['avg_builder_sharpe']:>8.3f} "
            f"{row['avg_integrator_sharpe']:>10.3f} "
            f"{row['builder_premium']:>+8.3f} "
            f"{int(row['n_months']):>7}"
        )
    logger.info("=" * 70)

    # Overall premium
    valid = monthly_df.dropna(subset=['builder_premium'])
    if not valid.empty:
        overall = valid['builder_premium'].mean()
        logger.info(f"  Overall avg builder premium: {overall:+.3f} Sharpe units")
        logger.info("=" * 70)


def run_regime_analysis():
    s3, bucket = _s3_client()

    logger.info("Loading FRED macro data from S3...")
    fred_df = load_fred_data(s3, bucket)

    logger.info("Loading historical stock prices...")
    prices = load_stock_prices(s3, bucket)

    if not prices:
        raise Exception("No stock price data available — check Alpha Vantage key or S3 cache")

    logger.info("Classifying macro regimes...")
    regime_df = classify_regimes(fred_df)

    logger.info("Computing per-period trailing Sharpe ratios...")
    monthly_df = build_monthly_sharpe(prices, regime_df)

    if monthly_df.empty:
        raise Exception("No monthly data produced — check date alignment between FRED and price data")

    logger.info("Aggregating by regime...")
    regime_summary = aggregate_by_regime(monthly_df)

    print_summary(regime_summary, monthly_df)

    output_dir = os.path.dirname(os.path.abspath(__file__))

    regime_csv = os.path.join(output_dir, 'regime_analysis.csv')
    monthly_df.to_csv(regime_csv, index=False)
    logger.info(f"Monthly regime data saved to {regime_csv}")

    summary_csv = os.path.join(output_dir, 'regime_summary.csv')
    regime_summary.to_csv(summary_csv, index=False)
    logger.info(f"Regime summary saved to {summary_csv}")

    # Save to S3 as Parquet
    monthly_records = monthly_df.copy()
    monthly_records['month'] = monthly_records['month'].astype(str)
    s3_write_parquet(s3, bucket, 'analysis/regime_analysis/regime_analysis.parquet',
                     monthly_records.to_dict('records'))
    logger.info(f"Monthly data written to s3://{bucket}/analysis/regime_analysis/regime_analysis.parquet")

    s3_write_parquet(s3, bucket, 'analysis/regime_analysis/regime_summary.parquet',
                     regime_summary.to_dict('records'))
    logger.info(f"Summary written to s3://{bucket}/analysis/regime_analysis/regime_summary.parquet")

    return monthly_df, regime_summary


if __name__ == '__main__':
    run_regime_analysis()
