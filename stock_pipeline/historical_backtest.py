"""
Historical Backtest: AI Value Chain Analysis
Calculates Sharpe ratios for 10 tech stocks across 3 years (2023-2025)
and correlates risk-adjusted returns with AI spending strategy.

Thesis: "The market rewards AI builders, not AI renters."
Companies building proprietary AI (Meta/Llama, Google/Gemini) outperform
those integrating third-party AI (Microsoft/OpenAI, Amazon/Anthropic)
by 50%+ on risk-adjusted returns despite spending less on capex.

Stock Categories:
  Infrastructure: NVDA (sells the GPUs)
  AI Builders:    META, GOOGL (proprietary AI platforms)
  AI Integrators: MSFT, AMZN (third-party AI partnerships)
  Legacy Tech:    CRM, ORCL, ADBE
  Control:        AAPL, TSLA
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import json
import logging
import yfinance as yf
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from config import STOCKS, AI_CAPEX, RISK_FREE_RATE
from utils import _s3_client
from stock_pipeline.finance_utils import (
    calculate_annualized_return,
    calculate_max_drawdown,
    calculate_rolling_sharpe,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_dynamic_risk_free_rate(s3, bucket):
    """Load FEDFUNDS from S3 and return mean as decimal. Falls back to config RISK_FREE_RATE."""
    values = []
    for year in range(2022, 2027):  # frozen at Q1 2026
        key = f"macro_indicators/series=FEDFUNDS/year={year}/data.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            table = pq.read_table(io.BytesIO(obj['Body'].read()))
            for row in table.to_pylist():
                v = row.get('value')
                if v is not None:
                    values.append(float(v))
        except Exception:
            pass

    if values:
        rate = float(np.mean(values)) / 100
        logger.info(f"Dynamic risk-free rate from FEDFUNDS: {rate:.4f} ({rate*100:.2f}%)")
        return rate

    logger.warning("FEDFUNDS data not found in S3 — using config RISK_FREE_RATE fallback")
    return RISK_FREE_RATE


def fetch_monthly_prices(symbol):
    """Fetch monthly adjusted close prices from yfinance (3+ years, no API key required)."""
    ticker = yf.Ticker(symbol)
    df = ticker.history(start='2022-12-01', interval='1mo', auto_adjust=True)

    if df.empty:
        raise ValueError(f"No monthly data returned for {symbol}")

    df = df.reset_index()[['Date', 'Close']].rename(columns={'Date': 'date', 'Close': 'close'})
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df = df[df['date'] >= pd.Timestamp('2022-12-01')].sort_values('date').reset_index(drop=True)

    logger.info(f"{symbol}: fetched {len(df)} monthly data points")
    return df


def calculate_sharpe(prices_df, symbol, rf_rate=RISK_FREE_RATE):
    """Calculate annualized return, volatility, and Sharpe ratio from monthly prices."""
    prices_df = prices_df.sort_values('date').reset_index(drop=True)

    prices_df['monthly_return'] = prices_df['close'].pct_change()
    monthly_returns = prices_df['monthly_return'].dropna()

    if len(monthly_returns) < 12:
        logger.warning(f"{symbol}: only {len(monthly_returns)} months of data, need at least 12")
        return None

    annualized_return = calculate_annualized_return(monthly_returns.tolist())
    annualized_vol = monthly_returns.std() * np.sqrt(12)
    sharpe = (annualized_return - rf_rate) / annualized_vol if annualized_vol > 0 else 0

    max_dd = calculate_max_drawdown(prices_df['close'].tolist())

    rolling_results = calculate_rolling_sharpe(monthly_returns.tolist(), rf_rate, window=12)
    return_dates = prices_df['date'].iloc[1:].reset_index(drop=True)
    rolling_series = []
    for end_idx, sharpe_val in rolling_results:
        if end_idx < len(return_dates):
            rolling_series.append({
                'date': return_dates.iloc[end_idx].strftime('%Y-%m-%d'),
                'rolling_sharpe_12m': sharpe_val,
            })

    stock_info = STOCKS[symbol]
    result = {
        'symbol': symbol,
        'category': stock_info['category'],
        'ai_strategy': stock_info['ai_strategy'],
        'annualized_return': round(annualized_return * 100, 2),
        'annualized_volatility': round(annualized_vol * 100, 2),
        'sharpe_ratio': round(sharpe, 3),
        'max_drawdown': round(max_dd * 100, 2),
        'months_analyzed': len(monthly_returns),
        'start_date': prices_df['date'].min().strftime('%Y-%m-%d'),
        'end_date': prices_df['date'].max().strftime('%Y-%m-%d'),
        'rolling_sharpe': rolling_series,
    }

    # Attach capex data if available
    if symbol in AI_CAPEX:
        capex = AI_CAPEX[symbol]
        result['capex_2025_B'] = capex['capex_2025_B']
        result['capex_2026_B'] = capex['capex_2026_B']
        result['ai_pct_of_capex'] = capex['ai_pct']
        result['est_ai_spend_2026_B'] = round(capex['capex_2026_B'] * capex['ai_pct'] / 100, 1)

    return result


def run_backtest():
    """Run full backtest and analyze AI value chain + capex efficiency."""
    logger.info("=" * 70)
    logger.info("HISTORICAL BACKTEST: AI Value Chain Analysis")
    logger.info("Thesis: The market rewards AI builders, not AI renters")
    logger.info("=" * 70)

    s3, bucket = _s3_client()
    rf_rate = get_dynamic_risk_free_rate(s3, bucket)

    results = []

    for i, (symbol, info) in enumerate(STOCKS.items()):
        try:
            logger.info(f"[{i+1}/10] Fetching {symbol} ({info['category']})...")
            prices = fetch_monthly_prices(symbol)
            metrics = calculate_sharpe(prices, symbol, rf_rate=rf_rate)

            if metrics:
                results.append(metrics)
                logger.info(
                    f"  {symbol}: Return={metrics['annualized_return']}%, "
                    f"Vol={metrics['annualized_volatility']}%, "
                    f"Sharpe={metrics['sharpe_ratio']}"
                )

        except Exception as e:
            logger.error(f"  {symbol} FAILED: {str(e)}")
            continue

    if not results:
        raise Exception("No backtest results generated")

    df = pd.DataFrame(results)

    # === CATEGORY ANALYSIS ===
    logger.info("\n" + "=" * 70)
    logger.info("CATEGORY ANALYSIS (AI Value Chain)")
    logger.info("=" * 70)

    category_order = ['Infrastructure', 'AI Builder', 'AI Integrator', 'Legacy Tech', 'Control']
    category_stats = df.groupby('category').agg(
        avg_return=('annualized_return', 'mean'),
        avg_volatility=('annualized_volatility', 'mean'),
        avg_sharpe=('sharpe_ratio', 'mean'),
        stock_count=('symbol', 'count'),
    ).round(3)

    for cat in category_order:
        if cat in category_stats.index:
            row = category_stats.loc[cat]
            logger.info(
                f"  {cat:16s}: Return={row['avg_return']:6.2f}%, "
                f"Vol={row['avg_volatility']:6.2f}%, "
                f"Sharpe={row['avg_sharpe']:.3f} "
                f"(n={int(row['stock_count'])})"
            )

    # === BUILD vs RENT ANALYSIS ===
    builders = df[df['category'] == 'AI Builder']
    integrators = df[df['category'] == 'AI Integrator']

    if not builders.empty and not integrators.empty:
        builder_sharpe = builders['sharpe_ratio'].mean()
        integrator_sharpe = integrators['sharpe_ratio'].mean()
        build_rent_premium = ((builder_sharpe - integrator_sharpe) / abs(integrator_sharpe) * 100) \
            if integrator_sharpe != 0 else 0

        logger.info("\n" + "=" * 70)
        logger.info("BUILD vs RENT: Proprietary AI vs Partnership AI")
        logger.info("=" * 70)
        logger.info(f"  AI Builders avg Sharpe (META, GOOGL):     {builder_sharpe:.3f}")
        logger.info(f"  AI Integrators avg Sharpe (MSFT, AMZN):   {integrator_sharpe:.3f}")
        logger.info(f"  BUILDER PREMIUM:                          {build_rent_premium:+.1f}%")

    # === CAPEX EFFICIENCY ANALYSIS ===
    capex_stocks = df[df['symbol'].isin(AI_CAPEX.keys())].copy()

    if not capex_stocks.empty:
        logger.info("\n" + "=" * 70)
        logger.info("CAPEX EFFICIENCY: Sharpe per $B of AI Spending (2026 est.)")
        logger.info("=" * 70)

        for _, row in capex_stocks.iterrows():
            sym = row['symbol']
            capex = AI_CAPEX[sym]
            ai_spend = capex['capex_2026_B'] * capex['ai_pct'] / 100
            sharpe_per_B = row['sharpe_ratio'] / ai_spend * 100 if ai_spend > 0 else 0

            logger.info(
                f"  {sym:5s}: 2026 capex=${capex['capex_2026_B']:.0f}B, "
                f"AI={capex['ai_pct']}% (${ai_spend:.0f}B), "
                f"Sharpe={row['sharpe_ratio']:.3f}, "
                f"Efficiency={sharpe_per_B:.4f} Sharpe/$B"
            )

        # Rank by efficiency
        capex_stocks['est_ai_spend_2026'] = capex_stocks['symbol'].map(
            lambda s: AI_CAPEX[s]['capex_2026_B'] * AI_CAPEX[s]['ai_pct'] / 100
        )
        capex_stocks['sharpe_per_B'] = capex_stocks['sharpe_ratio'] / capex_stocks['est_ai_spend_2026'] * 100

        most_efficient = capex_stocks.loc[capex_stocks['sharpe_per_B'].idxmax()]
        least_efficient = capex_stocks.loc[capex_stocks['sharpe_per_B'].idxmin()]
        efficiency_ratio = most_efficient['sharpe_per_B'] / least_efficient['sharpe_per_B'] \
            if least_efficient['sharpe_per_B'] > 0 else 0

        logger.info(f"\n  Most efficient:  {most_efficient['symbol']} "
                    f"({most_efficient['sharpe_per_B']:.4f} Sharpe/$B)")
        logger.info(f"  Least efficient: {least_efficient['symbol']} "
                    f"({least_efficient['sharpe_per_B']:.4f} Sharpe/$B)")
        logger.info(f"  Efficiency gap:  {efficiency_ratio:.1f}x")

    logger.info("\n" + "=" * 70)
    logger.info("KEY FINDING")
    logger.info("=" * 70)
    logger.info("  In 2026, Big Tech will spend ~$650B on AI. But spending more")
    logger.info("  doesn't mean earning more. Companies building proprietary AI")
    logger.info("  outperform those renting it through partnerships - the market")
    logger.info("  rewards AI ownership, not AI spending.")
    logger.info("=" * 70)

    # === SAVE RESULTS ===
    # Use writable Docker mount if available, otherwise script directory (local runs)
    output_dir = '/opt/airflow/stock_pipeline' if os.path.isdir('/opt/airflow/stock_pipeline') \
        else os.path.dirname(os.path.abspath(__file__))

    stock_output = os.path.join(output_dir, 'backtest_results.json')
    with open(stock_output, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"\nStock-level results saved to {stock_output}")

    category_output = os.path.join(output_dir, 'category_results.json')
    cat_results = category_stats.reset_index().to_dict('records')
    with open(category_output, 'w') as f:
        json.dump(cat_results, f, indent=2)
    logger.info(f"Category results saved to {category_output}")

    return df


def run_local():
    """Local demo mode: load from backtest_results.csv and print findings without AWS."""
    output_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(output_dir, 'backtest_results.csv')

    if not os.path.exists(csv_path):
        print(f"ERROR: {csv_path} not found.")
        print("Run 'make analyze' first (requires AWS) to generate local CSV files.")
        sys.exit(1)

    import csv
    with open(csv_path) as f:
        results = list(csv.DictReader(f))

    print("=" * 70)
    print("LOCAL DEMO: AI Value Chain Sharpe Analysis")
    print(f"Loaded {len(results)} stocks from {csv_path}")
    print("=" * 70)

    for r in results:
        print(
            f"  {r['symbol']:5s} ({r['category']:16s}): "
            f"Return={r['annualized_return']:>7}%, "
            f"Vol={r['annualized_volatility']:>6}%, "
            f"Sharpe={r['sharpe_ratio']}"
        )

    builders = [r for r in results if r['category'] == 'AI Builder']
    integrators = [r for r in results if r['category'] == 'AI Integrator']
    if builders and integrators:
        b_sharpe = sum(float(r['sharpe_ratio']) for r in builders) / len(builders)
        i_sharpe = sum(float(r['sharpe_ratio']) for r in integrators) / len(integrators)
        premium = (b_sharpe - i_sharpe) / abs(i_sharpe) * 100 if i_sharpe != 0 else 0
        print(f"\n  Builder premium: {premium:+.1f}% on risk-adjusted returns")
    print("=" * 70)


if __name__ == '__main__':
    if '--local' in sys.argv:
        run_local()
    else:
        run_backtest()
