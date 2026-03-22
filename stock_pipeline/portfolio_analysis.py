"""
Portfolio Analysis: Build vs Rent Deep Dive
Generates portfolio-level metrics and Power BI-ready visualizations
from historical backtest results.

No API calls needed - works entirely from backtest_results.json.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import csv
import logging
import numpy as np
from config import AI_CAPEX  # noqa: F401 — re-exported for tests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_results():
    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backtest_results.json')
    with open(results_path) as f:
        return json.load(f)


def build_vs_rent_analysis(results):
    """Compare AI Builders vs AI Integrators across multiple dimensions."""
    builders = [r for r in results if r['category'] == 'AI Builder']
    integrators = [r for r in results if r['category'] == 'AI Integrator']

    if not builders or not integrators:
        logger.error("Missing builder or integrator data")
        return

    logger.info("=" * 70)
    logger.info("BUILD vs RENT: Multi-Dimensional Comparison")
    logger.info("=" * 70)

    # Sharpe comparison
    b_sharpe = sum(r['sharpe_ratio'] for r in builders) / len(builders)
    i_sharpe = sum(r['sharpe_ratio'] for r in integrators) / len(integrators)
    premium = (b_sharpe - i_sharpe) / abs(i_sharpe) * 100

    logger.info("\n  RISK-ADJUSTED RETURNS (Sharpe Ratio)")
    logger.info(f"  Builders (META, GOOGL):     {b_sharpe:.3f}")
    logger.info(f"  Integrators (MSFT, AMZN):   {i_sharpe:.3f}")
    logger.info(f"  Builder premium:            {premium:+.1f}%")

    # Return comparison
    b_return = sum(r['annualized_return'] for r in builders) / len(builders)
    i_return = sum(r['annualized_return'] for r in integrators) / len(integrators)

    logger.info("\n  ABSOLUTE RETURNS")
    logger.info(f"  Builders:     {b_return:.1f}%")
    logger.info(f"  Integrators:  {i_return:.1f}%")
    logger.info(f"  Gap:          {b_return - i_return:+.1f} percentage points")

    # Volatility comparison
    b_vol = sum(r['annualized_volatility'] for r in builders) / len(builders)
    i_vol = sum(r['annualized_volatility'] for r in integrators) / len(integrators)

    logger.info("\n  RISK (Volatility)")
    logger.info(f"  Builders:     {b_vol:.1f}%")
    logger.info(f"  Integrators:  {i_vol:.1f}%")
    logger.info(
        f"  Builders carry {b_vol - i_vol:+.1f}pp more volatility"
        f" but compensate with {b_return - i_return:+.1f}pp more return"
    )

    return {
        'builder_sharpe': b_sharpe,
        'integrator_sharpe': i_sharpe,
        'premium_pct': round(premium, 1),
        'builder_return': b_return,
        'integrator_return': i_return,
        'builder_vol': b_vol,
        'integrator_vol': i_vol,
    }


def capex_efficiency_analysis(results):
    """Analyze AI spending efficiency: Sharpe per dollar of AI capex."""
    logger.info("\n" + "=" * 70)
    logger.info("CAPEX EFFICIENCY ANALYSIS")
    logger.info("=" * 70)

    rows = []
    for r in results:
        sym = r['symbol']
        if sym not in AI_CAPEX:
            continue

        capex = AI_CAPEX[sym]
        ai_spend_2026 = capex['capex_2026_B'] * capex['ai_pct'] / 100
        capex_to_revenue = capex['capex_2026_B'] / capex['revenue_2025_B'] * 100
        sharpe_per_B = r['sharpe_ratio'] / ai_spend_2026 * 100

        row = {
            'symbol': sym,
            'category': r['category'],
            'sharpe': r['sharpe_ratio'],
            'total_capex_2026': capex['capex_2026_B'],
            'ai_pct': capex['ai_pct'],
            'ai_spend_2026': round(ai_spend_2026, 1),
            'capex_to_revenue_pct': round(capex_to_revenue, 1),
            'sharpe_per_B': round(sharpe_per_B, 4),
        }
        rows.append(row)

        logger.info(
            f"  {sym:5s} ({r['category']:14s}): "
            f"Capex=${capex['capex_2026_B']:.0f}B, "
            f"AI={capex['ai_pct']}% (${ai_spend_2026:.0f}B), "
            f"Capex/Rev={capex_to_revenue:.0f}%, "
            f"Sharpe={r['sharpe_ratio']:.3f}, "
            f"Eff={sharpe_per_B:.4f}/B"
        )

    # Key insight: correlation between AI% and Sharpe
    ai_pcts = [row['ai_pct'] for row in rows]
    sharpes = [row['sharpe'] for row in rows]

    # Rank order for display
    ai_rank = sorted(range(len(ai_pcts)), key=lambda i: ai_pcts[i], reverse=True)
    sharpe_rank = sorted(range(len(sharpes)), key=lambda i: sharpes[i], reverse=True)

    logger.info(f"\n  Ranked by AI% of capex:  {' > '.join(rows[i]['symbol'] for i in ai_rank)}")
    logger.info(f"  Ranked by Sharpe ratio:  {' > '.join(rows[i]['symbol'] for i in sharpe_rank)}")

    # Spearman rank correlation: measures how consistently higher AI% -> higher Sharpe
    n = len(ai_pcts)
    rx = np.argsort(np.argsort(ai_pcts))
    ry = np.argsort(np.argsort(sharpes))
    rho = 1 - 6 * int(np.sum((rx - ry) ** 2)) / (n * (n ** 2 - 1)) if n > 1 else 0.0
    logger.info(f"  Spearman ρ (AI% → Sharpe): {rho:+.3f}"
                + (" — perfect positive correlation" if rho == 1.0 else ""))

    return rows


def rolling_sharpe_analysis(results):
    """Flatten per-stock rolling Sharpe series into rows for CSV export.

    Reads 'rolling_sharpe' from each result dict (list of {date, rolling_sharpe_12m}).
    Returns a flat list of dicts suitable for writing to rolling_sharpe.csv.
    """
    rows = []
    for r in results:
        for point in r.get('rolling_sharpe', []):
            rows.append({
                'symbol': r['symbol'],
                'category': r['category'],
                'date': point['date'],
                'rolling_sharpe_12m': point['rolling_sharpe_12m'],
            })
    return rows


def value_chain_summary(results):
    """Full value chain summary with all metrics."""
    logger.info("\n" + "=" * 70)
    logger.info("AI VALUE CHAIN SUMMARY")
    logger.info("=" * 70)

    categories = {}
    for r in results:
        cat = r['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(r)

    order = ['Infrastructure', 'AI Builder', 'AI Integrator', 'Control', 'Legacy Tech']
    summary_rows = []

    for cat in order:
        if cat not in categories:
            continue
        stocks = categories[cat]
        n = len(stocks)
        avg_ret = sum(s['annualized_return'] for s in stocks) / n
        avg_vol = sum(s['annualized_volatility'] for s in stocks) / n
        avg_sharpe = sum(s['sharpe_ratio'] for s in stocks) / n
        symbols = ', '.join(s['symbol'] for s in stocks)

        summary_rows.append({
            'rank': order.index(cat) + 1,
            'category': cat,
            'stocks': symbols,
            'avg_return': round(avg_ret, 2),
            'avg_volatility': round(avg_vol, 2),
            'avg_sharpe': round(avg_sharpe, 3),
            'stock_count': n,
        })

        logger.info(
            f"  {order.index(cat)+1}. {cat:16s} [{symbols:20s}]: "
            f"Sharpe={avg_sharpe:.3f}, Return={avg_ret:.1f}%, Vol={avg_vol:.1f}%"
        )

    return summary_rows


def save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows):
    """Save analysis as CSV files for Power BI."""
    output_dir = os.path.dirname(os.path.abspath(__file__))

    rank_map = {'Infrastructure': 1, 'AI Builder': 2, 'AI Integrator': 3, 'Control': 4, 'Legacy Tech': 5}
    bor_map = {'AI Builder': 'Build', 'AI Integrator': 'Rent'}

    # backtest_results.csv — stock-level detail
    br_path = os.path.join(output_dir, 'backtest_results.csv')
    br_fields = ['symbol', 'category', 'ai_strategy', 'annualized_return', 'annualized_volatility',
                 'sharpe_ratio', 'max_drawdown', 'months_analyzed', 'start_date', 'end_date',
                 'capex_2025_B', 'capex_2026_B', 'ai_pct_of_capex', 'est_ai_spend_2026_B']
    with open(br_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=br_fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    logger.info(f"Saved: {br_path}")

    # powerbi_master.csv — primary Power BI source
    pbi_path = os.path.join(output_dir, 'powerbi_master.csv')
    pbi_fields = ['symbol', 'category', 'ai_strategy', 'annualized_return', 'annualized_volatility',
                  'sharpe_ratio', 'max_drawdown', 'capex_2025_B', 'capex_2026_B', 'ai_pct_of_capex',
                  'est_ai_spend_2026_B', 'build_or_rent', 'value_chain_rank']
    with open(pbi_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=pbi_fields, extrasaction='ignore')
        writer.writeheader()
        for r in results:
            row = dict(r)
            row['build_or_rent'] = bor_map.get(r['category'], 'N/A')
            row['value_chain_rank'] = rank_map.get(r['category'], 99)
            writer.writerow(row)
    logger.info(f"Saved: {pbi_path}")

    # category_summary.csv — category-level aggregates
    cat_path = os.path.join(output_dir, 'category_summary.csv')
    with open(cat_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['category', 'avg_return', 'avg_volatility', 'avg_sharpe', 'stock_count'])
        writer.writeheader()
        for row in chain_summary:
            writer.writerow({
                'category': row['category'],
                'avg_return': row['avg_return'],
                'avg_volatility': row['avg_volatility'],
                'avg_sharpe': row['avg_sharpe'],
                'stock_count': row['stock_count'],
            })
    logger.info(f"Saved: {cat_path}")

    # Build vs Rent comparison
    bvr_path = os.path.join(output_dir, 'build_vs_rent.csv')
    with open(bvr_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'AI Builders', 'AI Integrators', 'premium'])
        writer.writerow([
            'Sharpe Ratio',
            build_rent['builder_sharpe'],
            build_rent['integrator_sharpe'],
            f"+{build_rent['premium_pct']}%",
        ])
        writer.writerow(['Annualized Return %', build_rent['builder_return'], build_rent['integrator_return'], ''])
        writer.writerow(['Volatility %', build_rent['builder_vol'], build_rent['integrator_vol'], ''])
    logger.info(f"\nSaved: {bvr_path}")

    # Capex efficiency
    capex_path = os.path.join(output_dir, 'capex_efficiency.csv')
    with open(capex_path, 'w', newline='') as f:
        fields = ['symbol', 'category', 'sharpe', 'total_capex_2026', 'ai_pct',
                  'ai_spend_2026', 'capex_to_revenue_pct', 'sharpe_per_B']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(capex_rows)
    logger.info(f"Saved: {capex_path}")

    # Value chain summary
    chain_path = os.path.join(output_dir, 'value_chain_summary.csv')
    with open(chain_path, 'w', newline='') as f:
        fields = ['rank', 'category', 'stocks', 'avg_return', 'avg_volatility', 'avg_sharpe', 'stock_count']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(chain_summary)
    logger.info(f"Saved: {chain_path}")

    # Rolling Sharpe time series
    if rolling_rows:
        rolling_path = os.path.join(output_dir, 'rolling_sharpe.csv')
        with open(rolling_path, 'w', newline='') as f:
            fields = ['symbol', 'category', 'date', 'rolling_sharpe_12m']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rolling_rows)
        logger.info(f"Saved: {rolling_path}")


def main():
    results = load_results()
    logger.info(f"Loaded {len(results)} stock results\n")

    build_rent = build_vs_rent_analysis(results)
    capex_rows = capex_efficiency_analysis(results)
    chain_summary = value_chain_summary(results)
    rolling_rows = rolling_sharpe_analysis(results)

    save_analysis(build_rent, capex_rows, chain_summary, results, rolling_rows)

    logger.info("\n" + "=" * 70)
    logger.info("HEADLINE: The market rewards AI builders, not AI renters.")
    logger.info(f"Builder premium: +{build_rent['premium_pct']}% on risk-adjusted returns.")
    logger.info("Combined 2026 AI capex: ~$650B across Big Tech.")
    logger.info("=" * 70)


def run_local():
    """Local demo mode: read from backtest_results.json and print findings without AWS."""
    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backtest_results.json')

    if not os.path.exists(results_path):
        print(f"ERROR: {results_path} not found.")
        print("Run 'make analyze' first (requires AWS) to generate local data files.")
        sys.exit(1)

    import json
    with open(results_path) as f:
        results = json.load(f)

    print(f"Loaded {len(results)} stock results from {results_path}\n")

    build_rent = build_vs_rent_analysis(results)
    if build_rent:
        print(f"\n  Builder premium: +{build_rent['premium_pct']}% on risk-adjusted returns")

    capex_rows = capex_efficiency_analysis(results)
    value_chain_summary(results)

    print("\n" + "=" * 70)
    print("HEADLINE: The market rewards AI builders, not AI renters.")
    if build_rent:
        print(f"Builder premium: +{build_rent['premium_pct']}% on risk-adjusted returns.")
    print("=" * 70)


if __name__ == '__main__':
    if '--local' in sys.argv:
        run_local()
    else:
        main()
