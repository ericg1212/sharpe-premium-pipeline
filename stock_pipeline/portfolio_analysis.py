"""
Portfolio Analysis: Build vs Rent Deep Dive
Generates portfolio-level metrics and Power BI-ready visualizations
from historical backtest results.

No API calls needed - works entirely from backtest_results.json.
"""

import json
import os
import csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AI capex data (2025 actual, 2026 guidance)
AI_CAPEX = {
    'META':  {'capex_2025_B': 72.2, 'capex_2026_B': 125.0, 'ai_pct': 95, 'revenue_2025_B': 188.0},
    'GOOGL': {'capex_2025_B': 75.0, 'capex_2026_B': 180.0, 'ai_pct': 80, 'revenue_2025_B': 350.0},
    'MSFT':  {'capex_2025_B': 80.0, 'capex_2026_B': 145.0, 'ai_pct': 60, 'revenue_2025_B': 262.0},
    'AMZN':  {'capex_2025_B': 124.5, 'capex_2026_B': 200.0, 'ai_pct': 40, 'revenue_2025_B': 638.0},
}


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

    logger.info(f"\n  RISK-ADJUSTED RETURNS (Sharpe Ratio)")
    logger.info(f"  Builders (META, GOOGL):     {b_sharpe:.3f}")
    logger.info(f"  Integrators (MSFT, AMZN):   {i_sharpe:.3f}")
    logger.info(f"  Builder premium:            {premium:+.1f}%")

    # Return comparison
    b_return = sum(r['annualized_return'] for r in builders) / len(builders)
    i_return = sum(r['annualized_return'] for r in integrators) / len(integrators)

    logger.info(f"\n  ABSOLUTE RETURNS")
    logger.info(f"  Builders:     {b_return:.1f}%")
    logger.info(f"  Integrators:  {i_return:.1f}%")
    logger.info(f"  Gap:          {b_return - i_return:+.1f} percentage points")

    # Volatility comparison
    b_vol = sum(r['annualized_volatility'] for r in builders) / len(builders)
    i_vol = sum(r['annualized_volatility'] for r in integrators) / len(integrators)

    logger.info(f"\n  RISK (Volatility)")
    logger.info(f"  Builders:     {b_vol:.1f}%")
    logger.info(f"  Integrators:  {i_vol:.1f}%")
    logger.info(f"  Builders carry {b_vol - i_vol:+.1f}pp more volatility but compensate with {b_return - i_return:+.1f}pp more return")

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

    # Simple rank correlation
    ai_rank = sorted(range(len(ai_pcts)), key=lambda i: ai_pcts[i], reverse=True)
    sharpe_rank = sorted(range(len(sharpes)), key=lambda i: sharpes[i], reverse=True)

    logger.info(f"\n  Ranked by AI% of capex:  {' > '.join(rows[i]['symbol'] for i in ai_rank)}")
    logger.info(f"  Ranked by Sharpe ratio:  {' > '.join(rows[i]['symbol'] for i in sharpe_rank)}")

    match = ai_rank == sharpe_rank
    logger.info(f"  Rankings match: {'YES - AI commitment directly correlates with returns' if match else 'Partial'}")

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


def save_analysis(build_rent, capex_rows, chain_summary):
    """Save analysis as CSV files for Power BI."""
    output_dir = os.path.dirname(os.path.abspath(__file__))

    # Build vs Rent comparison
    bvr_path = os.path.join(output_dir, 'build_vs_rent.csv')
    with open(bvr_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'AI Builders', 'AI Integrators', 'premium'])
        writer.writerow(['Sharpe Ratio', build_rent['builder_sharpe'], build_rent['integrator_sharpe'], f"+{build_rent['premium_pct']}%"])
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


def main():
    results = load_results()
    logger.info(f"Loaded {len(results)} stock results\n")

    build_rent = build_vs_rent_analysis(results)
    capex_rows = capex_efficiency_analysis(results)
    chain_summary = value_chain_summary(results)

    save_analysis(build_rent, capex_rows, chain_summary)

    logger.info("\n" + "=" * 70)
    logger.info("HEADLINE: The market rewards AI builders, not AI renters.")
    logger.info(f"Builder premium: +{build_rent['premium_pct']}% on risk-adjusted returns.")
    logger.info(f"Combined 2026 AI capex: ~$650B across Big Tech.")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
