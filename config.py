"""Central configuration for data-engineering-portfolio.

All pipeline DAGs import from here after `make dags` copies this file
to the dags/ directory alongside the DAG files:

    from config import STOCK_SYMBOLS, S3_BUCKET, ...

Standalone scripts (historical_backtest.py, portfolio_analysis.py) add
the project root to sys.path before importing.
"""

# --- Stock pipeline ---

STOCK_SYMBOLS = ['NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'CRM', 'ORCL', 'ADBE', 'AAPL', 'TSLA']

STOCKS = {
    'NVDA':  {'category': 'Infrastructure', 'ai_strategy': 'Sells AI hardware'},
    'META':  {'category': 'AI Builder',     'ai_strategy': 'Proprietary (Llama, MTIA chips)'},
    'GOOGL': {'category': 'AI Builder',     'ai_strategy': 'Proprietary (Gemini, TPUs)'},
    'MSFT':  {'category': 'AI Integrator',  'ai_strategy': 'Partnership (OpenAI)'},
    'AMZN':  {'category': 'AI Integrator',  'ai_strategy': 'Partnership (Anthropic)'},
    'CRM':   {'category': 'Legacy Tech',    'ai_strategy': 'AI features (Einstein)'},
    'ORCL':  {'category': 'Legacy Tech',    'ai_strategy': 'Cloud/AI pivot'},
    'ADBE':  {'category': 'Legacy Tech',    'ai_strategy': 'AI-disrupted (Firefly)'},
    'AAPL':  {'category': 'Control',        'ai_strategy': 'Late mover (Apple Intelligence)'},
    'TSLA':  {'category': 'Control',        'ai_strategy': 'Autonomous driving / robotics'},
}

# AI capex data (2025 actual, 2026 guidance) — sources: CNBC, Bloomberg, earnings calls Feb 2026
# Only tracked for the 4 hybrid companies where Build vs Rent thesis applies
AI_CAPEX = {
    'META':  {
        'capex_2025_B': 72.2, 'capex_2026_B': 125.0, 'ai_pct': 95, 'revenue_2025_B': 188.0,
        'notes': 'No cloud/logistics — nearly all capex is proprietary AI',
    },
    'GOOGL': {
        'capex_2025_B': 75.0, 'capex_2026_B': 180.0, 'ai_pct': 80, 'revenue_2025_B': 350.0,
        'notes': 'Mostly proprietary (TPUs, Gemini) + some cloud infra',
    },
    'MSFT':  {
        'capex_2025_B': 80.0, 'capex_2026_B': 145.0, 'ai_pct': 60, 'revenue_2025_B': 262.0,
        'notes': 'Split: Azure general cloud + OpenAI partnership',
    },
    'AMZN':  {
        'capex_2025_B': 124.5, 'capex_2026_B': 200.0, 'ai_pct': 40, 'revenue_2025_B': 638.0,
        'notes': 'Split: AWS, warehouses, logistics, Anthropic partnership',
    },
}

RISK_FREE_RATE = 0.045  # ~4.5% avg 10-year Treasury 2023-2025

RATE_LIMIT_DELAY = 12  # seconds between Alpha Vantage API calls (free tier: 5/min)

# --- Crypto pipeline ---

CRYPTO_SYMBOLS = ['BTC-USD', 'ETH-USD', 'SOL-USD']

# --- Weather pipeline ---

WEATHER_CITY = 'Brooklyn'

# --- AWS / Storage ---

S3_BUCKET = 'ai-sharpe-analysis-eric'
