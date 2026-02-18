"""Shared pytest fixtures for the data engineering portfolio test suite."""

import sys
import json
import os
import pytest
import pandas as pd
import numpy as np
import boto3
from datetime import datetime
from unittest.mock import MagicMock
from moto import mock_aws

# ── Stub Airflow so pipeline DAG modules can be imported without Airflow installed ──
for _mod in [
    'airflow',
    'airflow.models',
    'airflow.operators',
    'airflow.operators.python',
]:
    sys.modules.setdefault(_mod, MagicMock())

# Wire the bare 'data_quality' import used by DAG files (in Airflow they live
# alongside data_quality.py in dags/; in tests we point to the real module).
import monitoring.data_quality as _dq
sys.modules.setdefault('data_quality', _dq)
# ─────────────────────────────────────────────────────────────────────────────


# ── S3 fixture ────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_client(monkeypatch):
    """Moto-backed S3 client with fake credentials and a pre-created test bucket.

    Any boto3 calls made inside the test (including those inside pipeline
    load_to_s3 functions) are intercepted by moto for the duration of the test.
    """
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('S3_BUCKET', 'test-bucket')
    with mock_aws():
        client = boto3.client('s3', region_name='us-east-1')
        client.create_bucket(Bucket='test-bucket')
        yield client


# ── Fixtures for historical_backtest / portfolio_analysis tests ───────────────

@pytest.fixture
def sample_monthly_prices():
    """36 months of synthetic price data with known statistical properties."""
    np.random.seed(42)
    dates = pd.date_range(start='2023-01-31', periods=37, freq='ME')
    # Start at $100, random walk with ~2% monthly return and ~8% monthly vol
    returns = np.random.normal(0.02, 0.08, 36)
    prices = [100.0]
    for r in returns:
        prices.append(prices[-1] * (1 + r))

    return pd.DataFrame({
        'date': dates,
        'close': prices,
    })


@pytest.fixture
def short_price_series():
    """Only 6 months of data - too short for reliable Sharpe calculation."""
    dates = pd.date_range(start='2025-06-30', periods=7, freq='ME')
    prices = [100, 105, 103, 110, 108, 115, 112]
    return pd.DataFrame({'date': dates, 'close': prices})


@pytest.fixture
def flat_price_series():
    """Zero-volatility price series (constant price)."""
    dates = pd.date_range(start='2023-01-31', periods=25, freq='ME')
    prices = [100.0] * 25
    return pd.DataFrame({'date': dates, 'close': prices})


@pytest.fixture
def sample_backtest_results():
    """Realistic backtest results matching the format produced by historical_backtest.py."""
    return [
        {'symbol': 'META', 'category': 'AI Builder', 'ai_strategy': 'Proprietary (Llama, MTIA chips)',
         'annualized_return': 45.2, 'annualized_volatility': 28.1, 'sharpe_ratio': 2.369,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
        {'symbol': 'GOOGL', 'category': 'AI Builder', 'ai_strategy': 'Proprietary (Gemini, TPUs)',
         'annualized_return': 38.5, 'annualized_volatility': 25.3, 'sharpe_ratio': 1.979,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
        {'symbol': 'MSFT', 'category': 'AI Integrator', 'ai_strategy': 'Partnership (OpenAI)',
         'annualized_return': 28.1, 'annualized_volatility': 22.5, 'sharpe_ratio': 1.512,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
        {'symbol': 'AMZN', 'category': 'AI Integrator', 'ai_strategy': 'Partnership (Anthropic)',
         'annualized_return': 25.8, 'annualized_volatility': 27.0, 'sharpe_ratio': 1.232,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
        {'symbol': 'NVDA', 'category': 'Infrastructure', 'ai_strategy': 'Sells AI hardware',
         'annualized_return': 95.0, 'annualized_volatility': 42.0, 'sharpe_ratio': 3.335,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
        {'symbol': 'CRM', 'category': 'Legacy Tech', 'ai_strategy': 'AI features (Einstein)',
         'annualized_return': 15.2, 'annualized_volatility': 30.1, 'sharpe_ratio': 0.512,
         'months_analyzed': 36, 'start_date': '2023-01-31', 'end_date': '2025-12-31'},
    ]


@pytest.fixture
def valid_weather_data():
    """Valid weather data that should pass all quality checks."""
    return {
        'temperature': 72.5,
        'humidity': 55,
        'description': 'Clear',
        'city': 'Brooklyn',
    }


@pytest.fixture
def valid_stock_data():
    """Valid stock data list that should pass all quality checks."""
    return [
        {'symbol': 'NVDA', 'price': 135.50, 'volume': 45000000, 'change_percent': 2.3},
        {'symbol': 'META', 'price': 620.00, 'volume': 18000000, 'change_percent': -0.8},
    ]
