"""
Finance utility functions for the AI Sharpe analysis project.

Pure functions — no I/O, no AWS dependencies — so they are easy to test
and can be imported into any pipeline or notebook without side effects.
"""

import numpy as np


def calculate_annualized_return(monthly_returns):
    """
    Compound monthly returns into an annualized figure.

    Args:
        monthly_returns: array-like of monthly decimal returns (e.g. 0.05 = 5%)

    Returns:
        Annualized return as a decimal (e.g. 0.20 = 20%). None if < 12 months.
    """
    returns = np.array(monthly_returns, dtype=float)
    if len(returns) < 12:
        return None
    return (1 + returns.mean()) ** 12 - 1


def calculate_max_drawdown(prices):
    """
    Maximum peak-to-trough decline in a price series.

    Args:
        prices: array-like of prices in chronological order

    Returns:
        Max drawdown as a negative decimal (e.g. -0.35 = -35%).
        0.0 if prices never decline.
    """
    prices = np.array(prices, dtype=float)
    if len(prices) < 2:
        return 0.0
    peak = np.maximum.accumulate(prices)
    drawdowns = (prices - peak) / peak
    return float(drawdowns.min())


def calculate_beta(stock_returns, market_returns):
    """
    Market beta: sensitivity of stock returns to market returns.

    Beta > 1: more volatile than market
    Beta = 1: moves with market
    Beta < 1: less volatile (or inverse if negative)

    Args:
        stock_returns:  array-like of stock monthly returns
        market_returns: array-like of market (benchmark) monthly returns
                        must be the same length as stock_returns

    Returns:
        Beta as a float. None if fewer than 12 paired observations.
    """
    s = np.array(stock_returns, dtype=float)
    m = np.array(market_returns, dtype=float)
    if len(s) != len(m) or len(s) < 12:
        return None
    market_var = np.var(m, ddof=1)
    if market_var == 0:
        return None
    covariance = np.cov(s, m, ddof=1)[0, 1]
    return float(covariance / market_var)


def calculate_rolling_sharpe(monthly_returns, risk_free_rate, window=12):
    """
    Rolling annualized Sharpe ratio over a fixed window of months.

    Useful for answering: "Did the AI builder premium hold consistently,
    or was it driven by a single exceptional period?"

    Args:
        monthly_returns: array-like of monthly decimal returns
        risk_free_rate:  annualized risk-free rate (e.g. 0.043 = 4.3%)
        window:          rolling window in months (default 12)

    Returns:
        List of (window_end_index, sharpe) tuples. Empty if fewer observations
        than window size.
    """
    returns = np.array(monthly_returns, dtype=float)
    monthly_rf = risk_free_rate / 12

    results = []
    for end in range(window, len(returns) + 1):
        window_returns = returns[end - window:end]
        excess = window_returns - monthly_rf
        vol = np.std(excess, ddof=1)
        if vol == 0:
            sharpe = 0.0
        else:
            sharpe = float((np.mean(excess) / vol) * np.sqrt(12))
        results.append((end - 1, round(sharpe, 3)))

    return results
