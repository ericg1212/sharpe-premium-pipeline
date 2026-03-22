import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Raised when data quality checks fail"""
    pass

def validate_weather_data(data: Dict[str, Any]) -> bool:
    """
    Validate weather data quality
    Returns True if valid, raises DataQualityError if invalid
    """
    errors = []

    # Check required fields
    required_fields = ['temperature', 'humidity', 'description']
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    # Validate temperature range
    temp = data.get('temperature', 0)
    if temp < -100 or temp > 150:
        errors.append(f"Temperature out of range: {temp}°F")

    # Validate humidity
    humidity = data.get('humidity', 0)
    if humidity < 0 or humidity > 100:
        errors.append(f"Humidity out of range: {humidity}%")

    # Validate city name exists
    if not data.get('city'):
        errors.append("Missing city name")

    if errors:
        error_msg = "; ".join(errors)
        logger.error(f"Weather data quality check failed: {error_msg}")
        raise DataQualityError(error_msg)

    logger.info("Weather data quality check passed")
    return True

def validate_stock_data(stocks: List[Dict[str, Any]]) -> bool:
    """
    Validate stock data quality
    Returns True if valid, raises DataQualityError if invalid
    """
    errors = []

    if not stocks:
        raise DataQualityError("No stock data to validate")

    for stock in stocks:
        symbol = stock.get('symbol', 'UNKNOWN')

        # Check required fields
        required_fields = ['symbol', 'price', 'volume', 'change_percent']
        for field in required_fields:
            if field not in stock:
                errors.append(f"{symbol}: Missing required field: {field}")

        # Validate price
        price = stock.get('price', 0)
        if price <= 0:
            errors.append(f"{symbol}: Invalid price: ${price}")

        # Validate volume
        volume = stock.get('volume', 0)
        if volume < 0:
            errors.append(f"{symbol}: Invalid volume: {volume}")

        # Validate change percent is reasonable
        change_pct = stock.get('change_percent', 0)
        if change_pct < -50 or change_pct > 50:
            errors.append(f"{symbol}: Suspicious change: {change_pct}%")

    if errors:
        error_msg = "; ".join(errors)
        logger.error(f"Stock data quality check failed: {error_msg}")
        raise DataQualityError(error_msg)

    logger.info(f"Stock data quality check passed for {len(stocks)} stocks")
    return True

FRED_RANGES = {
    'GS10':     (0, 15),
    'CPIAUCSL': (100, 400),
    'UNRATE':   (0, 20),
    'FEDFUNDS': (0, 25),
}


def validate_fred_data(df, series_id: str) -> dict:
    errors = []

    required_cols = {'date', 'value', 'series_id'}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")
        return {'valid': False, 'errors': errors, 'row_count': len(df)}

    if df['date'].isnull().any():
        errors.append("Null values in date column")
    if df['series_id'].isnull().any():
        errors.append("Null values in series_id column")

    if series_id in FRED_RANGES:
        lo, hi = FRED_RANGES[series_id]
        out_of_range = df[(df['value'] < lo) | (df['value'] > hi)]
        if not out_of_range.empty:
            errors.append(
                f"{series_id} value out of range [{lo}, {hi}]: "
                f"{len(out_of_range)} rows"
            )

    return {'valid': len(errors) == 0, 'errors': errors, 'row_count': len(df)}


def validate_edgar_data(df) -> dict:
    errors = []

    required_cols = {'symbol', 'year', 'capex', 'revenue'}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")
        return {'valid': False, 'errors': errors, 'row_count': len(df)}

    if df['symbol'].isnull().any():
        errors.append("Null values in symbol column")
    if df['year'].isnull().any():
        errors.append("Null values in year column")

    negative_capex = df[df['capex'] < 0]
    if not negative_capex.empty:
        errors.append(f"capex < 0: {len(negative_capex)} rows")

    non_positive_revenue = df[df['revenue'] <= 0]
    if not non_positive_revenue.empty:
        errors.append(f"revenue <= 0: {len(non_positive_revenue)} rows")

    capex_exceeds_revenue = df[df['capex'] >= df['revenue']]
    if not capex_exceeds_revenue.empty:
        errors.append(f"capex >= revenue: {len(capex_exceeds_revenue)} rows")

    return {'valid': len(errors) == 0, 'errors': errors, 'row_count': len(df)}


def log_data_stats(data: Any, data_type: str = "data") -> None:
    """Log summary statistics about the data"""
    if isinstance(data, list):
        logger.info(f"{data_type}: {len(data)} records")
    elif isinstance(data, dict):
        logger.info(f"{data_type}: {len(data)} fields")
    else:
        logger.info(f"{data_type}: {type(data)}")
