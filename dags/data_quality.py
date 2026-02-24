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

def log_data_stats(data: Any, data_type: str = "data") -> None:
    """Log summary statistics about the data"""
    if isinstance(data, list):
        logger.info(f"{data_type}: {len(data)} records")
    elif isinstance(data, dict):
        logger.info(f"{data_type}: {len(data)} fields")
    else:
        logger.info(f"{data_type}: {type(data)}")