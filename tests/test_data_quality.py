"""Tests for data quality validation functions in monitoring/data_quality.py."""

import pytest
import pandas as pd

from monitoring.data_quality import (
    validate_weather_data,
    validate_stock_data,
    validate_fred_data,
    validate_edgar_data,
    DataQualityError,
)


class TestValidateWeatherData:
    """Tests for validate_weather_data()."""

    def test_valid_data_passes(self, valid_weather_data):
        """Complete, valid weather data should return True."""
        assert validate_weather_data(valid_weather_data) is True

    def test_missing_temperature_raises_error(self):
        """Missing 'temperature' field should raise DataQualityError."""
        data = {'humidity': 50, 'description': 'Clear', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Missing required field: temperature"):
            validate_weather_data(data)

    def test_missing_humidity_raises_error(self):
        """Missing 'humidity' field should raise DataQualityError."""
        data = {'temperature': 72, 'description': 'Clear', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Missing required field: humidity"):
            validate_weather_data(data)

    def test_missing_description_raises_error(self):
        """Missing 'description' field should raise DataQualityError."""
        data = {'temperature': 72, 'humidity': 50, 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Missing required field: description"):
            validate_weather_data(data)

    def test_temperature_too_high(self):
        """Temperature above 150F should fail."""
        data = {'temperature': 200, 'humidity': 50, 'description': 'Hot', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Temperature out of range"):
            validate_weather_data(data)

    def test_temperature_too_low(self):
        """Temperature below -100F should fail."""
        data = {'temperature': -150, 'humidity': 50, 'description': 'Cold', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Temperature out of range"):
            validate_weather_data(data)

    def test_humidity_over_100(self):
        """Humidity above 100% should fail."""
        data = {'temperature': 72, 'humidity': 110, 'description': 'Humid', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Humidity out of range"):
            validate_weather_data(data)

    def test_humidity_negative(self):
        """Negative humidity should fail."""
        data = {'temperature': 72, 'humidity': -5, 'description': 'Dry', 'city': 'Brooklyn'}
        with pytest.raises(DataQualityError, match="Humidity out of range"):
            validate_weather_data(data)

    def test_missing_city(self):
        """Missing city should fail."""
        data = {'temperature': 72, 'humidity': 50, 'description': 'Clear'}
        with pytest.raises(DataQualityError, match="Missing city name"):
            validate_weather_data(data)

    def test_multiple_errors_reported(self):
        """Multiple validation failures should all appear in error message."""
        data = {'temperature': 200, 'humidity': -5, 'description': 'Bad'}
        with pytest.raises(DataQualityError) as exc_info:
            validate_weather_data(data)
        error_msg = str(exc_info.value)
        assert "Temperature out of range" in error_msg
        assert "Humidity out of range" in error_msg
        assert "Missing city name" in error_msg


class TestValidateStockData:
    """Tests for validate_stock_data()."""

    def test_valid_data_passes(self, valid_stock_data):
        """Well-formed stock data should return True."""
        assert validate_stock_data(valid_stock_data) is True

    def test_empty_list_raises_error(self):
        """Empty stock list should raise DataQualityError."""
        with pytest.raises(DataQualityError, match="No stock data to validate"):
            validate_stock_data([])

    def test_missing_required_field(self):
        """Stock missing 'price' should fail."""
        stocks = [{'symbol': 'NVDA', 'volume': 1000, 'change_percent': 1.0}]
        with pytest.raises(DataQualityError, match="Missing required field: price"):
            validate_stock_data(stocks)

    def test_negative_price_fails(self):
        """Negative stock price should fail validation."""
        stocks = [{'symbol': 'NVDA', 'price': -10.0, 'volume': 1000, 'change_percent': 0.5}]
        with pytest.raises(DataQualityError, match="Invalid price"):
            validate_stock_data(stocks)

    def test_zero_price_fails(self):
        """Zero stock price should fail validation."""
        stocks = [{'symbol': 'NVDA', 'price': 0, 'volume': 1000, 'change_percent': 0.5}]
        with pytest.raises(DataQualityError, match="Invalid price"):
            validate_stock_data(stocks)

    def test_negative_volume_fails(self):
        """Negative volume should fail validation."""
        stocks = [{'symbol': 'NVDA', 'price': 100.0, 'volume': -500, 'change_percent': 0.5}]
        with pytest.raises(DataQualityError, match="Invalid volume"):
            validate_stock_data(stocks)

    def test_suspicious_change_percent(self):
        """Change >50% should be flagged as suspicious."""
        stocks = [{'symbol': 'NVDA', 'price': 100.0, 'volume': 1000, 'change_percent': 75.0}]
        with pytest.raises(DataQualityError, match="Suspicious change"):
            validate_stock_data(stocks)

    def test_multiple_stocks_validated(self):
        """All stocks in the list are validated, not just the first."""
        stocks = [
            {'symbol': 'NVDA', 'price': 100.0, 'volume': 1000, 'change_percent': 0.5},
            {'symbol': 'BAD', 'price': -1.0, 'volume': 1000, 'change_percent': 0.5},
        ]
        with pytest.raises(DataQualityError, match="BAD: Invalid price"):
            validate_stock_data(stocks)


def _fred_df(series_id='GS10', value=4.5, date='2024-01-01', include_cols=True):
    data = {'date': [date], 'value': [value], 'series_id': [series_id]}
    df = pd.DataFrame(data)
    return df


class TestValidateFredData:

    def test_valid_gs10_passes(self):
        df = _fred_df('GS10', 4.5)
        result = validate_fred_data(df, 'GS10')
        assert result['valid'] is True

    def test_invalid_gs10_out_of_range(self):
        df = _fred_df('GS10', 20.0)
        result = validate_fred_data(df, 'GS10')
        assert result['valid'] is False
        assert any('GS10' in e for e in result['errors'])

    def test_missing_columns_returns_invalid(self):
        df = pd.DataFrame({'date': ['2024-01-01'], 'series_id': ['GS10']})
        result = validate_fred_data(df, 'GS10')
        assert result['valid'] is False

    def test_null_date_returns_invalid(self):
        df = pd.DataFrame({'date': [None], 'value': [4.5], 'series_id': ['GS10']})
        result = validate_fred_data(df, 'GS10')
        assert result['valid'] is False

    def test_valid_fedfunds_passes(self):
        df = _fred_df('FEDFUNDS', 5.33)
        result = validate_fred_data(df, 'FEDFUNDS')
        assert result['valid'] is True


def _edgar_df(symbol='META', year=2024, capex=50e9, revenue=200e9, include_revenue=True):
    data = {'symbol': [symbol], 'year': [year], 'capex': [capex]}
    if include_revenue:
        data['revenue'] = [revenue]
    return pd.DataFrame(data)


class TestValidateEdgarData:

    def test_valid_edgar_data_passes(self):
        df = _edgar_df()
        result = validate_edgar_data(df)
        assert result['valid'] is True

    def test_negative_capex_returns_invalid(self):
        df = _edgar_df(capex=-1.0)
        result = validate_edgar_data(df)
        assert result['valid'] is False

    def test_zero_revenue_returns_invalid(self):
        df = _edgar_df(revenue=0)
        result = validate_edgar_data(df)
        assert result['valid'] is False

    def test_capex_exceeds_revenue_returns_invalid(self):
        df = _edgar_df(capex=300e9, revenue=200e9)
        result = validate_edgar_data(df)
        assert result['valid'] is False

    def test_missing_columns_returns_invalid(self):
        df = pd.DataFrame({'symbol': ['META'], 'year': [2024], 'capex': [50e9]})
        result = validate_edgar_data(df)
        assert result['valid'] is False
