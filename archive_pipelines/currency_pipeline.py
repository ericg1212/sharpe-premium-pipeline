import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def currency_pipeline():
    # 1. EXTRACT
    logging.info("Fetching exchange rates...")

    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    data = response.json()

    # Extract rates dictionary
    rates = data['rates']
    timestamp = datetime.now()

    # Convert to list of dictionaries
    all_data = []
    for currency, rate in rates.items():
        rate_record = {
            'currency': currency,
            'rate': float(rate),
            'timestamp': timestamp
        }
        all_data.append(rate_record)

    logging.info(f"Extracted {len(all_data)} currency rates")

    # 2. TRANSFORM
    df = pd.DataFrame(all_data)
    logging.info(f"Created DataFrame with {len(df)} rows")
    print(df.head())

    # Validate: rates must be > 0
    if (df['rate'] <= 0).any():
        logging.error("Invalid rates found (<=0)")
    else:
        logging.info("Validation passed")

    # 3. LOAD - PostgreSQL
    engine = create_engine('postgresql://postgres:3982@localhost:5432/finance_db')
    df.to_sql('exchange_rates', engine, if_exists='append', index=False)
    logging.info("Loaded to PostgreSQL")

    logging.info("Pipeline complete")

if __name__ == "__main__":
    currency_pipeline()
