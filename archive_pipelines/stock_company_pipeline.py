import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def crypto_pipeline():
    logging.info("Fetching crypto prices...")

    # 1. EXTRACT - Prices
    symbols = ['bitcoin', 'ethereum', 'solana']

    price_data = []
    for symbol in symbols:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd&include_market_cap=true"
        response = requests.get(url)
        data = response.json()

        price_data.append({
            'symbol': symbol,
            'price_usd': data[symbol]['usd'],
            'market_cap': data[symbol]['usd_market_cap']
        })

    logging.info(f"Fetched {len(price_data)} crypto prices")

    # 2. EXTRACT - Coin Info
    coin_data = []
    for symbol in symbols:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol}"
        response = requests.get(url)
        data = response.json()

        coin_data.append({
            'symbol': symbol,
            'name': data['name'],
            'category': data['categories'][0] if data['categories'] else 'Unknown'
        })

    logging.info(f"Fetched {len(coin_data)} coin details")

    # 3. TRANSFORM
    df_prices = pd.DataFrame(price_data)
    df_coins = pd.DataFrame(coin_data)
    df = pd.merge(df_prices, df_coins, on='symbol')
    print(df)

    # 4. LOAD
    engine = create_engine('postgresql://postgres:3982@localhost:5432/crypto_db')
    df.to_sql('crypto_details', engine, if_exists='append', index=False)

    logging.info("Complete")

if __name__ == "__main__":
    crypto_pipeline()
