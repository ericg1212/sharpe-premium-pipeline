import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def news_pipeline():
    # 1. EXTRACT
    logging.info("Fetching news...")

    url = "https://newsapi.org/v2/top-headlines"
    params = {'country': 'us', 'apiKey': os.environ['NEWS_API_KEY']}

    response = requests.get(url, params=params)
    articles = response.json()['articles']

    all_data = []
    for article in articles:
        all_data.append({
            'title': article['title'],
            'source': article['source']['name'],
            'published_at': article['publishedAt'],
            'timestamp': datetime.now()
        })

    logging.info(f"Extracted {len(all_data)} articles")

    # 2. TRANSFORM
    df = pd.DataFrame(all_data)
    logging.info(f"Created DataFrame with {len(df)} rows")

    # 3. LOAD
    engine = create_engine(os.environ['NEWS_DB_URL'])
    df.to_sql('headlines', engine, if_exists='append', index=False)
    logging.info("Complete")

if __name__ == "__main__":
    news_pipeline()
