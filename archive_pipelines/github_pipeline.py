import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def github_pipeline():
    # 1. EXTRACT
    logging.info("Fetching GitHub users...")  # ✅ Added logging

    users = ['torvalds', 'gvanrossum', 'octocat', 'defunkt', 'pjhyett']  # ✅ At correct level
    all_data = []  # ✅ Only declared once

    for user in users:  # ✅ Properly indented (4 spaces)
        url = f"https://api.github.com/users/{user}"  # ✅ Indented inside loop (8 spaces)
        response = requests.get(url)  # ✅ Indented inside loop
        data = response.json()  # ✅ Indented inside loop

        user_record = {  # ✅ Extract actual GitHub fields
            'username': data['login'],
            'name': data.get('name'),
            'followers': data['followers'],
            'public_repos': data['public_repos'],
            'created_at': data['created_at'],
            'timestamp': datetime.now()
        }
        all_data.append(user_record)  # ✅ Indented inside loop

    logging.info(f"Extracted {len(all_data)} users")  # ✅ Shows count

    # 2. TRANSFORM
    df = pd.DataFrame(all_data)  # ✅ Correct syntax - pass list of dicts
    print(df)  # ✅ Preview data

    # 3. LOAD
    engine = create_engine('postgresql://postgres:3982@localhost:5432/social_db')  # ✅ Correct DB
    df.to_sql('github_users', engine, if_exists='append', index=False)  # ✅ Correct table

    logging.info("Complete")

if __name__ == "__main__":
    github_pipeline()  # ✅ Correct function name
