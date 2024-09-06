# scripts/coingecko_data_extraction.py
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to fetch cryptocurrency prices
def fetch_crypto_data(crypto_ids, save_path="data/crypto/"):
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'usd'
    }

    response = requests.get("https://api.coingecko.com/api/v3/simple/price", params=params)
    
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(save_path, "crypto_prices.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Saved crypto prices data for {crypto_ids}")
    else:
        print(f"Failed to fetch data for {crypto_ids}. Status Code: {response.status_code}")

# Test the script
if __name__ == "__main__":
    crypto_ids = ["bitcoin", "ethereum"]
    fetch_crypto_data(crypto_ids)
