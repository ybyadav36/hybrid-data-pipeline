import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient

# Load environment variables from .env file
load_dotenv()

# Function to fetch daily cryptocurrency prices
def fetch_daily_data(crypto_ids, save_path="data/crypto/"):
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'usd'
    }

    response = requests.get("https://api.coingecko.com/api/v3/simple/price", params=params)
    
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(save_path, f"daily_prices_{datetime.now().strftime('%Y-%m-%d')}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Saved daily prices data for {crypto_ids}")
        
        # Upload to Azure Blob Storage
        connect_str = os.getenv("AZURITE_CONNECTION_STRING")
        container_name = "dailycryptodata"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        # Create container if it doesn't exist
        container_client = ContainerClient.from_connection_string(connect_str, container_name)
        try:
            container_client.create_container()
            print(f"Created new container: {container_name}")
        except Exception as e:
            print(f"Container '{container_name}' already exists or error creating: {e}")
        
        blob_client = container_client.get_blob_client(f"daily_prices_{datetime.now().strftime('%Y-%m-%d')}.json")
        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded daily_prices_{datetime.now().strftime('%Y-%m-%d')}.json to Azure Blob Storage.")
        
    else:
        print(f"Failed to fetch daily prices. Status Code: {response.status_code}")

# Test the script
if __name__ == "__main__":
    crypto_ids = ["bitcoin", "ethereum", "ripple", "litecoin", "bitcoin-cash", "cardano", "polkadot", "binancecoin", "chainlink", "stellar"]
    fetch_daily_data(crypto_ids)





