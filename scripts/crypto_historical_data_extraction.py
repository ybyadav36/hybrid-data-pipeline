import yfinance as yf
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables from .env file
load_dotenv()

# Function to fetch historical cryptocurrency data
def fetch_historical_data(crypto_ids, start_date, end_date, save_path="data/crypto/"):
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    historical_data = {}
    for crypto_id in crypto_ids:
        try:
            # Download historical data
            data = yf.download(crypto_id + "-USD", start=start_date, end=end_date)

            # Convert DataFrame to dictionary with string dates
            historical_data[crypto_id] = data.reset_index().to_dict(orient="records")
        except Exception as e:
            print(f"Failed to fetch historical data for {crypto_id}: {e}")

    file_path = os.path.join(save_path, "historical_data.json")
    with open(file_path, 'w') as f:
        json.dump(historical_data, f, indent=4, default=str)  # Convert timestamps to strings

    print(f"Saved historical data for {crypto_ids}")

    # Upload to Azure Blob Storage
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    container_name = "historicalcrypto"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create container if it doesn't exist
    if not container_client.exists():
        container_client.create_container()
    
    blob_client = container_client.get_blob_client("historical_data.json")
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded historical_data.json to Azure Blob Storage.")

# Test the script
if __name__ == "__main__":
    crypto_ids = ["BTC", "ETH", "XRP", "LTC", "BCH", "ADA", "DOT", "BNB", "LINK", "XLM"]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365)  # Last 3 years
    fetch_historical_data(crypto_ids, start_date, end_date)








