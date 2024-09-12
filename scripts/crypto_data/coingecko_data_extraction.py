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
        # Get the current year and month to form the filename
        current_year_month = datetime.now().strftime('%Y-%m')
        file_path = os.path.join(save_path, f"monthly_prices_{current_year_month}.json")
        
        # Load existing data if the file already exists, else create a new one
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                monthly_data = json.load(f)
        else:
            monthly_data = {}

        # Check if today's data is already saved
        today_date = datetime.now().strftime('%Y-%m-%d')
        if today_date in monthly_data:
            print(f"Data for {today_date} already exists. Exiting the script.")
            return
        
        # Add today's data to the monthly data
        daily_data = {
            "date": today_date,
            "prices": response.json()
        }
        monthly_data[today_date] = daily_data["prices"]

        # Save updated monthly data back to the file
        with open(file_path, 'w') as f:
            json.dump(monthly_data, f, indent=4)

        print(f"Saved daily prices for {crypto_ids} to monthly file for {current_year_month}")
        
        # Upload to Azure Blob Storage
        connect_str = os.getenv("AZURITE_CONNECTION_STRING")
        container_name = "monthlycryptodata"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Check if the container exists before creating it
        try:
            if not container_client.exists():
                container_client.create_container()
                print(f"Created new container: {container_name}")
            else:
                print(f"Container '{container_name}' already exists.")
        except Exception as e:
            print(f"Error checking/creating container: {e}")
            return
        
        # Upload the monthly file
        blob_client = container_client.get_blob_client(f"monthly_prices_{current_year_month}.json")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded monthly_prices_{current_year_month}.json to Azure Blob Storage.")
        
    else:
        print(f"Failed to fetch daily prices. Status Code: {response.status_code}")

# Test the script
if __name__ == "__main__":
    crypto_ids = ["bitcoin", "ethereum", "ripple", "litecoin", "bitcoin-cash", "cardano", "polkadot", "binancecoin", "chainlink", "stellar"]
    fetch_daily_data(crypto_ids)





