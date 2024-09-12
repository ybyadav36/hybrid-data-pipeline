import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from io import BytesIO
from dotenv import load_dotenv
from azure.core.exceptions import ResourceNotFoundError

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='process_weather_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get Azurite BlobServiceClient
def get_blob_service_client():
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(connect_str)

# Function to ensure a container exists
def ensure_container_exists(container_client):
    try:
        container_client.get_container_properties()
        logging.info(f"Container '{container_client.container_name}' already exists.")
    except ResourceNotFoundError:
        container_client.create_container()
        logging.info(f"Container '{container_client.container_name}' created.")

# Function to download blob data from a container
def download_blob_data(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob().readall()
    return blob_data

# Function to upload processed data to Azurite
def upload_processed_data(container_client, blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    try:
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Processed data uploaded to {blob_name}")
    except Exception as e:
        logging.error(f"Failed to upload data: {e}")

# Function to process weather data with transformations
def process_weather_data(raw_df):
    # Convert the 'date' column to datetime format
    raw_df['date'] = pd.to_datetime(raw_df['date'], format='%Y-%m-%d')

    # Handle radiation (ALLSKY_SFC_SW_DWN)
    raw_df['ALLSKY_SFC_SW_DWN'] = raw_df['ALLSKY_SFC_SW_DWN'].apply(lambda x: max(x, 0))  # Replace negative values with 0

    # Handle temperature (T2M_MAX, T2M_MIN) outliers
    raw_df['T2M_MAX'] = raw_df['T2M_MAX'].apply(lambda x: x if -10 <= x <= 55 else None)  # Reasonable max temperature range
    raw_df['T2M_MIN'] = raw_df['T2M_MIN'].apply(lambda x: x if -20 <= x <= 40 else None)  # Reasonable min temperature range

    # Handle wind speed (WS2M) outliers
    raw_df['WS2M'] = raw_df['WS2M'].apply(lambda x: x if x <= 40 else None)  # Cap wind speed to 40 m/s

    # Fill missing values in temperature and wind speed with forward fill or 0 as an example
    raw_df.ffill(inplace=True)
    raw_df.fillna(0, inplace=True)

    return raw_df

# Main function to process historical weather data
def main():
    # Set container names
    raw_container_name = 'historicalweatherdata'
    processed_container_name = 'processedhistoricalweatherdata'
    
    # Initialize BlobServiceClient
    blob_service_client = get_blob_service_client()
    
    # Get container clients for raw and processed data
    raw_container_client = blob_service_client.get_container_client(raw_container_name)
    processed_container_client = blob_service_client.get_container_client(processed_container_name)
    
    # Ensure the processed container exists
    ensure_container_exists(processed_container_client)
    
    # List all blobs (CSV files) in the raw container
    blobs_list = raw_container_client.list_blobs()

    for blob in blobs_list:
        blob_name = blob.name
        logging.info(f"Processing blob: {blob_name}")

        try:
            # Download raw CSV data
            raw_data = download_blob_data(raw_container_client, blob_name)
            raw_df = pd.read_csv(BytesIO(raw_data))

            # Process the data
            processed_df = process_weather_data(raw_df)

            # Convert processed data to Parquet format
            parquet_buffer = BytesIO()
            processed_df.to_parquet(parquet_buffer, index=False)

            # Save processed data to the processed container
            processed_blob_name = f"{blob_name.split('.')[0]}.parquet"
            upload_processed_data(processed_container_client, processed_blob_name, parquet_buffer.getvalue())
            
            logging.info(f"Processed and saved blob: {processed_blob_name}")

        except Exception as e:
            logging.error(f"Failed to process blob '{blob_name}': {e}")

if __name__ == "__main__":
    main()
