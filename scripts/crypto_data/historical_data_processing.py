import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# Azure storage connection and container details
AZURITE_CONNECTION_STRING = os.getenv("AZURITE_CONNECTION_STRING")
RAW_CONTAINER = 'historicalcrypto'
PROCESSED_CONTAINER = 'processedcryptodata'

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)

# Function to download blob data from Azure Blob Storage
def download_blob(container_name, blob_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    return download_stream.content_as_bytes()

# Function to upload processed data back into Azurite
def upload_processed_data(container_name, file_name, data):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(data, overwrite=True)

# Function to ensure the container exists or create it if it doesn't
def ensure_container_exists(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    try:
        if not container_client.exists():
            container_client.create_container()
            print(f"Created container: {container_name}")
        else:
            print(f"Container '{container_name}' already exists.")
    except Exception as e:
        print(f"Error creating/checking container {container_name}: {e}")
        raise e

# Function to convert DataFrame to Parquet format
def dataframe_to_parquet(df):
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    return parquet_buffer

# Function to process historical data (add your transformations here)
def process_data(df):
    # Example transformation: Fill missing values with 0
    df.fillna(0, inplace=True)

    # Example transformation: Add new calculated column (like daily percentage change)
    df['daily_change_pct'] = df['Close'].pct_change().fillna(0) * 100

    return df

# Function to process and load historical data into a single Parquet file
def process_and_load_historical_data():
    # Ensure the container exists before proceeding
    ensure_container_exists(RAW_CONTAINER)
    ensure_container_exists(PROCESSED_CONTAINER)

    # List all blobs in the raw data container (historicalcrypto)
    container_client = blob_service_client.get_container_client(RAW_CONTAINER)
    blobs_list = container_client.list_blobs()

    combined_df = pd.DataFrame()  # DataFrame to hold all crypto data

    for blob in blobs_list:
        try:
            # Download the raw data file
            raw_data = download_blob(RAW_CONTAINER, blob.name)

            # Process JSON data
            if blob.name.endswith('.json'):
                raw_json = json.loads(raw_data.decode('utf-8'))
                
                # Extract data for each crypto symbol 
                for symbol, records in raw_json.items():
                    print(f"Processing data for {symbol}")  # Print minimal output for each symbol
                    
                    raw_df = pd.DataFrame(records)
                    
                    if not 'Close' in raw_df.columns:
                        print(f"Column 'Close' not found in data for {symbol}, skipping...")
                        continue

                    # Process the data (transformations)
                    processed_df = process_data(raw_df)

                    # Append processed data to the combined DataFrame
                    processed_df['symbol'] = symbol  # Add symbol column to differentiate data
                    combined_df = pd.concat([combined_df, processed_df], ignore_index=True)

            else:
                print(f"Unsupported file format for {blob.name}, skipping...")
                continue

        except Exception as e:
            print(f"Error processing {blob.name}: {e}")

    if not combined_df.empty:
        # Convert combined DataFrame to Parquet format
        parquet_data = dataframe_to_parquet(combined_df)

        # Define processed data file name
        processed_file_name = 'processed_crypto_data.parquet'

        # Upload the processed data to Azurite
        upload_processed_data(PROCESSED_CONTAINER, processed_file_name, parquet_data.getvalue())

        # Show a message confirming processing and saving
        print(f"Data has been processed and saved to the file: {processed_file_name}")
    else:
        print("No data processed.")

# Run the data processing pipeline
if __name__ == "__main__":
    process_and_load_historical_data()



