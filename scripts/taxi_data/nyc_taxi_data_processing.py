import os
import logging
from tqdm import tqdm
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import pandas as pd
import re
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='download_and_process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch data from Azurite Blob Storage
def fetch_from_azurite(container_client, blob_name):
    try:
        logging.info(f"Fetching {blob_name} from Azurite Blob Storage")
        blob_client = container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        parquet_data = BytesIO(download_stream.readall())
        return parquet_data
    except Exception as e:
        logging.error(f"Failed to fetch {blob_name}: {e}")
        return None

# Data processing function to clean and process downloaded Parquet files
def process_parquet_data(parquet_data, blob_name, columns):
    try:
        parquet_df = pd.read_parquet(parquet_data)

        # Extract year and month from blob name
        match = re.search(r'(\d{4})-(\d{2})', blob_name)
        if not match:
            logging.error(f"Blob name {blob_name} doesn't contain valid date information")
            return None
        year, month = match.groups()
        
        logging.info(f"Processing {blob_name} - original shape: {parquet_df.shape}")

        # Ensure pickup and dropoff times are in datetime format
        for col in columns[1:3]:  # pickup and dropoff columns
            parquet_df[col] = pd.to_datetime(parquet_df[col], errors='coerce')

        # Filter out rows with unrealistic dates
        start_date = datetime(int(year), int(month), 1)
        end_date = datetime(int(year), int(month), 28 if int(month) == 2 else (30 if int(month) in [4, 6, 9, 11] else 31))
        parquet_df = parquet_df[
            (parquet_df[columns[1]] >= start_date) & 
            (parquet_df[columns[1]] <= end_date)
        ]

        # Convert passenger_count to integer and handle errors
        parquet_df[columns[3]] = pd.to_numeric(parquet_df[columns[3]], errors='coerce').fillna(0).astype(int)

        # Ensure passenger_count is non-negative
        parquet_df = parquet_df[parquet_df[columns[3]] >= 0]

        # Replace NaN or None values with zero for numeric columns before rounding
        float_columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        for col in float_columns:
            if col in parquet_df.columns:
                parquet_df[col] = pd.to_numeric(parquet_df[col], errors='coerce').fillna(0).astype(float)
                parquet_df[col] = parquet_df[col].round(2)  # Round to 2 decimal places

        logging.info(f"Processed {blob_name} - new shape: {parquet_df.shape}")

        return parquet_df

    except Exception as e:
        logging.error(f"Failed to process {blob_name}: {e}")
        return None

# Function to upload processed data to Azurite Blob Storage
def upload_to_azurite(processed_df, container_client, blob_name):
    try:
        logging.info(f"Uploading processed data to Azurite Blob Storage: {blob_name}")
        
        # Convert DataFrame to bytes
        output = BytesIO()
        processed_df.to_parquet(output)
        output.seek(0)

        # Upload data to Azurite
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(output, overwrite=True)
        logging.info(f"Successfully uploaded {blob_name} to Azurite")
    except Exception as e:
        logging.error(f"Failed to upload {blob_name} to Azurite: {e}")

# Main function to fetch data from Azurite, process it, and save in Azurite
def main():
    # Load Azurite connection string from .env file
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    raw_container_name = "taxirawdata"
    processed_container_name = "processedtaxidata"

    # Create blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Create container client for raw data
    raw_container_client = blob_service_client.get_container_client(raw_container_name)

    # Create or get container client for processed data
    try:
        processed_container_client = blob_service_client.create_container(processed_container_name)
        logging.info(f"Created container: {processed_container_name}")
    except Exception as e:
        if "ContainerAlreadyExists" in str(e):
            processed_container_client = blob_service_client.get_container_client(processed_container_name)
            logging.info(f"Container '{processed_container_name}' already exists.")
        else:
            logging.error(f"Failed to create or get container: {processed_container_name} - {e}")
            return

    # List all blobs in the raw data container
    blobs_list = list(raw_container_client.list_blobs())

    print("Fetching and processing data from Azurite...")

    # Create a progress bar for tracking the download process
    total_files = len(blobs_list)
    progress_bar = tqdm(total=total_files, desc="Total Progress", unit="file")

    # Define columns for yellow and green taxi datasets
    yellow_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
    green_columns = ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge']

    # Loop over blobs and process based on file type
    for blob in blobs_list:
        blob_name = blob.name
        columns = yellow_columns if 'yellow' in blob_name else green_columns

        # Fetch data from Azurite
        parquet_data = fetch_from_azurite(raw_container_client, blob_name)
        if parquet_data:
            # Process the fetched data
            processed_df = process_parquet_data(parquet_data, blob_name, columns)
            if processed_df is not None:
                # Upload the processed data back to Azurite
                processed_blob_name = f"processed_{blob_name}"
                upload_to_azurite(processed_df, processed_container_client, processed_blob_name)

        # Update the progress bar
        progress_bar.update(1)

    print(f"Processing completed for {total_files} files.")
    progress_bar.close()

if __name__ == "__main__":
    main()
