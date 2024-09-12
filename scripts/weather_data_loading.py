import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='load_weather_data_to_postgres.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get Azurite BlobServiceClient
def get_blob_service_client():
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(connect_str)

# Function to download blob data from a container
def download_blob_data(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob().readall()
    return blob_data

# Function to get PostgreSQL connection string
def get_postgres_connection_string():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_WEATHER_DB")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

# Function to create PostgreSQL table if not exists
def create_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS historical_weather_data (
        index BIGINT PRIMARY KEY,
        ALLSKY_SFC_SW_DWN FLOAT,
        T2M_MAX FLOAT,
        T2M_MIN FLOAT,
        WS2M FLOAT,
        date DATE
    );
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
    logging.info("Table 'historical_weather_data' created or already exists.")

# Function to load data from Parquet files to PostgreSQL
def load_data_to_postgres(blob_service_client, container_name, postgres_engine):
    container_client = blob_service_client.get_container_client(container_name)
    blobs_list = container_client.list_blobs()
    
    for blob in blobs_list:
        blob_name = blob.name
        logging.info(f"Processing blob: {blob_name}")

        if blob_name.endswith('.parquet'):
            try:
                # Download Parquet data
                raw_data = download_blob_data(container_client, blob_name)
                parquet_data = BytesIO(raw_data)
                
                # Load the Parquet file into a DataFrame
                df = pd.read_parquet(parquet_data)
                
                # Insert data into PostgreSQL
                df.to_sql('historical_weather_data', postgres_engine, if_exists='append', index=False)
                
                logging.info(f"Blob '{blob_name}' loaded into PostgreSQL.")
                
            except Exception as e:
                logging.error(f"Failed to process blob '{blob_name}': {e}")

# Main function to load data
def main():
    container_name = 'processedhistoricalweatherdata'
    
    # Initialize BlobServiceClient
    blob_service_client = get_blob_service_client()
    
    # PostgreSQL connection
    postgres_engine = create_engine(get_postgres_connection_string())
    
    # Create table if not exists
    create_table(postgres_engine)
    
    # Load data from all Parquet files to PostgreSQL
    load_data_to_postgres(blob_service_client, container_name, postgres_engine)

if __name__ == "__main__":
    main()

