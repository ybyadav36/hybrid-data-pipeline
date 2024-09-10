import os
import logging
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql, extras
from tqdm import tqdm
from time import time

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='load_to_postgresql.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection parameters
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DATABASE = os.getenv("POSTGRES_DB")

# Function to connect to PostgreSQL
def connect_postgresql():
    try:
        connection = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE,
            connect_timeout=10  # Timeout for slow connections
        )
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        return None

# Function to create table if it doesn't exist
def create_table_if_not_exists(connection, table_name, create_table_sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"), [table_name])
        if cursor.fetchone()[0]:
            logging.info(f"Table {table_name} already exists.")
        else:
            cursor.execute(create_table_sql)
            logging.info(f"Table {table_name} created.")
        connection.commit()
    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {e}")
    finally:
        cursor.close()

# Function to bulk load data into PostgreSQL
def load_data_to_postgresql(df, table_name):
    connection = None
    
    try:
        # Convert DataFrame column names to lowercase to match PostgreSQL's case-insensitive column names
        df.columns = map(str.lower, df.columns)

        # Handle NaN values and convert data types to match PostgreSQL types
        df = df.fillna(value={'passenger_count': 0, 'ratecodeid': 0, 'trip_type': 0, 'payment_type': 0, 'congestion_surcharge': 0, 'airport_fee': 0})
        df['passenger_count'] = df['passenger_count'].astype(int)
        df['payment_type'] = df['payment_type'].astype(int)
        df['trip_type'] = df['trip_type'].astype(int)
        df['ratecodeid'] = df['ratecodeid'].astype(int)

        # Connect to PostgreSQL
        connection = connect_postgresql()
        if connection:
            cursor = connection.cursor()

            # Bulk insert data using psycopg2.extras.execute_batch for efficiency
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
            )
            
            # Log the start of the bulk insert
            start_time = time()
            extras.execute_batch(cursor, insert_query.as_string(connection), df.values)
            logging.info(f"Bulk inserted {len(df)} rows into {table_name} in {time() - start_time} seconds.")

            connection.commit()
        else:
            logging.error(f"Failed to connect to PostgreSQL.")
    
    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
    
    finally:
        if connection:
            connection.close()

# Main function to load processed data from Azurite to PostgreSQL
def main():
    # Load Azurite connection string from .env file
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    raw_container_name = "processedtaxidata"

    # Create blob service client and container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(raw_container_name)

    # List all blobs in the container
    blobs_list = list(container_client.list_blobs())

    logging.info("Loading data from Azurite to PostgreSQL...")

    # PostgreSQL table creation SQL
    yellow_table_create_sql = """
    CREATE TABLE IF NOT EXISTS yellow_taxi_data (
        vendorid INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance REAL,
        ratecodeid INT,
        store_and_fwd_flag CHAR(1),
        pulocationid INT,
        dolocationid INT,
        payment_type INT,
        fare_amount REAL,
        extra REAL,
        mta_tax REAL,
        tip_amount REAL,
        tolls_amount REAL,
        improvement_surcharge REAL,
        total_amount REAL,
        congestion_surcharge REAL,
        airport_fee REAL
    );
    """

    green_table_create_sql = """
    CREATE TABLE IF NOT EXISTS green_taxi_data (
        vendorid INT,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag CHAR(1),
        ratecodeid INT,
        pulocationid INT,
        dolocationid INT,
        passenger_count INT,
        trip_distance REAL,
        fare_amount REAL,
        extra REAL,
        mta_tax REAL,
        tip_amount REAL,
        tolls_amount REAL,
        ehail_fee REAL,
        improvement_surcharge REAL,
        total_amount REAL,
        payment_type INT,
        trip_type INT,
        congestion_surcharge REAL
    );
    """

    # Connect to PostgreSQL
    connection = connect_postgresql()
    if connection:
        # Create tables if not exists
        create_table_if_not_exists(connection, 'yellow_taxi_data', yellow_table_create_sql)
        create_table_if_not_exists(connection, 'green_taxi_data', green_table_create_sql)
        
        # Loop over blobs and load data based on file type
        for blob in tqdm(blobs_list, desc="Loading blobs", unit="blob"):
            blob_name = blob.name

            try:
                # Log the start of downloading the blob
                logging.info(f"Downloading blob {blob_name}")
                start_time = time()

                # Download blob data
                blob_client = container_client.get_blob_client(blob_name)
                blob_data = blob_client.download_blob()
                df = pd.read_parquet(BytesIO(blob_data.readall()))

                # Log the time taken to download and read the blob
                logging.info(f"Downloaded and read {blob_name} in {time() - start_time} seconds.")

                # Load and process data into PostgreSQL
                if 'yellow' in blob_name:
                    load_data_to_postgresql(df, 'yellow_taxi_data')
                elif 'green' in blob_name:
                    load_data_to_postgresql(df, 'green_taxi_data')

            except Exception as e:
                logging.error(f"Error processing blob {blob_name}: {e}")

    else:
        logging.error("Failed to connect to PostgreSQL.")

if __name__ == "__main__":
    main()


