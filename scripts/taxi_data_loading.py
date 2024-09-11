import os
import logging
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values  # Import extras module for batch inserts
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='load_to_postgresql.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            dbname=PG_DATABASE
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

# Function to drop the yellow_taxi_data table
def drop_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
        connection.commit()
        logging.info(f"Table {table_name} dropped.")
    except Exception as e:
        logging.error(f"Failed to drop table {table_name}: {e}")
    finally:
        cursor.close()

# Function to check if data already exists
def check_if_data_exists(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(sql.SQL("SELECT COUNT(1) FROM {}").format(sql.Identifier(table_name)))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        logging.error(f"Failed to check data in table {table_name}: {e}")
        return False

# Function to load data into PostgreSQL in batches
def load_data_to_postgresql(df, table_name, batch_size=5000):
    connection = None
    try:
        # Convert DataFrame column names to lowercase to match PostgreSQL's case-insensitive column names
        df.columns = map(str.lower, df.columns)

        # Ensure the columns for float data types are explicitly cast to float64 in pandas
        float_columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                         'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        df[float_columns] = df[float_columns].astype('float64')

        # Connect to PostgreSQL
        connection = connect_postgresql()
        if connection:
            cursor = connection.cursor()

            # Insert data into PostgreSQL in batches
            for i in tqdm(range(0, len(df), batch_size), desc=f"Inserting data into {table_name}"):
                batch_df = df.iloc[i:i+batch_size]
                rows = [tuple(row) for row in batch_df.values]
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, batch_df.columns))
                )
                execute_values(cursor, insert_query, rows)
                connection.commit()

            logging.info(f"Data loaded into table {table_name}.")
        else:
            logging.error(f"Failed to connect to PostgreSQL.")
            exit(1)  # Exit the script with an error code

    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
        exit(1)  # Exit the script with an error code

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

    print("Loading data from Azurite to PostgreSQL...")

    # PostgreSQL table creation SQL for green taxi data only
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
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT
    );
    """

    # Connect to PostgreSQL
    connection = connect_postgresql()
    if connection:
        # Drop yellow_taxi_data table if it exists
        drop_table(connection, 'yellow_taxi_data')

        # Create green_taxi_data table if not exists
        create_table_if_not_exists(connection, 'green_taxi_data', green_table_create_sql)

        # Check if green taxi data is already loaded
        green_data_exists = check_if_data_exists(connection, 'green_taxi_data')

        # Loop over blobs and load data based on file type
        for blob in tqdm(blobs_list, desc="Loading blobs", unit="blob"):
            blob_name = blob.name

            # Download blob data
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob()
            df = pd.read_parquet(BytesIO(blob_data.readall()))

            # Log progress
            logging.info(f"Processing blob: {blob_name}")

            # Load green taxi data only if not already loaded
            if 'green' in blob_name and not green_data_exists:
                logging.info("Loading green taxi data...")
                load_data_to_postgresql(df, 'green_taxi_data')

        logging.info("All blobs processed.")
    else:
        logging.error("Failed to connect to PostgreSQL.")
        exit(1)  # Exit the script with an error code

if __name__ == "__main__":
    main()













