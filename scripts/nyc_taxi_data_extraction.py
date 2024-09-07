import os
import requests
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='download.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to scrape URLs for different categories
def get_taxi_data_urls(page_url, year, category):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    urls = [link['href'] for link in links if f"{category}_tripdata_{year}" in link['href']]
    return urls

# Function to upload data directly to Azurite Blob Storage from memory
def upload_to_azurite(container_client, blob_name, data):
    # Retry upload in case of failure
    retries = 3
    for attempt in range(retries):
        try:
            container_client.upload_blob(name=blob_name, data=data)
            logging.info(f"Uploaded {blob_name} to Azurite Blob Storage.")
            return True
        except Exception as e:
            logging.error(f"Error uploading {blob_name}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying upload of {blob_name} (Attempt {attempt + 1}/{retries})")
            else:
                logging.error(f"Failed to upload {blob_name} after {retries} attempts.")
    return False

# Function to download Parquet data and directly upload to Azurite
def download_parquet_file(url, container_client, progress_bar, retries=3, timeout=60):
    base_filename = url.split('/')[-1].replace(".parquet", "")
    
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    for attempt in range(retries):
        try:
            logging.info(f"Starting download attempt {attempt + 1}/{retries} for {base_filename}")
            with http.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()

                # Upload directly to Azurite
                total_size = int(r.headers.get('content-length', 0))
                block_size = 1024 * 1024
                blob_name = f"{base_filename}.parquet"

                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {base_filename}", leave=False) as t:
                    buffer = BytesIO()
                    for data in r.iter_content(block_size):
                        t.update(len(data))
                        buffer.write(data)

                # Seek to the start of the buffer before uploading
                buffer.seek(0)
                upload_successful = upload_to_azurite(container_client, blob_name, buffer)

                if upload_successful:
                    logging.info(f"Downloaded {base_filename} as Parquet and uploaded directly to Azurite")
                    tqdm.write(f"Downloaded {base_filename} and uploaded directly to Azurite")
                    progress_bar.update(1)

                    return base_filename
                else:
                    logging.error(f"Upload failed for {base_filename}")

        except requests.RequestException as e:
            logging.error(f"Error downloading {base_filename}: {e}")
            tqdm.write(f"Error downloading {base_filename}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying... (Attempt {attempt + 1}/{retries})")
                tqdm.write(f"Retrying... (Attempt {attempt + 1}/{retries})")
            else:
                logging.error(f"Failed to download {base_filename} after {retries} attempts.")
                tqdm.write(f"Failed to download {base_filename} after {retries} attempts.")
    return None

# Main function to download and store data directly to Azurite
def main():
    # Load Azurite connection string from .env file
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    container_name = "taxirawdata"
    
    # Create blob service client and container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure container exists, check before creating
    if not container_client.exists():
        try:
            container_client.create_container()
        except Exception as e:
            logging.warning(f"Failed to create container {container_name}: {e}")

    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    year = "2019"
    categories = ["yellow", "green"]

    print("Extracting URLs...")
    all_urls = []
    for category in categories:
        urls = get_taxi_data_urls(page_url, year, category)
        all_urls.extend(urls)

    print(f"Found {len(all_urls)} files to download across all categories.")

    # Create a progress bar for tracking the download process
    progress_bar = tqdm(total=len(all_urls), desc="Total Progress", unit="file")

    # Create ThreadPoolExecutor and download concurrently
    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_parquet_file, url, container_client, progress_bar): url for url in all_urls}
        num_downloaded_files = 0
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                num_downloaded_files += 1

    # Check if all files were downloaded
    if num_downloaded_files == len(all_urls):
        print(f"Download completed for all {len(all_urls)} files.")
    else:
        print(f"Warning: Downloaded {num_downloaded_files} files, but expected {len(all_urls)}.")

    progress_bar.close()

if __name__ == "__main__":
    main()


