# scripts/nyc_taxi_data_extraction.py
import requests
import os

# Function to download NYC Taxi data for a specific month and year
def download_nyc_taxi_data(year, month, save_path="data/"):
    # NYC Open Data API URL template for taxi data
    url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{month:02d}.csv"

    # File name to save the dataset
    file_name = f"yellow_tripdata_{year}_{month:02d}.csv"
    file_path = os.path.join(save_path, file_name)

    # Check if data directory exists, if not, create it
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    # Download and save the data
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded data for {year}-{month:02d}")
    else:
        print(f"Failed to download data for {year}-{month:02d}. Status Code: {response.status_code}")

# Test the script
if __name__ == "__main__":
    year = 2019
    for month in range(1, 13):
        download_nyc_taxi_data(year, month)
