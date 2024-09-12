import os
import requests
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Loads environment variables from my .env file
load_dotenv()

# states and union teretories
STATE_COORDINATES = {
    'Andhra Pradesh': (15.9129, 79.7400),
    'Arunachal Pradesh': (28.2180, 94.7278),
    'Assam': (26.2006, 92.9376),
    'Bihar': (25.0961, 85.3131),
    'Chhattisgarh': (21.2787, 81.8661),
    'Goa': (15.2993, 74.1240),
    'Gujarat': (22.2587, 71.1924),
    'Haryana': (29.0588, 76.0856),
    'Himachal Pradesh': (31.1048, 77.1734),
    'Jharkhand': (23.6102, 85.2799),
    'Karnataka': (15.3173, 75.7139),
    'Kerala': (10.8505, 76.2711),
    'Madhya Pradesh': (22.9734, 78.6569),
    'Maharashtra': (19.7515, 75.7139),
    'Manipur': (24.6637, 93.9063),
    'Meghalaya': (25.4670, 91.3662),
    'Mizoram': (23.1645, 92.9376),
    'Nagaland': (26.1584, 94.5624),
    'Odisha': (20.9517, 85.0985),
    'Punjab': (31.1471, 75.3412),
    'Rajasthan': (27.0238, 74.2179),
    'Sikkim': (27.5330, 88.5122),
    'Tamil Nadu': (11.1271, 78.6569),
    'Telangana': (18.1124, 79.0193),
    'Tripura': (23.9408, 91.9882),
    'Uttar Pradesh': (26.8467, 80.9462),
    'Uttarakhand': (30.0668, 79.0193),
    'West Bengal': (22.9868, 87.8550),
    'Andaman and Nicobar Islands': (11.7401, 92.6586),
    'Chandigarh': (30.7333, 76.7794),
    'Dadra and Nagar Haveli': (20.1809, 73.0169),
    'Daman and Diu': (20.3974, 72.8328),
    'Lakshadweep': (10.5667, 72.6417),
    'Delhi': (28.7041, 77.1025),
    'Puducherry': (11.9416, 79.8083),
    'Ladakh': (34.1526, 77.5770),
    'Jammu and Kashmir': (33.7782, 76.5762)
}

YEARS = [2020,2021,2022,2023, 2024]  

# Function to fetch weather data from NASA POWER API
def fetch_nasa_power_data(latitude, longitude, years):
    data_frames = []

    for year in years:
        print(f"Fetching weather data for year {year} at ({latitude}, {longitude}) from NASA POWER...")

        # NASA POWER API URL
        url = (
            f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=ALLSKY_SFC_SW_DWN,T2M_MAX,T2M_MIN,WS2M"
            f"&community=AG&longitude={longitude}&latitude={latitude}&start={year}0101&end={year}1231&format=JSON"
        )

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            
            # Extract the "properties" which contains the useful data under "parameter"
            if "properties" in data and "parameter" in data["properties"]:
                parameter_data = data["properties"]["parameter"]

                # Transform data into a DataFrame (flattening the date-based structure)
                df = pd.DataFrame(parameter_data)
                df['date'] = pd.to_datetime(df.index)
                data_frames.append(df)
            else:
                print(f"No data found for year {year} at ({latitude}, {longitude}).")
        else:
            print(f"Failed to fetch data for year {year}, Status Code: {response.status_code}")

    # Check if data_frames is not empty before concatenating
    if data_frames:
        return pd.concat(data_frames)
    else:
        print("No data available to concatenate for the selected years.")
        return pd.DataFrame()  # Return an empty DataFrame if no data is available

# Function to fetch and store weather data in Azure Blob Storage
def fetch_and_store_weather_data():
    connect_str = os.getenv("AZURITE_CONNECTION_STRING")
    container_name = "historicalweatherdata"

    # Creates BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Creates container if it doesn't exist
    try:
        container_client.create_container()
        print(f"Container '{container_name}' created successfully.")
    except Exception as e:
        if "ContainerAlreadyExists" in str(e):
            print(f"Container '{container_name}' already exists.")
        else:
            print(f"Failed to create container: {e}")

    # Loop through states and fetch data
    for state, (latitude, longitude) in STATE_COORDINATES.items():
        print(f"Fetching weather data for {state}...")
        weather_data = fetch_nasa_power_data(latitude, longitude, YEARS)

        if not weather_data.empty:
            print(f"Data fetched for {state}, saving data...")

            # Convert DataFrame to CSV (in-memory operation)
            file_name = f"{state}_weather_data.csv"

            # Upload CSV to Azure Blob Storage directly from memory
            blob_client = container_client.get_blob_client(file_name)
            csv_data = weather_data.to_csv(index=False).encode('utf-8')  # Convert to bytes
            blob_client.upload_blob(csv_data, overwrite=True)

            print(f"Uploaded {file_name} to Azure Blob Storage in container {container_name}.")
        else:
            print(f"No weather data available for {state}")
# Runs the script
if __name__ == "__main__":
    fetch_and_store_weather_data()


