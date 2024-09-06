# scripts/openweathermap_data_extraction.py
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the API key from environment variables
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Function to fetch real-time weather data for a specific city
def fetch_weather_data(city, save_path="data/weather/"):
    url = f"{BASE_URL}?q={city}&appid={API_KEY}"
    
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(save_path, f"{city}_weather.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Saved weather data for {city}")
    else:
        print(f"Failed to retrieve data for {city}. Status Code: {response.status_code}")

# Test the script
if __name__ == "__main__":
    city = "London"
    fetch_weather_data(city)
