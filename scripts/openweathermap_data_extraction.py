import os
import time
import requests
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Constants
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

# Kafka configuration
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
KAFKA_TOPIC = os.getenv("KAFKA_WEATHER_TOPIC")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch weather data function
def fetch_openweather_data(state, latitude, longitude):
    api_key = os.getenv("OPENWEATHERMAP_API_KEY")
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    params = {
        "lat": latitude,
        "lon": longitude,
        "appid": api_key,
        "units": "metric"  # Temperature in Celsius
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return {
            "datetime": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "state": state,
            "temperature": data.get("main", {}).get("temp", None),
            "humidity": data.get("main", {}).get("humidity", None),
            "weather": data.get("weather", [{}])[0].get("description", None),
            "wind_speed": data.get("wind", {}).get("speed", None)
        }
    except Exception as e:
        print(f"Error fetching data for {latitude}, {longitude}: {e}")
        return None

# Send data to Kafka topic
def send_data_to_kafka(data):
    try:
        producer.send(KAFKA_TOPIC, data)
        print(f"Data sent to Kafka topic {KAFKA_TOPIC}.")
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")

# Main function to fetch and send data at intervals
def fetch_and_send_weather_data():
    while True:
        for state, (latitude, longitude) in STATE_COORDINATES.items():
            print(f"Fetching weather data for {state}...")
            weather_data = fetch_openweather_data(state, latitude, longitude)
            if weather_data:
                print(f"Data fetched for {state}, sending data...")
                send_data_to_kafka(weather_data)
            else:
                print(f"No weather data available for {state}")
        
        print("Waiting for 5 minutes before fetching again...")
        time.sleep(300)  # Wait for 5 minutes

# Run the script
if __name__ == "__main__":
    fetch_and_send_weather_data()



