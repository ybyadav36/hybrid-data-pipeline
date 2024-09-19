from kafka import KafkaConsumer
import json
from datetime import datetime

# Kafka setup
KAFKA_TOPIC = "weather_reports"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def process_weather_data(weather_data):
    try:
        # Extract relevant fields from the weather data
        state = weather_data.get('state', 'Unknown')
        temperature = weather_data.get('temperature', 'N/A')
        humidity = weather_data.get('humidity', 'N/A')
        weather_desc = weather_data.get('weather', 'N/A')
        wind_speed = weather_data.get('wind_speed', 'N/A')
        timestamp = weather_data.get('datetime', 'Unknown')

        # Print the extracted weather data
        print(f"State: {state}")
        print(f"Temperature: {temperature:.2f}°C")
        print(f"Humidity: {humidity}%")
        print(f"Weather: {weather_desc}")
        print(f"Wind Speed: {wind_speed} m/s")
        print(f"Timestamp: {timestamp}")
        print("-------\n")

        # You can add additional logic for saving data to databases or real-time dashboards here.

    except Exception as e:
        print(f"Error processing weather data: {e}")

def main():
    print("Listening to Kafka topic for weather data...")
    for message in consumer:
        weather_data = message.value
        process_weather_data(weather_data)

if __name__ == "__main__":
    main()

