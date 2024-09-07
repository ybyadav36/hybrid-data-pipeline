from confluent_kafka import Producer
import requests
import json
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')

# Function to create a Kafka producer
def create_kafka_producer():
    return Producer({'bootstrap.servers': KAFKA_SERVER})

# Function to fetch live crypto prices
def fetch_crypto_data(crypto_ids):
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'usd'
    }

    response = requests.get("https://api.coingecko.com/api/v3/simple/price", params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {crypto_ids}. Status Code: {response.status_code}")
        return None

# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Main streaming function to fetch and send data to Kafka
def stream_crypto_prices(producer, crypto_ids, delay=15):
    while True:
        data = fetch_crypto_data(crypto_ids)
        if data:
            producer.produce(KAFKA_TOPIC, value=json.dumps(data), callback=delivery_report)
            producer.poll(1)  # Ensures messages are sent
            print(f"Sent data to Kafka: {data}")
        time.sleep(delay)

# Test the script
if __name__ == "__main__":
    crypto_ids = ["bitcoin", "ethereum", "dogecoin", "solana", "ripple", "cardano", "polkadot", "litecoin", "chainlink", "uniswap"]
    producer = create_kafka_producer()
    stream_crypto_prices(producer, crypto_ids)