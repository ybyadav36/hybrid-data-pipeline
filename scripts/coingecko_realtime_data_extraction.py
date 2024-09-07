import requests
import json
import os
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Kafka Producer configuration
producer = Producer({'bootstrap.servers': KAFKA_SERVER})

def delivery_report(err, msg):
    """ Callback function called once the message is acknowledged by Kafka. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to fetch cryptocurrency prices
def fetch_crypto_data(crypto_ids):
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'usd'
    }

    response = requests.get("https://api.coingecko.com/api/v3/simple/price", params=params)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None

# Main function to stream live data to Kafka
def main():
    crypto_ids = ["bitcoin", "ethereum", "ripple", "litecoin", "cardano", "polkadot", "stellar", "chainlink", "dogecoin", "uniswap"]

    while True:
        data = fetch_crypto_data(crypto_ids)
        if data:
            message = json.dumps(data)
            producer.produce(KAFKA_TOPIC, message, callback=delivery_report)
            producer.flush()
            print(f"Published data to Kafka topic '{KAFKA_TOPIC}'")
        
        # Wait for 15 seconds before fetching the next data
        time.sleep(15)

if __name__ == "__main__":
    main()