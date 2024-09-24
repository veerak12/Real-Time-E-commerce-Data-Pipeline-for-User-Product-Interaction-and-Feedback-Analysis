# producer_confluent.py

import requests
from confluent_kafka import Producer
import json
import time

KAFKA_TOPIC = 'user-activity'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Confluent Kafka producer configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_user_activity():
    """Fetch user activity data from the API."""
    try:
        response = requests.get('http://localhost:5000/api/v1/user-activity')
        if response.status_code == 200:
            return response.json()  # the API returns as JSON
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data from API: {str(e)}")
    return None

def send_to_kafka(data):
    """Send the fetched data to Kafka."""
    try:
        producer.produce(KAFKA_TOPIC, key=None, value=json.dumps(data), callback=delivery_report)
        producer.poll(0)  # Trigger the delivery report callback
        print(f"Sent to Kafka: {data}")
    except Exception as e:
        print(f"Error sending data to Kafka: {str(e)}")

if __name__ == "__main__":
    while True:
        data = fetch_user_activity()
        if data:
            send_to_kafka(data)
        producer.flush()  # Ensure all the messages are delivered
        time.sleep(20)  # Poll the API every 20 seconds
