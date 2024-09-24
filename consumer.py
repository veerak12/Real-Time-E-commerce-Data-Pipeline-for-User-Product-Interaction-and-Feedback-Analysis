from confluent_kafka import Consumer, KafkaError
import json
import os

KAFKA_TOPIC = 'user-activity'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'my-group'

# Folder to store the consumed data
DATA_FOLDER = './ekart-data'

# Ensure the folder exists
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# Confluent Kafka consumer configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start reading from the earliest message
}

consumer = Consumer(conf)

def consume_data():
    """Consume data from the Kafka topic and store it in a file."""
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition, nothing to do
                else:
                    print(f"Error: {msg.error()}")
                    continue

            # Message successfully received
            try:
                # Decode and load message as JSON
                data = json.loads(msg.value().decode('utf-8'))

                # Create a unique filename using the current timestamp
                filename = f"{DATA_FOLDER}/user_activity_{msg.timestamp()[1]}.json"
                
                # Write the data to a JSON file
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=4)
                
                print(f"Saved message to {filename}")

            except json.JSONDecodeError as e:
                print(f"Error decoding message: {str(e)}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # Cleanup consumer

if __name__ == "__main__":
    consume_data()
