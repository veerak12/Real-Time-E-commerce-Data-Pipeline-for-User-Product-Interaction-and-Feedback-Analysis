import os
import logging
import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_parquet(data, data_path):
    """ Save data to a Parquet file """
    # Convert the data to a DataFrame
    df = pd.DataFrame([data])
    
    # Generate a timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    file_path = os.path.join(data_path, f"{timestamp}.parquet")
    
    # Save the DataFrame to a Parquet file
    df.to_parquet(file_path, index=False)
    logger.info(f"Saved message to {file_path}")

def consume_and_save_messages(data_path, kafka_topic, kafka_bootstrap_servers, kafka_group_id):
    """ Consume messages from Kafka and save them to Parquet files """
    # Ensure the data path exists
    os.makedirs(data_path, exist_ok=True)

    # Kafka consumer configuration
    consumer_conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': kafka_group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create a Kafka Consumer instance
    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic])

    try:
        logger.info("Starting Kafka consumer...")
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second

            if msg is None:
                logger.info("Waiting for new messages...")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, continue consuming
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            # Decode message and parse as JSON
            try:
                message_value = msg.value().decode('utf-8')
                logger.info(f"Received message: {message_value}")
                parsed_data = json.loads(message_value)  # Assuming the message is in JSON format

                # Save the JSON data to a Parquet file
                save_parquet(parsed_data, data_path)

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

    finally:
        # Close down the consumer to commit final offsets.
        logger.info("Closing Kafka consumer...")
        consumer.close()

# Example usage
if __name__ == "__main__":
    # Define paths and configuration
    data_path = "./stream_data"  # Path where Parquet files will be saved
    kafka_topic = 'user-activity'
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_group_id = 'my-group'

    # Start the consumer
    consume_and_save_messages(data_path, kafka_topic, kafka_bootstrap_servers, kafka_group_id)
