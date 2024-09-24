import os
def create_topic():
    """Create or update a Kafka topic."""
    topic = 'ekart_topic'  # desired topic name
    partitions = 3  # the desired number of partitions

    # Check if the topic already exists #sometimes it does not have  .sh 
    cmd = f"docker exec -it kafka kafka-topics.sh --describe --topic {topic} --bootstrap-server kafka:9092"
    topic_exists = os.system(cmd) == 0

    if topic_exists:
        print(f"Topic '{topic}' already exists. Updating partitions to {partitions}.")
        # Update the number of partitions for an existing topic
        cmd = f"docker exec -it kafka kafka-topics.sh --alter --topic {topic} --partitions {partitions} --bootstrap-server kafka:9092"
        os.system(cmd)
    else:
        # Create the topic with the specified number of partitions
        cmd = f"docker exec -it kafka kafka-topics.sh --create --topic {topic} --bootstrap-server kafka:9092 --partitions {partitions} --replication-factor 1"
        os.system(cmd)
        print(f"Topic '{topic}' created with {partitions} partitions.")

if __name__ == "__main__":
    create_topic()
