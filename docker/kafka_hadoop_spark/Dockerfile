FROM bde2020/spark-master:3.3.0-hadoop3.3

# Set the working directory
WORKDIR /app

# Copy the Python script into the container
COPY kafka_to_hdfs.py .

# Install any additional Python dependencies if necessary
RUN pip install confluent-kafka

# Command to run the script
CMD ["spark-submit", "kafka_to_hdfs.py"]