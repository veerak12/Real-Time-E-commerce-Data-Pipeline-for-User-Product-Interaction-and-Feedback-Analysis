import os
import uuid
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit  
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Initialize Spark session with JDBC and Kafka packages
spark = SparkSession.builder \
    .appName("KafkaConsumerToMySQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,mysql:mysql-connector-java:8.0.25") \
    .getOrCreate()
# .config("spark.jars", "C:\\spark\\spark-3.5.1-bin-hadoop3\\jars\\mysql-connector-j-8.0.33.jar") \
logger.info("Spark session initialized")

# Kafka topic and server details
KAFKA_TOPIC = 'user-activity'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# MySQL configuration
MYSQL_URL = "jdbc:mysql://mysql-veera-vkellaka-920a.f.aivencloud.com:25250/ekart?user=&password="

# Define the schema of the JSON data we expect from Kafka
schema = StructType([
    StructField("address", StringType(), True),
    StructField("annual_income", IntegerType(), True),
    StructField("bank_account_number", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("credit_card_expiry", StringType(), True),
    StructField("credit_card_number", StringType(), True),
    StructField("customer_review", StringType(), True),
    StructField("customer_status", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("email", StringType(), True),
    StructField("feedback", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("last_name", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("session_duration", IntegerType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("twitter_handle", StringType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("user_id", StringType(), True),  # Ensure we are using `user_id`
    StructField("user_rating", DoubleType(), True),
    StructField("username", StringType(), True),
    StructField("website_url", StringType(), True)
])

logger.info("Schema defined")

# Read data from Kafka as a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Data read from Kafka")

# The 'value' column contains the actual message in Kafka, as raw bytes
# Cast the 'value' to string and apply the JSON schema
parsed_df = df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema).alias("data"), col("timestamp"))

logger.info("Data parsed from JSON")

# Select individual fields from the parsed JSON data
final_df = parsed_df.select(
    col("data.address"),
    col("data.annual_income"),
    col("data.bank_account_number"),
    col("data.category_id"),
    col("data.city"),
    col("data.company_name"),
    col("data.country"),
    col("data.created_at"),
    col("data.credit_card_expiry"),
    col("data.credit_card_number"),
    col("data.customer_review"),
    col("data.customer_status"),
    col("data.date_of_birth"),
    col("data.email"),
    col("data.feedback"),
    col("data.first_name"),
    col("data.gender"),
    col("data.job_title"),
    col("data.last_login"),
    col("data.last_name"),
    col("data.latitude"),
    col("data.longitude"),
    col("data.nationality"),
    col("data.payment_method"),
    col("data.phone_number"),
    col("data.product_id"),
    col("data.session_duration"),
    col("data.start_date"),
    col("data.subscription_type"),
    col("data.timestamp"),
    col("data.transaction_amount"),
    col("data.transaction_date"),
    col("data.twitter_handle"),
    col("data.updated_at"),
    col("data.user_id"),  # Using user_id, not uuid
    col("data.user_rating"),
    col("data.username"),
    col("data.website_url")
)

logger.info("Data selected")

# Function to write data to MySQL
def write_to_mysql(df, table_name):
    logger.info(f"Writing data to {table_name} table")
    try:
        df.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()
        logger.info(f"Data successfully written to {table_name} table")
    except Exception as e:
        logger.error(f"Error writing data to {table_name} table: {e}")

# Function to process each batch
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info("Waiting for new messages...")
    else:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        # Insert new users into the 'users' table
        new_users_df = batch_df.select(
            col("user_id"),
            col("first_name"),
            col("last_name"),
            col("start_date"),
            col("email"),
            col("phone_number"),
            col("address"),
            col("date_of_birth"),
            col("gender"),
            col("nationality"),
            col("job_title"),
            col("company_name"),
            col("start_date"),
            col("username"),
            col("website_url"),
            col("twitter_handle"),
            col("country"),
            col("city"),
            col("subscription_type"),
            col("customer_status")
        ).distinct()  # Ensure only new users are added

        logger.info("Processing new users")
        write_to_mysql(new_users_df, "users")

        # Insert user product interactions into 'user_product_interactions' table
        user_product_interactions_df = batch_df.select(
            col("user_id"),  # Referencing user_id
            col("product_id"),
            col("category_id"),
            col("session_duration"),
            col("timestamp").alias("interaction_date")
        ).withColumn("interaction_id", F.lit(str(uuid.uuid4())))  # Generate unique interaction_id

        logger.info("Processing user product interactions")
        write_to_mysql(user_product_interactions_df, "user_product_interactions")

        # Insert user feedback into 'user_feedback' table
        user_feedback_df = batch_df.select(
            col("user_id"),  # Referencing user_id
            col("product_id"),
            col("feedback"),
            col("user_rating").alias("rating"),
            col("timestamp").alias("feedback_date")
        ).withColumn("feedback_id", F.lit(str(uuid.uuid4())))  # Generate unique feedback_id

        logger.info("Processing user feedback")
        write_to_mysql(user_feedback_df, "user_feedback")

# Start the streaming query
query = final_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

logger.info("Streaming query started")

# Await termination
query.awaitTermination()
logger.info("Streaming query terminated")
