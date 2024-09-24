import logging
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import pyspark.sql.functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark session with JDBC and Kafka packages
spark = SparkSession.builder \
    .appName("KafkaConsumerToMySQL_UserProductAndFeedback") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,mysql:mysql-connector-java:8.0.25") \
    .getOrCreate()

# Set the log level for Spark to ERROR to suppress warnings
spark.sparkContext.setLogLevel("ERROR")

logger.info("Spark session initialized for user product interactions and feedback")

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
    StructField("user_id", StringType(), True),
    StructField("user_rating", DoubleType(), True),
    StructField("username", StringType(), True),
    StructField("website_url", StringType(), True)
])

logger.info("Schema defined for user product interactions and feedback")

# Read data from Kafka as a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Data read from Kafka for user product interactions and feedback")

# The 'value' column contains the actual message in Kafka, as raw bytes
# Cast the 'value' to string and apply the JSON schema
parsed_df = df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema).alias("data"), col("timestamp"))

logger.info("Data parsed from JSON for user product interactions and feedback")

# Function to write data to MySQL
def write_to_mysql(df, table_name):
    logger.info(f"Writing data to {table_name} table")
    try:
        sample_data = df.limit(5).toPandas()  # Log a sample of records
        logger.info(f"Sample data to be written:\n{sample_data}")

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
        logger.info(f"Batch {batch_id} is empty. Waiting for new data...")
    else:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

        # Process user product interactions with unique interaction_id
        user_product_interactions_df = batch_df.select(
            col("data.user_id"),
            col("data.product_id"),
            col("data.category_id"),
            col("data.session_duration"),
            col("timestamp").alias("interaction_date")  # Rename 'timestamp' as 'interaction_date'
        ).withColumn("interaction_id", lit(str(uuid.uuid4())))  # Generate unique UUIDs for interaction_id

        # Handle potential duplicates
        user_product_interactions_df = user_product_interactions_df.dropDuplicates(["interaction_id"])

        if user_product_interactions_df.count() > 0:
            logger.info("Inserting user product interactions into MySQL")
            write_to_mysql(user_product_interactions_df, "user_product_interactions")

        # Process user feedback with unique feedback_id
        user_feedback_df = batch_df.select(
            col("data.user_id"),
            col("data.product_id"),
            col("data.feedback"),
            col("data.user_rating").alias("rating"),  # Rename user_rating as rating
            col("timestamp").alias("feedback_date")  # Rename 'timestamp' as 'feedback_date'
        ).withColumn("feedback_id", lit(str(uuid.uuid4())))  # Generate unique UUIDs for feedback_id

        # Handle potential duplicates
        user_feedback_df = user_feedback_df.dropDuplicates(["feedback_id"])

        if user_feedback_df.count() > 0:
            logger.info("Inserting user feedback into MySQL")
            write_to_mysql(user_feedback_df, "user_feedback")

# Define the query and start streaming with a 20-second trigger interval
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='20 seconds') \
    .start()

logger.info("Streaming query started for user product interactions and feedback")

# Await termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Streaming query terminated by user")
finally:
    query.stop()
    logger.info("Streaming query stopped for user product interactions and feedback")
