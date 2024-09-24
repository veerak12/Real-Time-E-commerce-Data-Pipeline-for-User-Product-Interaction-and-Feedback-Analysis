import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

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
logger.info("Spark session initialized")

# Set the log level for Spark to ERROR to suppress warnings
spark.sparkContext.setLogLevel("ERROR")

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
    StructField("created_at", TimestampType(), True),  # This will be mapped to registration_date
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
# Rename 'created_at' to 'registration_date'
final_df = parsed_df.select(
    col("data.user_id"),            # user_id
    col("data.first_name"),         # first_name
    col("data.last_name"),          # last_name
    col("data.created_at").alias("registration_date"), # Rename created_at to registration_date
    col("data.email"),              # email
    col("data.phone_number"),       # phone_number
    col("data.address"),            # address
    col("data.date_of_birth"),      # date_of_birth
    col("data.gender"),             # gender
    col("data.nationality"),        # nationality
    col("data.job_title"),          # job_title
    col("data.company_name"),       # company_name
    col("data.start_date"),         # start_date
    col("data.username"),           # username
    col("data.website_url"),        # website_url
    col("data.twitter_handle"),     # twitter_handle
    col("data.country"),            # country
    col("data.city"),               # city
    col("data.subscription_type"),  # subscription_type
    col("data.customer_status")     # customer_status
).distinct()

logger.info("Data selected")

# Function to write data to MySQL
def write_to_mysql(df, table_name):
    logger.info(f"Writing data to {table_name} table")
    
    # Log a sample of records to debug issues
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
        logger.info("Waiting for new messages...")
    else:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

        # Read existing users from MySQL
        users_df = spark.read \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "users") \
            .load()

        # Perform a left anti join to filter out existing users
        new_users_df = batch_df.join(users_df, on="user_id", how="left_anti")

        # Insert new users into the 'users' table
        if new_users_df.count() > 0:
            logger.info(f"Inserting {new_users_df.count()} new users")
            write_to_mysql(new_users_df, "users")
        else:
            logger.info("No new users to insert")

# Define the query and start streaming with a 20-second trigger interval
query = final_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='20 seconds') \
    .start()

logger.info("Streaming query started")

# Await termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Streaming query terminated by user")
finally:
    query.stop()
    logger.info("Streaming query stopped")
