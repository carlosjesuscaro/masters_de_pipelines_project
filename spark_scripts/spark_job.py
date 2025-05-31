import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" # Docker Compose service name and port
KAFKA_INPUT_TOPIC = "api_events"
KAFKA_OUTPUT_TOPIC = "word_counts_output"

# Set up SparkSession
# The appName is useful for identifying your application in the Spark UI
# .config() is where you'd add any specific Spark configurations if needed
spark = SparkSession.builder \
    .appName("GNewsWordCountStreaming") \
    .getOrCreate()

# Set Spark logging level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
logger.warn("SparkSession and Logger initialized.")
logger.warn(f"Reading from Kafka topic: {KAFKA_INPUT_TOPIC}")
logger.warn(f"Writing to Kafka topic: {KAFKA_OUTPUT_TOPIC}")

# --- Define Schema for incoming Kafka JSON messages ---
# This schema should match the structure of the JSON messages produced by your gnews-service
# It's important to define this so Spark can correctly parse the 'value' field.
news_schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("publishedAt", StringType(), True) # Keeping as StringType for now, can convert to TimestampType if needed
])

# --- Read from Kafka ---
# `spark.readStream` creates a DataFrame representing the unbounded table of stream data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "earliest") # Start reading from the beginning of the topic
    .load()

logger.warn("Kafka stream loaded. Processing data...")

# --- Process the Kafka messages ---
# 1. Cast the 'value' column (which is binary) to a String
# 2. Parse the JSON string using the defined schema
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), news_schema).alias("news_article"))

# 3. Extract text content (e.g., from 'title' and 'description')
#    Combine title and description for word count, handle potential nulls
text_df = parsed_df.select(
    col("news_article.title"),
    col("news_article.description")
)

# 4. Clean and tokenise the text for word count
#    - Coalesce title and description to handle cases where one might be null
#    - Convert to lowercase
#    - Remove punctuation and split into words
words_df = text_df.select(
    explode(
        # Combine title and description, handle None by using empty string
        regexp_replace(
            lower(
                col("title")
                .cast(StringType()) # Ensure it's string, handles potential non-string types
                .cast("string") # Ensure it's string
                + " " +
                col("description")
                .cast(StringType()) # Ensure it's string, handles potential non-string types
                .cast("string") # Ensure it's string
            ),
            "[^a-z\\s]", # Regex: anything not a lowercase letter or space
            ""
        ).split("\\s+") # Split by one or more spaces
    ).alias("word")
).where(col("word") != "") # Filter out empty strings that might result from splitting

# 5. Perform word count
word_counts = words_df.groupBy("word").count()

logger.warn("Word count logic defined.")

# --- Write the results back to Kafka (or print to console for testing) ---

# For testing, you can write to console:
query = word_counts.writeStream \
    .outputMode("complete") # 'complete' mode needed for groupBy. You can also use 'update' with Kafka.
    .format("console") \
    .trigger(processingTime="5 seconds") # Process every 5 seconds
    .start()

# Alternatively, write to a Kafka topic
# Ensure your output topic (word_counts_output) exists in Kafka
# If you write to Kafka, the value needs to be binary
# output_query = word_counts.selectExpr("CAST(word AS STRING) AS key", "CAST(count AS STRING) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#     .option("topic", KAFKA_OUTPUT_TOPIC) \
#     .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") # Required for Kafka sink
#     .trigger(processingTime="5 seconds") \
#     .outputMode("update") # 'update' mode is suitable for streaming word counts
#     .start()
#
# output_query.awaitTermination()


logger.warn("Stream started. Waiting for termination (Ctrl+C to stop)...")
query.awaitTermination() # Keeps the application running until terminated
logger.warn("Stream terminated.")