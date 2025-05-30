import os
import json
import time
import requests
import logging # Import the logging module
from kafka import KafkaProducer

# --- Configure Logging ---
# Create a logger instance
logger = logging.getLogger(__name__) # Get a logger for this module
logger.setLevel(logging.INFO)      # Set the default logging level for this logger

# Create a console handler and set its level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the console handler
# Example format: [TIMESTAMP] [LEVEL] [MODULE.FUNCTION] MESSAGE
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

# --- Configuration (using logger instead of print) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default_topic')
GNEWS_API_KEY = os.getenv('GNEWS_API_KEY') # Loaded from docker-compose.yml

GNEWS_API_URL = "https://gnews.io/api/v4/top-headlines"
SEARCH_QUERY = "technology"

logger.info("Starting GNews Kafka Producer...")
logger.info(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"Sending to topic: {KAFKA_TOPIC}")

# --- Initialize Kafka Producer ---
producer = None # Initialize producer outside try-except
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 2)
    )
    logger.info("Kafka producer initialized successfully.")
except Exception as e:
    logger.error(f"Could not connect to Kafka: {e}", exc_info=True) # exc_info=True prints traceback
    # Depending on your app, you might want to retry or exit
    exit(1)

# --- Function to fetch news and send to Kafka ---
def fetch_and_send_news():
    if not GNEWS_API_KEY:
        logger.error("GNEWS_API_KEY not set. Cannot fetch news.")
        return

    logger.info(f"Fetching top headlines for query: '{SEARCH_QUERY}'...")
    try:
        params = {
            'token': GNEWS_API_KEY,
            'lang': 'en',
            'q': SEARCH_QUERY,
            'max': 10
        }
        response = requests.get(GNEWS_API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        articles = data.get('articles', [])
        logger.info(f"Fetched {len(articles)} articles.")

        if not articles:
            logger.info("No new articles to send.")
            return

        for article in articles:
            message = {
                'title': article.get('title'),
                'description': article.get('description'),
                'url': article.get('url'),
                'publishedAt': article.get('publishedAt')
            }
            # Send the message
            # For debugging, you might use logger.debug to avoid too much output
            logger.debug(f"Attempting to send message for article: {message.get('title')[:50]}...")
            producer.send(KAFKA_TOPIC, value=message)


        # --- IMPORTANT: Flush the producer to ensure messages are sent ---
        producer.flush()
        logger.info("All messages flushed to Kafka.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching news from GNews API: {e}", exc_info=True)
    except json.JSONDecodeError:
        logger.error("Could not decode JSON response from GNews API.", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)

# --- Main execution logic ---
if __name__ == "__main__":
    fetch_and_send_news()
    logger.info("GNews service finished its task.")

    # If you wanted this to run continuously, you'd wrap fetch_and_send_news() in a loop:
    # while True:
    #     fetch_and_send_news()
    #     logger.info("Sleeping for 60 seconds...")
    #     time.sleep(60)