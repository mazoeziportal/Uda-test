import os
import json
import logging
from kafka import KafkaProducer

kafka_url = os.environ.get("KAFKA_URL", "localhost:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "default_topic")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("udaconnect-location-event-producer-service")

producer = KafkaProducer(bootstrap_servers=[kafka_url])

def publish_location(locations):
    logger.info(f"Data to be sent to Kafka: {locations}")

    encoded_data = json.dumps(locations).encode('utf-8')
    logger.info(f"Data to be sent to Kafka: {encoded_data}")
    producer.send(kafka_topic, encoded_data)
    producer.flush()

    logger.info(f"Published location data {locations} to Kafka successfully.")