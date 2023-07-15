import time
import logging
import os
import json
import grpc
import location_pb2
import location_pb2_grpc
from concurrent import futures
from kafka import KafkaProducer
from producer import publish_location

kafka_url = os.environ["KAFKA_URL"]
kafka_topic = os.environ["KAFKA_TOPIC"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("udaconnect-location-event-service")

logger.info('connecting to Kafka %s', kafka_url)
logger.info('connecting to Kafka topic %s', kafka_topic)
producer = KafkaProducer(bootstrap_servers=[kafka_url])

class LocationServicer(location_pb2_grpc.locationServiceServicer):
    def Create(self, request, context):
        logger.info("Received location message!")

        request_value = {
            "userId": int(request.userId),
            "latitude": int(request.latitude),
            "longitude": int(request.longitude)
        }
        logger.info(request_value)
        user_encode_data = json.dumps(request_value, indent=2).encode('utf-8')
        producer.send(kafka_topic, user_encode_data)
        publish_location(request_value)
        return location_pb2.LocationMessage(**request_value)

# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_locationServiceServicer_to_server(LocationServicer(), server)

logger.info("Server starting on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()

try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)





























