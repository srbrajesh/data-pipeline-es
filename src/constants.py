import os 

KAFKA_HOST = os.getenv("KAFKA_HOST", "10.0.0.16:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "clickstream")