from kafka import KafkaProducer
import numpy as np
import yaml
from api_sink.serializers import serializer


CONFIG_PATH = "./config/config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
    print(f"\tConfiguration file loaded from: {CONFIG_PATH}")

PROJECT_ENV = config["project"]["environment"]
BROKER_ADD = config["kafka"][PROJECT_ENV]["broker-1"]["address"]
BROKER_PORT = config["kafka"][PROJECT_ENV]["broker-1"]["port"]
TOPIC_NAME = "data_status"

producer = KafkaProducer(
    bootstrap_servers = [f'{BROKER_ADD}:{BROKER_PORT}'],
    value_serializer = serializer
    )
    
if __name__ == "__main__":
    json_to_send = {
        "name": "Teo",
        "hope": "not really",
        "will it work": "plz I need it",
        "how much in a scale to 1 to 10": np.random.randint(1,12)
    }
    
    producer.send(TOPIC_NAME, value=json_to_send)
    print(json_to_send)
    producer.flush()