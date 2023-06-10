#LIBRARIES---------------------------
from kafka import KafkaConsumer
import json
import yaml

#CLASSESS----------------------------
from serializers import serializer, deserializer

#MAIN--------------------------------
if __name__ == "__main__":
  ##load config
  CONFIG_PATH = "./config/config.yaml"
  with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
    print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
    PROJECT_ENV = config["project"]["environment"]

  ## start listening
  BROKER_ADD = config["kafka"][PROJECT_ENV]["broker-1"]["address"]
  BROKER_PORT = config["kafka"][PROJECT_ENV]["broker-1"]["port"]
  TOPIC_NAME = config["kafka"]["topics"]["head-api_sink"]

  consumer = KafkaConsumer(
    bootstrap_servers=[f'{BROKER_ADD}:{BROKER_PORT}'],
    value_deserializer = deserializer,
    auto_offset_reset='latest'
  )
  print(f"\tConnected to {BROKER_ADD}:{BROKER_PORT}")

  consumer.subscribe(topics=TOPIC_NAME)
  print(f'\tListening to topic: {TOPIC_NAME}...')
  for message in consumer:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))