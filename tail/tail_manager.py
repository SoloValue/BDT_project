#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import pyspark
import yaml
from datetime import datetime
import time

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

  ## wait for message (consumer)
  BROKER_ADD = config["kafka"][PROJECT_ENV]["broker-1"]["address"]
  BROKER_PORT = config["kafka"][PROJECT_ENV]["broker-1"]["port"]
  TOPIC_CONSUMER = config["kafka"]["topics"]["api_sink-tail"]

  connected = False
  while not connected:
    try:
      consumer = KafkaConsumer(
          bootstrap_servers=[f'{BROKER_ADD}:{BROKER_PORT}'],
          value_deserializer = deserializer,
          auto_offset_reset='latest')
      connected = True
    except errors.NoBrokersAvailable:
      print("\tNO BROKER AVAILABLE!")
      print("\tRetring in 5 seconds...")
      time.sleep(5)
  print(f"\tConnected to {BROKER_ADD}:{BROKER_PORT}")

  consumer.subscribe(topics=TOPIC_CONSUMER)
  print(f'\tListening to topic: {TOPIC_CONSUMER}...')
  for message in consumer:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))
    
    if message.value["status"] != "GREAT":
      print(f"API sink gone wrong. Status: {message.value['status']}")
      pass

    print("\tAPI sink compleated. Starting spark...")

    ##do spark stuff

    ##read mongodb using spark

    ##ML project 2

    

