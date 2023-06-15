#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
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

  ## send start message (producer)
  BROKER_ADD = config["kafka"][PROJECT_ENV]["broker-1"]["address"]
  BROKER_PORT = config["kafka"][PROJECT_ENV]["broker-1"]["port"]
  TOPIC_PRODUCER = config["kafka"]["topics"]["head-api_sink"]
  TOPIC_CONSUMER = config["kafka"]["topics"]["api_sink-tail"]

  connected = False
  while not connected:
    try:
      producer = KafkaProducer(
          bootstrap_servers = [f'{BROKER_ADD}:{BROKER_PORT}'],
          value_serializer = serializer)
      connected = True
    except errors.NoBrokersAvailable:
      print("\tNO BROKER AVAILABLE!")
      print("\tRetring in 5 seconds...")
      time.sleep(5)
  print(f"\tConnected to {BROKER_ADD}:{BROKER_PORT}")

  time.sleep(10)
  print("\tSending start message...")
  current_time = datetime.now()
  producer.send(TOPIC_PRODUCER, value={
      "date": f'{current_time.year}.{current_time.month}.{current_time.day}:{current_time.hour}.{current_time.minute}.{current_time.second}',
      "status": "START"
    })
  producer.flush()

  ##wait for confirm message (consumer)
  consumer = KafkaConsumer(
    bootstrap_servers=[f'{BROKER_ADD}:{BROKER_PORT}'],
    value_deserializer = deserializer,
    auto_offset_reset='latest'
  )
  
  consumer.subscribe(topics=TOPIC_CONSUMER)
  print(f"\tListening to {BROKER_ADD}:{BROKER_PORT} - {TOPIC_CONSUMER}...")
  for message in consumer:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))
    
    if message.value["status"] == "GREAT":
      print("\tAPI sink compleated. Stopping consumer.")
      break
    else:
      raise Exception(f"API sink gone wrong. Status: {message.value['status']}")