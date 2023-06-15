#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import yaml
import pymongo
from datetime import datetime
import time

#CLASSESS----------------------------
from serializers import serializer, deserializer
#import sys CHANGE ROOT PATH
#look for setup.py

#MAIN--------------------------------
if __name__ == "__main__":
  ##load config
  CONFIG_PATH = "./config/config.yaml"
  with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
    print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
    PROJECT_ENV = config["project"]["environment"]

  ## start listening (consumer)
  BROKER_ADD_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["address"] for i in range(3)]
  BROKER_PORT_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["port"] for i in range(3)]
  TOPIC_CONSUMER = config["kafka"]["topics"]["head-api_sink"]
  TOPIC_PRODUCER = config["kafka"]["topics"]["api_sink-tail"]

  connected = False
  while not connected:
    try:
      consumer = KafkaConsumer(
          bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                             f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                             f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
          value_deserializer = deserializer,
          auto_offset_reset='latest')
      connected = True
    except errors.NoBrokersAvailable:
      print("\tNO BROKERS AVAILABLE!")
      print("\tRetring in 5 seconds...")
      time.sleep(5)
  print(f"\tConnected to {BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}")

  consumer.subscribe(topics=TOPIC_CONSUMER)
  print(f'\tListening to topic: {TOPIC_CONSUMER}...')
  for message in consumer:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))
    
    ##retrive data from API TODO
    to_send = {
        "name": "Teo",
        "hope": "not really",
        "will it work": "plz I need it",
        "how much in a scale to 1 to 10": 9
    }
    print("\tData from API recived")

    ##save it on mongodb TODO
    CONNECTION_STRING = config["mongodb"]["atlas"]["connection_string"]
    myclient = pymongo.MongoClient(CONNECTION_STRING)
    mydb = myclient["mydatabase"]
    mycol = mydb["customers"]
    x = mycol.insert_one(to_send)
    print(x.inserted_id)
    print("\tData saved in mongodb")

    ##start next section (producer)
    print("\tStarting next section...")
    producer = KafkaProducer(
        bootstrap_servers = [f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}'],
        value_serializer = serializer)
    
    current_time = datetime.now()
    producer.send(TOPIC_PRODUCER, value={
      "date": f'{current_time.year}.{current_time.month}.{current_time.day}:{current_time.hour}.{current_time.minute}.{current_time.second}',
      "status": "GREAT"
    })
    producer.flush()
    print(f"\t...message sent to: {BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}-{TOPIC_PRODUCER}")

