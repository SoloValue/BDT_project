#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import yaml
from datetime import datetime
import time

#CLASSESS----------------------------
from serializers import serializer, deserializer
from prediction_retrival import prediction_retrival

#MAIN--------------------------------
if __name__ == "__main__":
  ##load config
  CONFIG_PATH = "./config/config.yaml"
  with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
    print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
    PROJECT_ENV = config["project"]["environment"]

  ## waiting for input message
  BROKER_ADD_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["address"] for i in range(3)]
  BROKER_PORT_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["port"] for i in range(3)]
  TOPIC_PRODUCER = config["kafka"]["topics"]["head-api_sink"]
  TOPIC_CONSUMER_1 = config["kafka"]["topics"]["input-head"]
  TOPIC_CONSUMER_2 = config["kafka"]["topics"]["api_sink-tail"]

  connected = False
  while not connected:
    try:
      consumer_1 = KafkaConsumer(
          bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                             f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                             f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
          value_deserializer = deserializer
      )
      connected = True
    except errors.NoBrokersAvailable:
      print("\tNO BROKER AVAILABLE!")
      print("\tRetring in 5 seconds...")
      time.sleep(5)
  print(f"\tConnected to {BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}")

  consumer_1.subscribe(topics=TOPIC_CONSUMER_1)
  print(f"\tWaiting for input message on: {TOPIC_CONSUMER_1} ...")
  for message in consumer_1:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))
    if message.value["status"] != "START":
      break

    print(f"\t... message recived.")
    
    ## send start message (producer)
    producer = KafkaProducer(
        bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                           f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                           f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
        value_serializer = serializer
    )
    #time.sleep(1)
    print("\tSending start message...")
    current_time = datetime.now().isoformat()
    producer.send(TOPIC_PRODUCER, value={
        "request_time": current_time,
        "status": "START",
        "id_location": message.value["id_location"]
      })
    producer.flush()

    ##wait for confirm message (consumer)
    consumer_2 = KafkaConsumer(
      bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                         f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                         f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
      value_deserializer = deserializer,
      auto_offset_reset='latest'
    )
    
    consumer_2.subscribe(topics=TOPIC_CONSUMER_2)
    print(f"\tListening to {BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]} - {TOPIC_CONSUMER_2}...")
    for message in consumer_2:
      print ("%d:%d: msg=%s" % (
        message.partition,
        message.offset,
        message.value))
      
      if message.value["status"] == "GREAT":
        print("\tAPI sink compleated. Stopping consumer.")
        break
      else:
        raise Exception(f"API sink gone wrong. Status: {message.value['status']}")