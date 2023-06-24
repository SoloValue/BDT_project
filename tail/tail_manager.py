#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import pymongo
import yaml
import time

#CLASSESS----------------------------
from serializers import serializer, deserializer
from preprocess import pre_proc

#MAIN--------------------------------
if __name__ == "__main__":
  ##load config
  CONFIG_PATH = "./config/config.yaml"
  with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
    print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
    PROJECT_ENV = config["project"]["environment"]

  ## wait for message (consumer)
  BROKER_ADD_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["address"] for i in range(3)]
  BROKER_PORT_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["port"] for i in range(3)]
  TOPIC_CONSUMER = config["kafka"]["topics"]["api_sink-tail"]
  TOPIC_PRODUCER = config["kafka"]["topics"]["tail-output"]

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
      print("\tNO BROKER AVAILABLE!")
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
    
    if message.value["status"] != "GREAT":
      print(f"API sink gone wrong. Status: {message.value['status']}")
      pass

    print("\tAPI sink compleated. Starting spark...")

    ## mongo connection
    MONGO_ENV = config["mongodb"]["environment"]
    if MONGO_ENV == "atlas":
        CONNECTION_STRING = config["mongodb"][MONGO_ENV]["connection_string"]
        mongo_client = pymongo.MongoClient(CONNECTION_STRING)
    else:
        MONGO_ADD = config["mongodb"][MONGO_ENV]["address"]
        MONGO_PORT = config["mongodb"][MONGO_ENV]["port"]
        CONNECTION_STRING = f'{MONGO_ADD}:{MONGO_PORT}'
        username = config["mongodb"]["username"]
        username = config["mongodb"]["password"]
        mongo_client = pymongo.MongoClient(CONNECTION_STRING,
                                    username = "root",
                                    password = "psw")

    db_api = mongo_client[config["mongodb"]["databases"]["api_raw"]]
    db_pp = mongo_client[config["mongodb"]["databases"]["preprocess_data"]]
    request_time = message.value["request_time"]
    #weather_data, traffic_data, air_data = pre_proc(db_api, db_pp, request_time)
    pp_weather = pre_proc(db_api, db_pp, request_time)
    #print(pp_weather)
    print(f"\tData recovered from: {CONNECTION_STRING}")

    ## ML project 2 #TODO
    predictions = []
    for i in range(97):
      predictions.append(180-i)
    print(f"Predictions: {predictions}")

    ## Saving predictions
    pred_db = mongo_client[config["mongodb"]["databases"]["output"]]
    pred_collection = pred_db["predictions"]
    pred_collection.insert_one({
      "datetime": request_time,
      "predictions": predictions
    })

    ## Send output
    print("\tSending output...")
    producer = KafkaProducer(
          bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                             f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                             f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
          value_serializer = serializer
          )
    #time.sleep(1)
    producer.send(TOPIC_PRODUCER, value={
        "request_time": request_time,
        "status": "GREAT",
        "predictions": predictions
      })
    producer.flush()
    print(f"\tOutput sent to {TOPIC_PRODUCER}.")