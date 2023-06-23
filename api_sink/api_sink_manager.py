#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import yaml
import pymongo
from datetime import datetime
import time
import pandas as pd 

#CLASSESS----------------------------
from serializers import serializer, deserializer
from api_requests import get_all_requests, insert_docs

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
    
    ##retrieve data from API
    def get_request_input(localita):
        data_cities=pd.read_csv('./config/cities.csv')
        id_localita=data_cities.loc[data_cities["localita"]==localita, data_cities["id_localita"]]
        in_lat=data_cities.loc[data_cities["localita"]==localita, data_cities['lat']]
        in_long=data_cities.loc[data_cities["localita"]==localita, data_cities['long']]
        return id_localita, in_lat, in_long

    id_localita, in_lat, in_long=get_request_input("Trento")
    traffic_data, air_data, weather_data, request_time = get_all_requests(in_lat, in_long, id_localita, 4)
    print("\tData from API recived")

    ##save it on mongodb
    MONGO_ENV = config["mongodb"]["environment"]
    if MONGO_ENV == "atlas":
      CONNECTION_STRING = config["mongodb"][MONGO_ENV]["connection_string"]
      myclient = pymongo.MongoClient(CONNECTION_STRING)
    else:
      MONGO_ADD = CONNECTION_STRING = config["mongodb"][MONGO_ENV]["address"]
      MONGO_PORT = CONNECTION_STRING = config["mongodb"][MONGO_ENV]["port"]
      username = config["mongodb"]["username"]
      username = config["mongodb"]["password"]
      myclient = pymongo.MongoClient(f'{MONGO_ADD}:{MONGO_PORT}',
                                    username = "root",
                                    password = "psw")

    mydb = myclient[config["mongodb"]["databases"]["api_raw"]]
    traffic_id, air_id, weather_id = insert_docs(traffic_data, air_data, weather_data, mydb)
    print("\tData saved in mongodb")

    ##start next section (producer)
    print("\tSending message...")
    #time.sleep(1)
    producer = KafkaProducer(
        bootstrap_servers = [f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}'],
        value_serializer = serializer)
    
    current_time = datetime.now()
    producer.send(TOPIC_PRODUCER, value={
      "date": f'{current_time.year}.{current_time.month}.{current_time.day}:{current_time.hour}.{current_time.minute}.{current_time.second}',
      "status": "GREAT",
      "traffic_id": str(traffic_id),
      "air_id": str(air_id),
      "weather_id": str(weather_id),
      "request_time": request_time,
    })
    producer.flush()
    print(f"\t...message sent to: {BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}-{TOPIC_PRODUCER}")

