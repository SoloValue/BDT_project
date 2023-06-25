#LIBRARIES---------------------------
from kafka import KafkaConsumer, KafkaProducer, errors
import pymongo
import yaml
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#CLASSESS----------------------------
from serializers import serializer, deserializer
from preprocess import pre_proc
from magic_formulatry import aqi_formula

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

  spark = SparkSession.builder.master("local").appName("MongoDBSparkConnector").getOrCreate()
  sc = spark.sparkContext.setLogLevel("WARN")

  consumer.subscribe(topics=TOPIC_CONSUMER)
  print(f'\tWaiting for message on: {TOPIC_CONSUMER}...')
  for message in consumer:
    print ("%d:%d: msg=%s" % (
      message.partition,
      message.offset,
      message.value))
    
    if message.value["status"] != "GREAT":
      print(f"API sink gone wrong. Status: {message.value['status']}")
      pass

    print("\tAPI sink compleated. Starting spark...")

    ## mongo connection and preprocess of data
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

    def printRDD(rdd):
      coll_rdd = rdd.collect()
      for row in coll_rdd:
        print(row)
        print("\n")

    db_api = mongo_client[config["mongodb"]["databases"]["api_raw"]]
    db_pp = mongo_client[config["mongodb"]["databases"]["preprocess_data"]]
    request_time = message.value["request_time"]

    pp_weather, pp_traffic, pp_air = pre_proc(db_api, db_pp, request_time)
    print(f"\tData recovered from: {CONNECTION_STRING}")

    ## CORE COMPUTATIONS (PREDICTIONS)

    rdd_weather = spark.sparkContext.parallelize(pp_weather["forecasts"])
    rdd_traffic = spark.sparkContext.parallelize(pp_traffic["forecasts"])
    #rdd_traffic = spark.sparkContext.parallelize([f for f in pp_traffic["forecasts"]])
    rdd_air = spark.sparkContext.parallelize([pp_air])

    # join on datetime
    rdd1_formatted = rdd_weather.map(lambda x: (x[0], x))    
    rdd2_formatted = rdd_traffic.map(lambda x: (x[0], x))   
    
    rdd_joined = rdd1_formatted.join(rdd2_formatted)
    #rdd_joined = rdd_joined.map(lambda x: x[1])

    #df_joined.collect()

    ## Apply the air quality formula to each record: The map transformation applies a given function to each 
    ## element of the RDD and returns a new RDD with the transformed results.
    # output_rdd = rdd_joined.map(lambda data_point: aqi_formula(data_point['traffic'],
    #                               data_point['precipitazioni'], 
    #                               data_point['prob_prec'], 
    #                               data_point['wind']))   

    output_rdd = rdd_joined.map(lambda data_point: aqi_formula(data_point[0][2],   # traffic
                                                           data_point[1][1],       # prec
                                                           data_point[1][2],       # prob_prec
                                                           data_point[1][3]))      # wind

    
    predictions = output_rdd.collect()

    # packing up output
    # Convert RDD to DataFrame
    #schema = StructType([StructField("prediction", DoubleType(), True)])
    #output_df = spark.createDataFrame(output_rdd, schema)
    #predictions = output_df.select("prediction").rdd.flatMap(lambda x: [x]).collect()

    # PLEASE
    # index_rdd = 0
    # def dict_to_string():
    #   index_rdd = index_rdd + 1
    #   return index_rdd

    # output_rdd = output_rdd.map(lambda x: (dict_to_string(x), x))
    # predictions = output_rdd.first()

    # Convert DataFrame to a list
    #predictions = output_rdd.flatMap(lambda x: x)

    # PLEASE !!
    # Serialize RDD elements to JSON
    #predictions = output_rdd.map(lambda x: json.dumps(x))

    # Collect serialized RDD into a list
    #predictions = output_rdd_serializable.collect()

    # predictions = []
    exp_traffic = []
    for i in range(97):
    #   predictions.append(120-i)
      exp_traffic.append(i/37)
    print(f"Predictions: {predictions}")

    ## Saving predictions
    pred_db = mongo_client[config["mongodb"]["databases"]["output"]]
    pred_collection = pred_db["predictions"]
    '''pred_collection.insert_one({
      "request_time": request_time,
      "id_location": message.value["id_location"],
      "predictions": predictions,
      "exp_traffic": exp_traffic
    })'''

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
        "predictions": predictions,
        "exp_traffic": exp_traffic
      })
    producer.flush()
    print(f"\tPredictions sent to topic: {TOPIC_PRODUCER}.")