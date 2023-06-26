from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

from magic_formula import aqi_formula

spark = SparkSession.builder.master("local").appName("MongoDBSparkConnector").getOrCreate()
sc = spark.sparkContext.setLogLevel("WARN")
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)
print("\tSpark on.")

## PARSE PREPROCESSED DATA
# ... code in tail_manager.py with mongoDB connection & pre-processing stage returning dictionaries ...

## CORE COMPUTATIONS (with Spark)
rdd_weather = spark.sparkContext.parallelize(pp_weather["forecasts"])
rdd_traffic = spark.sparkContext.parallelize(pp_traffic["forecasts"])
rdd_air = spark.sparkContext.parallelize([pp_air])

# join on datetime
rdd1_formatted = rdd_weather.map(lambda x: (x[0], x))    
rdd2_formatted = rdd_traffic.map(lambda x: (x[0], x))   

rdd_joined = rdd1_formatted.join(rdd2_formatted)
# map aqi_formula on each element of the joined rdd
output_rdd = rdd_joined.map(lambda data_point: aqi_formula(data_point[0][2],    # traffic
                                                        data_point[1][1],       # prec
                                                        data_point[1][2],       # prob_prec
                                                        data_point[1][3]))      # wind
# extract output AQI predictions
predictions = output_rdd.collect()
