import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession 
from pyspark.sql import SQLContext
import os

#os.environ['SPARK_HOME'] = ''
os.environ["JAVA_HOME"] = "C:\Program Files (x86)\Java\jre-1.8"


conf=pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo=spark=connector_2.12:3.0.1"
                             ).setMaster("local").setAppName("MyApp").setAll([("spark.driver.memory", "40g"), 
                                                                              ("spark.executor.memory","50g")])

sc=SparkContext(conf=conf)
