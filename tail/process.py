import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, col
from pyspark.sql.types import *  # to write data schema

connection_string="mongodb://root:psw@localhost:27017/"    # old atlas connection 
#connection_string = "mongodb://localhost:27017/"

spark = SparkSession.builder.master("local").appName("MongoDBSparkConnector") \
    .config("spark.driver.memory", "15g") \
    .config("spark.mongodb.read.connection.uri", connection_string) \
    .config("spark.mongodb.write.connection.uri", connection_string) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

sc = spark.sparkContext.setLogLevel("WARN")

weather_schema = StructType() \
    .add("_id", StringType()) \
    .add("forecast", 
            ArrayType(
                StructType() \
                .add("datetime", StringType()) \
                .add("precipitazioni", DoubleType()) \
                .add("prob_prec", IntegerType()) \
                .add("wind", DoubleType())   
         ))
    

## FOLLOWING SECTION IS TO FIX
# read and create spark dataframe
df = spark.read.format("mongodb") \
    .option("uri", connection_string) \
    .option("database", "preprocess_data") \
    .option("collection", "weather") \
    .schema(weather_schema) \
    .load()

df = df.select(explode("forecast").alias("forecast"))

df = df.withColumn("datetime", col("forecast.datetime")) \
    .withColumn("precipitazioni", col("forecast.precipitazioni")) \
    .withColumn("prob_prec", col("forecast.prob_prec")) \
    .withColumn("wind", col("forecast.wind")) \

# Drop the original 'forecast' column if needed
df = df.drop("forecast")


df.printSchema()

df.show()


spark.stop()