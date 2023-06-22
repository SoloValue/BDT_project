import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, col
from pyspark.sql.types import *  # to write data schema

connection_string="mongodb://root:psw@localhost:27017/"    # old atlas connection 
betas=[1.0, -0.1,-0.5]

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
    .withColumn("computed",  (col("forecast.prob_prec")*col("forecast.precipitazioni"))*betas[1] + col("forecast.wind")*betas[2])
df = df.drop("forecast")

#df.printSchema()

df.show()

rdd_weather = df.select("computed").rdd

f_of_x = 50 #AQI of this moment
f_list = [f_of_x]
for value in rdd_weather.collect():
    f_of_x = max(1, f_of_x + value[0])
    f_list.append(f_of_x)
print(f_list)

spark.stop()