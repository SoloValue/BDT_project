import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, col
from pyspark.sql.types import *  # to write data schema
from datetime import datetime
from pyspark.sql.functions import date_format, to_date, hour, substring


connection_string="mongodb://root:psw@localhost:27017/"    
betas=[1.0, -0.1,-0.5]
request_time="2023-06-24T10:12:54.608381"

spark = SparkSession.builder.master("local").appName("MongoDBSparkConnector") \
    .config("spark.driver.memory", "15g") \
    .config("spark.mongodb.read.connection.uri", connection_string) \
    .config("spark.mongodb.write.connection.uri", connection_string) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

sc = spark.sparkContext.setLogLevel("WARN")

weather_schema = StructType() \
    .add("_id", StringType()) \
    .add("request_time", StringType())\
    .add("forecast", 
            ArrayType(
                StructType() \
                .add("request_time", StringType())\
                .add("datetime", StringType()) \
                .add("precipitazioni", DoubleType()) \
                .add("prob_prec", IntegerType()) \
                .add("wind", DoubleType())   
         ))
    
tomtom_schema=StructType() \
    .add("_id", StringType()) \
    .add("request_time", StringType())\
    .add('forecasts',
            ArrayType(
                StructType()\
                .add("request_time", StringType())\
                .add("datetime", StringType())\
                .add("actual_traffic",DoubleType())
            ))

air_schema=StructType()\
    .add("_id", StringType())\
    .add("request_time", StringType())\
    .add("forecasts",
         ArrayType(
             StructType()\
             .add("request_time", StringType())\
             .add("datetime", StringType())
             .add("aqi", IntegerType())
         ))


## FOLLOWING SECTION IS TO FIX
# read and create spark dataframe

#WEATHER--------------------------------

df_weather= spark.read.format("mongodb") \
    .option("uri", connection_string) \
    .option("database", "preprocess_data") \
    .option("collection", "weather") \
    .schema(weather_schema) \
    .load()

df_weather = df_weather.select(explode("forecasts").alias("forecasts"))

df_weather=df_weather.filter(df_weather["forecasts.request_time"]==request_time)

df_weather_with_date = df_weather.withColumn("request_time", col("forecasts.request_time"))\
    .withColumn("datetime", col('forecasts.datetime'))\
    .withColumn('date', substring('datetime', 1, 10))\
    .withColumn('hour', substring('datetime', 12, 2))\
    .withColumn("precipitazioni", col("forecasts.precipitazioni")) \
    .withColumn("prob_prec", col("forecasts.prob_prec")) \
    .withColumn("wind", col("forecasts.wind")) \
    .withColumn("computed",  (col("forecasts.prob_prec")*col("forecasts.precipitazioni"))*betas[1] + col("forecasts.wind")*betas[2])
df_weather_with_day=df_weather_with_date.withColumn('day_of_week', date_format('date', 'EEEE'))
df_weather_with_day = df_weather_with_day.drop("forecasts")

#df.printSchema()

df_weather_with_day.show()

rdd_weather = df_weather_with_day.select("computed").rdd

#TOMTOM-------------------------
df_tomtom= spark.read.format("mongodb") \
    .option("uri", connection_string) \
    .option("database", "preprocess_data") \
    .option("collection", "tomtom") \
    .schema(tomtom_schema) \
    .load()

df_tomtom = df_tomtom.select(explode("forecasts").alias("forecasts"))

df_tomtom=df_tomtom.filter(df_tomtom["forecasts.request_time"]==request_time)

df_tomtom_with_date= df_tomtom.withColumn("request_time", col("forecasts.request_time"))\
    .withColumn("datetime", col('forecasts.datetime'))\
    .withColumn('date', substring('datetime', 1, 10))\
    .withColumn('hour', substring('datetime', 12, 2))\
    .withColumn("actual_traffic", col("forecasts.actual_traffic")) \

   
df_tomtom_with_day= df_tomtom_with_date.withColumn('day_of_week', date_format('date', 'EEEE'))
df_tomtom_with_day=df_tomtom_with_day.drop("_id")
df_tomtom_with_day=df_tomtom_with_day.drop("forecasts")
df_tomtom_with_day.show()

#AIR--------------------
df_air= spark.read.format("mongodb") \
    .option("uri", connection_string) \
    .option("database", "preprocess_data") \
    .option("collection", "air") \
    .schema(air_schema) \
    .load()

df_air = df_air.select(explode("forecasts").alias("forecasts"))

df_air=df_air.filter(df_air["forecasts.request_time"]==request_time)

df_air_with_date= df_air.withColumn("request_time", col("forecasts.request_time"))\
    .withColumn("datetime", col('forecasts.datetime')) \
    .withColumn('date', substring('datetime', 1, 10))\
    .withColumn('hour', substring('datetime', 12, 2))\
    .withColumn('aqi', col("forecasts.aqi")) 
    
    
df_air_with_day= df_air_with_date.withColumn('day_of_week', date_format('date', 'EEEE'))

df_air_with_day = df_air_with_day.drop("forecasts")
df_air_with_day.show()

f_of_x = 50 #AQI of this moment
f_list = [f_of_x]
for value in rdd_weather.collect():
    f_of_x = max(1, f_of_x + value[0])
    f_list.append(f_of_x)
print(f_list)

spark.stop()