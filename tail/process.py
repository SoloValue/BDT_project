import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession 
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType  # to write data schema

connection_string="mongodb+srv://admin:psw@cluster0.ew7zhpy.mongodb.net/"    # old atlas connection 
#connection_string = "mongodb://localhost:27017/"

spark = SparkSession.builder.master("local").appName("MongoDBSparkConnector") \
    .config("spark.driver.memory", "15g") \
    .config("spark.mongodb.read.connection.uri", connection_string) \
    .config("spark.mongodb.write.connection.uri", connection_string) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

sc = spark.sparkContext.setLogLevel("WARN")

## FOLLOWING SECTION IS TO FIX
# read and create spark dataframe
df = spark.read.format("mongodb") \
    .option("uri", connection_string) \
    .option("database", "mydatabase") \
    .option("collection", "tomtom") \
    .option("collection", "air") \
    .option("collection", "weather") \
    .load()

df.printSchema()

# Create a schema for the JSON data #TO CHECK WITH df.printSchema above with actual database
json_schema = StructType().add("request_time", "string") \
                          .add("traffic_flow", "double") \
                          .add("aqi", "integer") \
                          .add("precipitazioni", "integer") \
                          .add("prob_precipitazioni", "integer") \
                          .add("wind", "double")    # maybe put as double ?

# Convert JSON data to a PySpark DataFrame
#data_df = spark.createDataFrame(df, schema=json_schema)
df.show()

spark.stop()


