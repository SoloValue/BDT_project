import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession 
from pyspark.sql import SQLContext

connection_string="mongodb+srv://admin:psw@cluster0.ew7zhpy.mongodb.net"

spark = SparkSession.builder.appName("MongoDBSparkConnector") \
    .config("spark.mongodb.input.uri", connection_string) \
    .config("spark.mongodb.output.uri", connection_string) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

sc = spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("mongodb").option("uri", connection_string).option("database", "mydatabase").option("collection", "customers").load()
df.printSchema()
spark.stop()

# spark.conf.set("spark.mongodb.input.connectionTimeoutMS", "100000")  # Set timeout to 100 seconds


# conf=pyspark.SparkConf().set("spark.jars.packages", "mongodb+srv://admin:psw@cluster0.ew7zhpy.mongodb.net/"
#                              ).setMaster("").setAppName("MyApp").setAll([("spark.driver.memory", "40g"), 
#                                                                               ("spark.executor.memory","50g")])
# sc=SparkContext(conf=conf)
