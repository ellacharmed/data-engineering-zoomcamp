import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

ps_schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

scala_version = '2.12'
pyspark_version = pyspark.__version__

kafka_jar_package = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{pyspark_version}'

spark = SparkSession.builder \
    .appName("GreenTripsConsumer") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()   
     

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "checkpoint") \
    .load()

popular_destinations = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), ps_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.current_timestamp()) \
    .groupBy(F.window("timestamp", "5 minutes"), "DOLocationID") \
    .count() \
    .orderBy(F.col("count").desc())


query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

spark.stop()
