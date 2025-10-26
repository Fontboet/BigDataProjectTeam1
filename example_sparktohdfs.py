from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType

'''
Script example, creates a spark session with HDFS
'''

# 1 : Create Spark Session with HDFS and MongoDB configs
spark = SparkSession.builder \
    .appName("SparkToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()

# 2️ : Example of JSON schema for flight data
schema = StructType() \
    .add("flight_number", StringType()) \
    .add("airline", StringType()) \
    .add("origin", StringType()) \
    .add("destination", StringType()) \
    .add("delay", DoubleType()) \
    .add("status", StringType())

# 3 : Read stream for Kafka topic (not sure about the 29092 port)
# Replace "topic_name" with the actual Kafka topic name
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "topic_name")  \
    .option("startingOffsets", "latest") \
    .load()

# 4 : Parse Kafka value as JSON according to schema
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 5 : Simple transformation example
df_transformed = df_parsed.withColumn(
    "delay_category",
    when(col("delay") > 0, "Delayed")
    .otherwise("On Time")
)

# 6 : Write stream to HDFS in Parquet format
hdfs_query = df_transformed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:9000/flights/stream_output") \
    .option("checkpointLocation", "/tmp/hdfs_checkpoint") \
    .outputMode("append") \
    .start()