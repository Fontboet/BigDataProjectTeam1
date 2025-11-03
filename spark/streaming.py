from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("flights_stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Kafka source (adjust bootstrap if needed: kafka:9092 or kafka:29092)
kafka_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bdsp_topic_test")   # use your topic
    .option("startingOffsets", "earliest")
    .load())

print("Kafka source created")  # debug visible in spark-submit stdout

schema = StructType() \
    .add("YEAR", StringType()) \
    .add("MONTH", StringType()) \
    .add("DAY", StringType()) \
    .add("DAY_OF_WEEK", StringType()) \
    .add("AIRLINE", StringType()) \
    .add("FLIGHT_NUMBER", StringType()) \
    

df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# debug: write parsed rows to console
query = df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

