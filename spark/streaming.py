from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp
import os
import sys
import time

spark = SparkSession.builder \
    .appName("flights_stream") \
    .config("spark.cassandra.connection.host", os.environ.get("CASSANDRA_HOST", "cassandra")) \
    .config("spark.cassandra.connection.port", os.environ.get("CASSANDRA_PORT", "9042")) \
    .getOrCreate()

# print("Topic: ", os.environ.get("KAFKA_TOPIC", "flights_topic"))

# Kafka source
kafka_flights_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "flights_topic"))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

print("Kafka source created")

flights_schema = StructType() \
    .add("YEAR", IntegerType()) \
    .add("MONTH", IntegerType()) \
    .add("DAY", IntegerType()) \
    .add("DAY_OF_WEEK", IntegerType()) \
    .add("AIRLINE", StringType()) \
    .add("FLIGHT_NUMBER", IntegerType()) \
    .add("TAIL_NUMBER", StringType()) \
    .add("ORIGIN_AIRPORT", StringType()) \
    .add("DESTINATION_AIRPORT", StringType()) \
    .add("SCHEDULED_DEPARTURE", IntegerType()) \
    .add("DEPARTURE_TIME", IntegerType()) \
    .add("DEPARTURE_DELAY", IntegerType()) \
    .add("TAXI_OUT", IntegerType()) \
    .add("WHEELS_OFF", IntegerType()) \
    .add("SCHEDULED_TIME", IntegerType()) \
    .add("ELAPSED_TIME", IntegerType()) \
    .add("AIR_TIME", IntegerType()) \
    .add("DISTANCE", IntegerType()) \
    .add("WHEELS_ON", IntegerType()) \
    .add("TAXI_IN", IntegerType()) \
    .add("SCHEDULED_ARRIVAL", IntegerType()) \
    .add("ARRIVAL_TIME", IntegerType()) \
    .add("ARRIVAL_DELAY", IntegerType()) \
    .add("DIVERTED", IntegerType()) \
    .add("CANCELLED", IntegerType()) \
    .add("CANCELLATION_REASON", StringType()) \
    .add("AIR_SYSTEM_DELAY", IntegerType()) \
    .add("SECURITY_DELAY", IntegerType()) \
    .add("AIRLINE_DELAY", IntegerType()) \
    .add("LATE_AIRCRAFT_DELAY", IntegerType()) \
    .add("WEATHER_DELAY", IntegerType())

flights_df = kafka_flights_df.select(from_json(col("value").cast("string"), flights_schema).alias("data")).select("data.*")

flights_df.printSchema()

airport_df = spark.read.csv('data/airports.csv',header=True,inferSchema=True)
airline_df = spark.read.csv('data/airlines.csv',header=True,inferSchema=True)

airline_df=airline_df.withColumnRenamed("AIRLINE","AIRLINES")
flights_airlines_df = flights_df.join(airline_df, flights_df.AIRLINE == airline_df.IATA_CODE, "left")\
    .drop(airline_df.IATA_CODE)

airport_origin_df = airport_df.withColumnRenamed("IATA_CODE", "ORIGIN_AIRPORT_CODE")\
    .withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME")\
    .withColumnRenamed("CITY", "ORIGIN_CITY")\
    .withColumnRenamed("STATE", "ORIGIN_STATE")\
    .withColumnRenamed("COUNTRY", "ORIGIN_COUNTRY")\
    .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE")\
    .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")

flights_airlines_airports_df = flights_airlines_df.join(
    airport_origin_df,
    flights_airlines_df.ORIGIN_AIRPORT == airport_origin_df.ORIGIN_AIRPORT_CODE,
    "left"
)

airport_destination_df = airport_df.withColumnRenamed("IATA_CODE", "DESTINATION_AIRPORT_CODE")\
    .withColumnRenamed("AIRPORT", "DESTINATION_AIRPORT_NAME")\
    .withColumnRenamed("CITY", "DESTINATION_CITY")\
    .withColumnRenamed("STATE", "DESTINATION_STATE")\
    .withColumnRenamed("COUNTRY", "DESTINATION_COUNTRY")\
    .withColumnRenamed("LATITUDE", "DESTINATION_LATITUDE")\
    .withColumnRenamed("LONGITUDE", "DESTINATION_LONGITUDE")\
    .withColumnRenamed("AIRLINE","AIRLINES")

flights_airlines_airports_df = flights_airlines_airports_df.join(
    airport_destination_df,
    flights_airlines_airports_df.DESTINATION_AIRPORT == airport_destination_df.DESTINATION_AIRPORT_CODE,
    "left"
)

# 1. Airline performance - Filter null AIRLINE (primary key)
airline_stats = flights_airlines_airports_df \
    .filter(col("AIRLINE").isNotNull()) \
    .groupBy("AIRLINE", "AIRLINES") \
    .agg(
        F.count("*").alias("total_flights"),
        F.avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
        F.avg("ARRIVAL_DELAY").alias("avg_arrival_delay"),
        F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights")
    )

airline_stats_out = airline_stats.select(
    col("AIRLINE").alias("airline"),
    col("AIRLINES").alias("airlines"),
    col("total_flights"),
    col("avg_departure_delay"),
    col("avg_arrival_delay"),
    col("cancelled_flights")
)

# 2. Route analysis - Filter null primary keys
route_stats = flights_airlines_airports_df \
    .filter(col("ORIGIN_AIRPORT").isNotNull() & col("DESTINATION_AIRPORT").isNotNull()) \
    .groupBy(
        "ORIGIN_AIRPORT", "ORIGIN_CITY", "ORIGIN_STATE",
        "DESTINATION_AIRPORT", "DESTINATION_CITY", "DESTINATION_STATE"
    ) \
    .agg(
        F.count("*").alias("flight_count"),
        F.avg("DISTANCE").alias("avg_distance"),
        F.avg("ARRIVAL_DELAY").alias("avg_delay")
    )

route_stats_out = route_stats.select(
    col("ORIGIN_AIRPORT").alias("origin_airport"),
    col("ORIGIN_CITY").alias("origin_city"),
    col("ORIGIN_STATE").alias("origin_state"),
    col("DESTINATION_AIRPORT").alias("destination_airport"),
    col("DESTINATION_CITY").alias("destination_city"),
    col("DESTINATION_STATE").alias("destination_state"),
    col("flight_count"),
    col("avg_distance"),
    col("avg_delay")
)

# 3. Geographic heatmap data - Filter null latitude/longitude (primary keys)
geo_analysis = flights_airlines_airports_df \
    .filter(col("ORIGIN_LATITUDE").isNotNull() & col("ORIGIN_LONGITUDE").isNotNull()) \
    .groupBy(
        "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", 
        "ORIGIN_CITY", "ORIGIN_STATE"
    ) \
    .agg(F.count("*").alias("flight_count"))

geo_analysis_out = geo_analysis.select(
    col("ORIGIN_CITY").alias("origin_city"),
    col("ORIGIN_STATE").alias("origin_state"),
    col("ORIGIN_LATITUDE").alias("origin_latitude"),
    col("ORIGIN_LONGITUDE").alias("origin_longitude"),
    col("flight_count")
)

def write_airline_stats(batch_df, batch_id):
    print(f"Writing airline_stats batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="airline_stats", keyspace="flights_db") \
        .save()

    # Write to HDFS as Parquet
    batch_df.write \
        .mode("append") \
        .parquet(f"hdfs://namenode:9000/flights/airline_stats/")


def write_route_stats(batch_df, batch_id):
    print(f"Writing route_stats batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="route_stats", keyspace="flights_db") \
        .save()

    # Write to HDFS
    batch_df.write \
        .mode("append") \
        .parquet("hdfs://namenode:9000/flights/route_stats/")


def write_geo_analysis(batch_df, batch_id):
    print(f"Writing geo_analysis batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="geo_analysis", keyspace="flights_db") \
        .save()

    # Write to HDFS
    batch_df.write \
        .mode("append") \
        .parquet("hdfs://namenode:9000/flights/geo_analysis/")


# Start all streaming queries
query1 = airline_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_airline_stats) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/airline") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Query 1 (airline_stats) started")

query2 = route_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_route_stats) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/route") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Query 2 (route_stats) started")

query3 = geo_analysis_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_geo_analysis) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/geo") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Query 3 (geo_analysis) started")

print("Raw Kafka stream output (for debugging):")
raw_df = kafka_flights_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
query = raw_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
query.awaitTermination()



while query1.isActive or query2.isActive or query3.isActive:
    def safe_rows(q):
        lp = q.lastProgress
        return lp['numInputRows'] if lp else 0

    status_line = (
        f"Q1 rows={safe_rows(query1)} | "
        f"Q2 rows={safe_rows(query2)} | "
        f"Q3 rows={safe_rows(query3)}"
    )
    print(status_line, end="\r", flush=True)
    time.sleep(2)


    

print("\nStreaming queries finished")
