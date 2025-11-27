#!/usr/bin/env python3
"""
Spark Structured Streaming: read from Kafka, transform flight messages, write aggregates to HDFS (Parquet).

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  spark/spark_to_HDFS.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import pyspark.sql.functions as F
import os

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bdsp_topic_test"
AIRPORTS_CSV = "data/airports.csv"  # file:// to force local filesystem
AIRLINES_CSV = "data/airlines.csv"
HDFS_URI = "hdfs://localhost:9000"
OUTPUT_BASE = "hdfs://localhost:9000/user/admin/flights_output"  # hdfs:// explicit for output
CHECKPOINT_BASE = "hdfs://localhost:9000/tmp/spark_checkpoints"


def main():
    # SparkSession w/o fs.defaultFS because locally, need to add it when deployed
    spark = SparkSession.builder \
        .appName("flights_stream_to_hdfs") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    # Kafka source
    kafka_flights_df = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load())
    
    print("Kafka source created")

    # Schema JSON
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

    airport_df = spark.read.csv(AIRPORTS_CSV, header=True, inferSchema=True)
    airline_df = spark.read.csv(AIRLINES_CSV, header=True, inferSchema=True)
    airline_df = airline_df.withColumnRenamed("AIRLINE", "AIRLINES")

    # Joins
    flights_airlines_df = flights_df.join(airline_df, flights_df.AIRLINE == airline_df.IATA_CODE, "left").drop(airline_df.IATA_CODE)

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

    flights_enriched_df = flights_airlines_airports_df.join(
        airport_destination_df,
        flights_airlines_airports_df.DESTINATION_AIRPORT == airport_destination_df.DESTINATION_AIRPORT_CODE,
        "left"
    )

    # Aggregations
    airline_stats = flights_enriched_df \
        .filter(col("AIRLINE").isNotNull()) \
        .groupBy("AIRLINE", "AIRLINES") \
        .agg(
            F.count("*").alias("total_flights"),
            F.avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
            F.avg("ARRIVAL_DELAY").alias("avg_arrival_delay"),
            F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights")
        )

    route_stats = flights_enriched_df \
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

    geo_analysis = flights_enriched_df \
        .filter(col("ORIGIN_LATITUDE").isNotNull() & col("ORIGIN_LONGITUDE").isNotNull()) \
        .groupBy(
            "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE",
            "ORIGIN_CITY", "ORIGIN_STATE"
        ) \
        .agg(F.count("*").alias("flight_count"))

    def write_batch_to_hdfs(batch_df, batch_id, path, partition_cols=None):
        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] No rows to write to {path}")
            return
        writer = batch_df.write.mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)
        print(f"[batch {batch_id}] Wrote {batch_df.count()} rows to {path}")

    airline_out = os.path.join(OUTPUT_BASE, "airline_stats")
    route_out   = os.path.join(OUTPUT_BASE, "route_stats")
    geo_out     = os.path.join(OUTPUT_BASE, "geo_analysis")

    # Streaming queries
    query1 = (airline_stats.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, batch_id: write_batch_to_hdfs(df, batch_id, airline_out, partition_cols=["AIRLINE"]))
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "airline"))
        .start()
    )

    query2 = (route_stats.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, batch_id: write_batch_to_hdfs(df, batch_id, route_out, partition_cols=["ORIGIN_STATE", "DESTINATION_STATE"]))
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "route"))
        .start()
    )

    query3 = (geo_analysis.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, batch_id: write_batch_to_hdfs(df, batch_id, geo_out, partition_cols=["ORIGIN_STATE"]))
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "geo"))
        .start()
    )

    print("Streaming queries started. Waiting for termination...")
    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()

if __name__ == "__main__":
    main()