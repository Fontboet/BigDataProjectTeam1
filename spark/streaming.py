from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp
import os

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
# Columns: airline, cancelled_flights, on_time_flights, delayed_flights, avg_departure_delay, avg_arrival_delay

airline_stats = flights_airlines_airports_df \
    .filter(col("AIRLINE").isNotNull()) \
    .groupBy("AIRLINE") \
    .agg(
        F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights"),
        F.sum(F.when((F.col("CANCELLED") == 0) & (F.col("DEPARTURE_DELAY") <= 15), 1).otherwise(0)).alias("on_time_flights"),
        F.sum(F.when((F.col("CANCELLED") == 0) & (F.col("DEPARTURE_DELAY") > 15), 1).otherwise(0)).alias("delayed_flights"),
        F.avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
        F.avg("ARRIVAL_DELAY").alias("avg_arrival_delay")
    )

airline_stats_out = airline_stats.select(
    col("AIRLINE").alias("airline"),
    col("cancelled_flights"),
    col("on_time_flights"),
    col("delayed_flights"),
    col("avg_departure_delay"),
    col("avg_arrival_delay")
)

# 2. Delay by Reason Analysis
# Columns: delay_reason, count, avg_duration
# We need to unpivot the delay columns to analyze reasons

# Select delay columns
delay_cols = [
    "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", 
    "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"
]

# Filter rows where at least one delay reason > 0
delay_df = flights_airlines_airports_df.filter(
    (col("AIR_SYSTEM_DELAY") > 0) | 
    (col("SECURITY_DELAY") > 0) | 
    (col("AIRLINE_DELAY") > 0) | 
    (col("LATE_AIRCRAFT_DELAY") > 0) | 
    (col("WEATHER_DELAY") > 0)
)

# Unpivot/Stack the delay columns
# Using a stack expression string dynamically
stack_expr = "stack(5, " + ", ".join([f"'{c}', {c}" for c in delay_cols]) + ") as (delay_reason, duration)"

delay_unpivoted = delay_df.selectExpr(stack_expr) \
    .filter(col("duration") > 0)

delay_stats = delay_unpivoted \
    .groupBy("delay_reason") \
    .agg(
        F.count("*").alias("count"),
        F.avg("duration").alias("avg_duration")
    )

delay_stats_out = delay_stats.select(
    col("delay_reason"),
    col("count"),
    col("avg_duration")
)

# 3. Route analysis - Filter null primary keys
route_stats = flights_airlines_airports_df \
    .filter(col("ORIGIN_AIRPORT").isNotNull() & col("DESTINATION_AIRPORT").isNotNull()) \
    .groupBy(
        "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
        "ORIGIN_CITY", "ORIGIN_STATE", "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE",
        "DESTINATION_CITY", "DESTINATION_STATE"
    ) \
    .agg(
        F.avg("ARRIVAL_DELAY").alias("avg_delay")
    )

route_stats_out = route_stats.select(
    col("ORIGIN_AIRPORT").alias("original_airport"),
    col("DESTINATION_AIRPORT").alias("destination_airport"),
    col("ORIGIN_CITY").alias("original_city"),
    col("ORIGIN_STATE").alias("original_state"),
    col("DESTINATION_CITY").alias("destination_city"),
    col("DESTINATION_STATE").alias("destination_state"),
    col("ORIGIN_LATITUDE").alias("original_latitude"),
    col("ORIGIN_LONGITUDE").alias("original_longitude"),
    col("avg_delay")
)

def write_airline_stats(batch_df, batch_id):
    print(f"Writing airline_stats batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="airline_stats", keyspace="flights_db") \
        .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
        .save()


def write_delay_stats(batch_df, batch_id):
    print(f"Writing delay_stats batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="delay_by_reason", keyspace="flights_db") \
        .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
        .save()

def write_route_stats(batch_df, batch_id):
    print(f"Writing route_stats batch {batch_id} - {batch_df.count()} rows")

    batch_df = batch_df.withColumn("updated_at", current_timestamp())

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="route_stats", keyspace="flights_db") \
        .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
        .save()

# Start all streaming queries
query1 = airline_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_airline_stats) \
    .trigger(processingTime="10 seconds") \
    .start()
    # .option("checkpointLocation", "/tmp/checkpoint/airline") \

query2 = delay_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_delay_stats) \
    .trigger(processingTime="10 seconds") \
    .start()
    # .option("checkpointLocation", "/tmp/checkpoint/delay") \

query3 = route_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_route_stats) \
    .trigger(processingTime="10 seconds") \
    .start()
    # .option("checkpointLocation", "/tmp/checkpoint/route") \

query3.awaitTermination()
