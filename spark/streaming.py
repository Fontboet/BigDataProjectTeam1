from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("flights_stream") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Kafka source
kafka_flights_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bdsp_topic_test")
    .option("startingOffsets", "earliest")
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

airport_df.printSchema()
airline_df.printSchema()

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

airport_origin_df.printSchema()

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

# Create multiple aggregations for different dashboards

# 1. Airline performance
airline_stats = flights_airlines_airports_df.groupBy("AIRLINE", "AIRLINES") \
    .agg(
        F.count("*").alias("total_flights"),
        F.avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
        F.avg("ARRIVAL_DELAY").alias("avg_arrival_delay"),
        F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights")
    )

# 2. Route analysis
route_stats = flights_airlines_airports_df.groupBy(
    "ORIGIN_AIRPORT", "ORIGIN_CITY", "ORIGIN_STATE",
    "DESTINATION_AIRPORT", "DESTINATION_CITY", "DESTINATION_STATE"
) \
    .agg(
        F.count("*").alias("flight_count"),
        F.avg("DISTANCE").alias("avg_distance"),
        F.avg("ARRIVAL_DELAY").alias("avg_delay")
    )

# 3. Geographic heatmap data
geo_analysis = flights_airlines_airports_df.groupBy(
    "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", 
    "ORIGIN_CITY", "ORIGIN_STATE"
) \
    .agg(F.count("*").alias("flight_count"))

# Write to Cassandra functions
def write_airline_stats(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="airline_stats", keyspace="flights_db") \
        .save()

def write_route_stats(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="route_stats", keyspace="flights_db") \
        .save()

def write_geo_analysis(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="geo_analysis", keyspace="flights_db") \
        .save()

# Start all streaming queries
query1 = airline_stats.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_airline_stats) \
    .option("checkpointLocation", "/tmp/checkpoint/airline") \
    .start()

query2 = route_stats.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_route_stats) \
    .option("checkpointLocation", "/tmp/checkpoint/route") \
    .start()

query3 = geo_analysis.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_geo_analysis) \
    .option("checkpointLocation", "/tmp/checkpoint/geo") \
    .start()

query3.awaitTermination()