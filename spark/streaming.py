from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, sum as _sum, avg, count, stddev, percentile_approx
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
import pyspark.sql.functions as F
import os
import sys

spark = SparkSession.builder \
    .appName("flights_stream") \
    .config("spark.cassandra.connection.host", os.environ.get("CASSANDRA_HOST", "cassandra")) \
    .config("spark.cassandra.connection.port", os.environ.get("CASSANDRA_PORT", "9042")) \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoint")
# CHECKPOINT_BASE = "hdfs://namenode:8020/checkpoint"

# Test HDFS connectivity
if CHECKPOINT_BASE.startswith("hdfs://"):
    spark.read.text('hdfs://namenode:8020/').count()

# Kafka source
kafka_flights_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "flights_topic"))
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .option("failOnDataLoss", "false")
    .load())

print("Kafka source created")

# Define schema as ALL Strings first to ensure parsing succeeds, then cast
flights_schema = StructType([
    StructField("YEAR", StringType()),
    StructField("MONTH", StringType()),
    StructField("DAY", StringType()),
    StructField("DAY_OF_WEEK", StringType()),
    StructField("AIRLINE", StringType()),
    StructField("FLIGHT_NUMBER", StringType()),
    StructField("TAIL_NUMBER", StringType()),
    StructField("ORIGIN_AIRPORT", StringType()),
    StructField("DESTINATION_AIRPORT", StringType()),
    StructField("SCHEDULED_DEPARTURE", StringType()),
    StructField("DEPARTURE_TIME", StringType()),
    StructField("DEPARTURE_DELAY", StringType()),
    StructField("TAXI_OUT", StringType()),
    StructField("WHEELS_OFF", StringType()),
    StructField("SCHEDULED_TIME", StringType()),
    StructField("ELAPSED_TIME", StringType()),
    StructField("AIR_TIME", StringType()),
    StructField("DISTANCE", StringType()),
    StructField("WHEELS_ON", StringType()),
    StructField("TAXI_IN", StringType()),
    StructField("SCHEDULED_ARRIVAL", StringType()),
    StructField("ARRIVAL_TIME", StringType()),
    StructField("ARRIVAL_DELAY", StringType()),
    StructField("DIVERTED", StringType()),
    StructField("CANCELLED", StringType()),
    StructField("CANCELLATION_REASON", StringType()),
    StructField("AIR_SYSTEM_DELAY", StringType()),
    StructField("SECURITY_DELAY", StringType()),
    StructField("AIRLINE_DELAY", StringType()),
    StructField("LATE_AIRCRAFT_DELAY", StringType()),
    StructField("WEATHER_DELAY", StringType())
])

# Parse JSON
raw_flights_df = kafka_flights_df.select(from_json(col("value").cast("string"), flights_schema).alias("data")).select("data.*")

# Cast to correct types
flights_df = raw_flights_df \
    .withColumn("YEAR", col("YEAR").cast(IntegerType())) \
    .withColumn("MONTH", col("MONTH").cast(IntegerType())) \
    .withColumn("DAY", col("DAY").cast(IntegerType())) \
    .withColumn("DAY_OF_WEEK", col("DAY_OF_WEEK").cast(IntegerType())) \
    .withColumn("FLIGHT_NUMBER", col("FLIGHT_NUMBER").cast(IntegerType())) \
    .withColumn("SCHEDULED_DEPARTURE", col("SCHEDULED_DEPARTURE").cast(IntegerType())) \
    .withColumn("DEPARTURE_TIME", col("DEPARTURE_TIME").cast(IntegerType())) \
    .withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast(IntegerType())) \
    .withColumn("TAXI_OUT", col("TAXI_OUT").cast(IntegerType())) \
    .withColumn("WHEELS_OFF", col("WHEELS_OFF").cast(IntegerType())) \
    .withColumn("SCHEDULED_TIME", col("SCHEDULED_TIME").cast(IntegerType())) \
    .withColumn("ELAPSED_TIME", col("ELAPSED_TIME").cast(IntegerType())) \
    .withColumn("AIR_TIME", col("AIR_TIME").cast(IntegerType())) \
    .withColumn("DISTANCE", col("DISTANCE").cast(IntegerType())) \
    .withColumn("WHEELS_ON", col("WHEELS_ON").cast(IntegerType())) \
    .withColumn("TAXI_IN", col("TAXI_IN").cast(IntegerType())) \
    .withColumn("SCHEDULED_ARRIVAL", col("SCHEDULED_ARRIVAL").cast(IntegerType())) \
    .withColumn("ARRIVAL_TIME", col("ARRIVAL_TIME").cast(IntegerType())) \
    .withColumn("ARRIVAL_DELAY", col("ARRIVAL_DELAY").cast(IntegerType())) \
    .withColumn("DIVERTED", col("DIVERTED").cast(IntegerType())) \
    .withColumn("CANCELLED", col("CANCELLED").cast(IntegerType())) \
    .withColumn("AIR_SYSTEM_DELAY", col("AIR_SYSTEM_DELAY").cast(IntegerType())) \
    .withColumn("SECURITY_DELAY", col("SECURITY_DELAY").cast(IntegerType())) \
    .withColumn("AIRLINE_DELAY", col("AIRLINE_DELAY").cast(IntegerType())) \
    .withColumn("LATE_AIRCRAFT_DELAY", col("LATE_AIRCRAFT_DELAY").cast(IntegerType())) \
    .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast(IntegerType()))

# Create flight timestamp and extract scheduled hour for time series
flights_df = flights_df.withColumn(
    "flight_timestamp",
    F.to_timestamp(
        F.concat_ws(
            "-", 
            col("YEAR"), 
            F.lpad(col("MONTH"), 2, "0"),
            F.lpad(col("DAY"), 2, "0")
        ),
        "yyyy-MM-dd"
    )
).withColumn(
    "scheduled_hour", 
    (col("SCHEDULED_DEPARTURE") / 100).cast(IntegerType())
)

# Watermarking: Handle late-arriving data (2-day tolerance)
flights_df = flights_df.withWatermark("flight_timestamp", "2 days")
print("Watermarking applied: 2 days late data tolerance")

flights_df.printSchema()

# Drop columns not needed for analysis
flights_df = flights_df.drop("YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "FLIGHT_NUMBER", "TAIL_NUMBER", "ARRIVAL_TIME", "DEPARTURE_TIME")

# Load static data
airport_df = spark.read.csv('/app/data/smallcsv/airports.csv', header=True, inferSchema=True).cache()
airline_df = spark.read.csv('/app/data/smallcsv/airlines.csv', header=True, inferSchema=True).cache()
airport_df.count()
airline_df.count()

airline_df = airline_df.withColumnRenamed("AIRLINE", "AIRLINES")

# Join Logic
flights_airlines_df = flights_df.join(airline_df, flights_df.AIRLINE == airline_df.IATA_CODE, "left") \
    .drop(airline_df.IATA_CODE)

airport_origin_df = airport_df.withColumnRenamed("IATA_CODE", "ORIGIN_AIRPORT_CODE") \
    .withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME") \
    .withColumnRenamed("CITY", "ORIGIN_CITY") \
    .withColumnRenamed("STATE", "ORIGIN_STATE") \
    .withColumnRenamed("COUNTRY", "ORIGIN_COUNTRY") \
    .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE") \
    .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")

flights_airlines_airports_df = flights_airlines_df.join(
    airport_origin_df,
    flights_airlines_df.ORIGIN_AIRPORT == airport_origin_df.ORIGIN_AIRPORT_CODE,
    "left"
)

airport_destination_df = airport_df.withColumnRenamed("IATA_CODE", "DESTINATION_AIRPORT_CODE") \
    .withColumnRenamed("AIRPORT", "DESTINATION_AIRPORT_NAME") \
    .withColumnRenamed("CITY", "DESTINATION_CITY") \
    .withColumnRenamed("STATE", "DESTINATION_STATE") \
    .withColumnRenamed("COUNTRY", "DESTINATION_COUNTRY") \
    .withColumnRenamed("LATITUDE", "DESTINATION_LATITUDE") \
    .withColumnRenamed("LONGITUDE", "DESTINATION_LONGITUDE") \
    .withColumnRenamed("AIRLINE", "AIRLINES")

flights_airlines_airports_df = flights_airlines_airports_df.join(
    airport_destination_df,
    flights_airlines_airports_df.DESTINATION_AIRPORT == airport_destination_df.DESTINATION_AIRPORT_CODE,
    "left"
)

airline_stats = flights_airlines_airports_df \
    .filter(col("AIRLINE").isNotNull()) \
    .groupBy("AIRLINE") \
    .agg(
        _sum(when(col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights"),
        _sum(when((col("CANCELLED") == 0) & (col("DEPARTURE_DELAY") <= 15), 1).otherwise(0)).alias("on_time_flights"),
        _sum(when((col("CANCELLED") == 0) & (col("DEPARTURE_DELAY") > 15), 1).otherwise(0)).alias("delayed_flights"),
        avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
        avg("ARRIVAL_DELAY").alias("avg_arrival_delay")
    )

airline_stats_out = airline_stats.select(
    col("AIRLINE").alias("airline"),
    col("cancelled_flights"),
    col("on_time_flights"),
    col("delayed_flights"),
    col("avg_departure_delay"),
    col("avg_arrival_delay")
)

delay_cols = ["AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]

delay_df = flights_airlines_airports_df.filter(
    (col("AIR_SYSTEM_DELAY") > 0) | 
    (col("SECURITY_DELAY") > 0) | 
    (col("AIRLINE_DELAY") > 0) | 
    (col("LATE_AIRCRAFT_DELAY") > 0) | 
    (col("WEATHER_DELAY") > 0)
)

stack_expr = "stack(5, " + ", ".join([f"'{c}', {c}" for c in delay_cols]) + ") as (delay_reason, duration)"

delay_unpivoted = delay_df.selectExpr(stack_expr) \
    .filter(col("duration") > 0)

delay_stats = delay_unpivoted \
    .groupBy("delay_reason") \
    .agg(
        count("*").alias("count"),
        avg("duration").alias("avg_duration")
    )

delay_stats_out = delay_stats.select(
    col("delay_reason"),
    col("count"),
    col("avg_duration")
)

route_stats = flights_airlines_airports_df \
    .filter(col("ORIGIN_AIRPORT").isNotNull() & col("DESTINATION_AIRPORT").isNotNull()) \
    .groupBy(
        "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
        "ORIGIN_CITY", "ORIGIN_STATE", "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE",
        "DESTINATION_CITY", "DESTINATION_STATE", "DESTINATION_LATITUDE", "DESTINATION_LONGITUDE"
    ) \
    .agg(
        avg("ARRIVAL_DELAY").alias("avg_delay")
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
    col("DESTINATION_LATITUDE").alias("destination_latitude"),
    col("DESTINATION_LONGITUDE").alias("destination_longitude"),
    col("avg_delay")
)

# =============================================================================
# HOURLY TIME SERIES ANALYSIS
# =============================================================================
hourly_stats = flights_airlines_airports_df \
    .filter(
        col("scheduled_hour").isNotNull() & 
        (col("scheduled_hour") >= 0) & 
        (col("scheduled_hour") <= 23)
    ) \
    .groupBy("scheduled_hour") \
    .agg(
        count("*").alias("flight_count"),
        avg("DEPARTURE_DELAY").alias("avg_delay"),
        stddev("DEPARTURE_DELAY").alias("stddev_delay"),
        percentile_approx("DEPARTURE_DELAY", 0.5).alias("median_delay"),
        percentile_approx("DEPARTURE_DELAY", 0.95).alias("p95_delay"),
        _sum(when(col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_count"),
        _sum(when(col("DEPARTURE_DELAY") > 15, 1).otherwise(0)).alias("delayed_count")
    )

hourly_stats_out = hourly_stats.select(
    col("scheduled_hour"),
    col("flight_count"),
    col("avg_delay"),
    col("stddev_delay"),
    col("median_delay"),
    col("p95_delay"),
    col("cancelled_count"),
    col("delayed_count")
)

# =============================================================================
# WRITE FUNCTIONS
# =============================================================================

def check_data_quality(batch_df, batch_id):
    total = batch_df.count()
    if total > 0:
        null_airline = batch_df.filter(col("AIRLINE").isNull()).count()
        null_delay = batch_df.filter(col("DEPARTURE_DELAY").isNull()).count()
        print(f"DQ Check Batch {batch_id}: Total={total}, NullAirline={null_airline}, NullDepDelay={null_delay}")   
        print("Sample Data:")
        batch_df.select("AIRLINE", "DEPARTURE_DELAY", "scheduled_hour").show(5, truncate=False)
        sys.stdout.flush()
    else:
        print(f"DQ Check Batch {batch_id}: Empty batch")
        sys.stdout.flush()

def write_airline_stats(batch_df, batch_id):
    try:
        if batch_df.isEmpty(): return
        print(f"Writing airline_stats batch {batch_id} - {batch_df.count()} rows")
        sys.stdout.flush()
        batch_df = batch_df.withColumn("updated_at", current_timestamp())
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="airline_stats", keyspace="flights_db") \
            .option("spark.cassandra.output.consistency.level", "ONE") \
            .save()
    except Exception as e:
        print(f"Error writing airline_stats: {e}")
        sys.stdout.flush()

def write_delay_stats(batch_df, batch_id):

    try:
        if batch_df.isEmpty(): return        
        print(f"Writing delay_stats batch {batch_id} - {batch_df.count()} rows")
        sys.stdout.flush()
        batch_df = batch_df.withColumn("updated_at", current_timestamp())
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="delay_by_reason", keyspace="flights_db") \
            .option("spark.cassandra.output.consistency.level", "ONE") \
            .save()
    except Exception as e:
        print(f"Error writing delay_stats: {e}")
        sys.stdout.flush()

def write_route_stats(batch_df, batch_id):
    try:
        if batch_df.isEmpty(): return
        print(f"Writing route_stats batch {batch_id} - {batch_df.count()} rows")
        sys.stdout.flush()
        batch_df = batch_df.withColumn("updated_at", current_timestamp())
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="route_stats", keyspace="flights_db") \
            .option("spark.cassandra.output.consistency.level", "ONE") \
            .save()
    except Exception as e:
        print(f"Error writing route_stats: {e}")
        sys.stdout.flush()

def write_hourly_stats(batch_df, batch_id):
    try:
        if batch_df.isEmpty(): return
        print(f"Writing hourly_stats batch {batch_id} - {batch_df.count()} rows")
        sys.stdout.flush()
        batch_df = batch_df.withColumn("updated_at", current_timestamp())
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="hourly_stats", keyspace="flights_db") \
            .option("spark.cassandra.output.consistency.level", "ONE") \
            .save()
    except Exception as e:
        print(f"Error writing hourly_stats: {e}")
        sys.stdout.flush()

# =============================================================================
# START STREAMING QUERIES WITH CHECKPOINTING
# =============================================================================
print("Starting streaming queries with checkpointing for exactly-once semantics...")

# Data quality monitoring
dq_query = flights_df.writeStream \
    .foreachBatch(check_data_quality) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/dq") \
    .trigger(processingTime="10 seconds") \
    .start()

# Airline stats
query1 = airline_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_airline_stats) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/airline_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

# Delay stats
query2 = delay_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_delay_stats) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/delay_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

# Route stats
query3 = route_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_route_stats) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/route_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

# Hourly time series stats
query4 = hourly_stats_out.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_hourly_stats) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/hourly_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

# # Archival writings of the enriched flight data to HDFS
# # Commented because too unstable to be used in "production"
# try:
#     if CHECKPOINT_BASE.startswith("hdfs://"):
#         query5 = (
#             flights_airlines_airports_df.writeStream
#             .outputMode("append")
#             .format("parquet")
#             .option("path", "hdfs://namenode:8020/flights")
#             .option("checkpointLocation", f"{CHECKPOINT_BASE}/flights")
#             .trigger(processingTime="10 seconds")
#             .start()
#         )
#         print("Streaming query started successfully.")
# except Exception as e:
#     print(f"Error when trying to start query : {e}")

print(f"Started {len(spark.streams.active)} streaming queries")
spark.streams.awaitAnyTermination()
