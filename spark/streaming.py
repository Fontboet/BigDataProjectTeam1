from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("flights_stream") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    # .getOrCreate()

# Kafka source (adjust bootstrap if needed: kafka:9092 or kafka:29092)
kafka_flights_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bdsp_topic_test")   # use your topic
    .option("startingOffsets", "earliest")
    .load())

print("Kafka source created")  # debug visible in spark-submit stdout

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

flights_df.printSchema()  # debug: print schema of parsed DataFrame

airport_df = spark.read.csv('data/airports.csv',header=True,inferSchema=True)
airline_df = spark.read.csv('data/airlines.csv',header=True,inferSchema=True)

airport_df.printSchema()  # debug: print schema of airport DataFrame
airline_df.printSchema()  # debug: print schema of airline DataFrame


airline_df=airline_df.withColumnRenamed("AIRLINE","AIRLINES")
# Join the Flights DataFrame with Airlines DataFrame on AIRLINE -> IATA_CODE
flights_airlines_df = flights_df.join(airline_df, flights_df.AIRLINE == airline_df.IATA_CODE, "left")\
    .drop(airline_df.IATA_CODE)  # Drop duplicate IATA_CODE column after join

# Rename columns in airportdf for the first join
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

geo_analysis = flights_airlines_airports_df.groupBy("ORIGIN_LATITUDE", "ORIGIN_LONGITUDE")\
    .agg(F.count("*").alias("flight_count"))\
    .orderBy("flight_count", ascending=False)

write_query = (geo_analysis.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start())

write_query.awaitTermination()