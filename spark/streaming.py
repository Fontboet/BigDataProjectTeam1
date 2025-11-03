from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, broadcast
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("flights_stream").getOrCreate()
# Kafka source
kafka_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "flights.events")
    .option("startingOffsets", "latest")
    .load())

print("OK")
# define schema for flights event (select columns you need)
schema = StructType() \
    .add("FL_DATE", StringType()) \
    .add("OP_CARRIER", StringType()) \
    .add("ORIGIN", StringType()) \
    .add("DEST", StringType()) \
    .add("CRS_DEP_TIME", StringType()) \
    .add("DEP_DELAY", StringType()) \
    .add("ARR_DELAY", StringType()) \
    .add("event_time", StringType()) \
    # add other fields as needed

# parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
# # cast event_time properly
# events = parsed.withColumn("event_time", to_timestamp("event_time"))

# # read lookups (small) and broadcast
# airlines = spark.read.csv("/data/lookups/airlines.csv", header=True)  # or read from Cassandra
# airports = spark.read.csv("/data/lookups/airports.csv", header=True)

# # Example: 30-minute tumbling window average arrival delay per airline
# agg = (events
#        .withWatermark("event_time", "1 hour")
#        .groupBy(window(col("event_time"), "30 minutes"), col("OP_CARRIER"))
#        .agg(
#             expr("avg(cast(ARR_DELAY as double)) as avg_arr_delay"),
#             expr("count(*) as num_flights")
#        ))

# # enrich with airline names using broadcast
# agg_enriched = agg.join(broadcast(airlines), agg.OP_CARRIER == airlines.Code, "left") \
#                   .select("window", "OP_CARRIER", "Name", "avg_arr_delay", "num_flights")

