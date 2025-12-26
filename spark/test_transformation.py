from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.functions import from_json, col
import unittest
import json

class TestFlightParsing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestFlightParsing") \
            .master("local[2]") \
            .getOrCreate()
        
        # Define the schema matching streaming.py (All StringType initially)
        cls.raw_schema = StructType([
            StructField("DEPARTURE_DELAY", StringType()),
            StructField("AIRLINE", StringType())
        ])

    def test_parsing_empty_string(self):
        # Simulate JSON with empty string for integer field
        data = json.dumps({"DEPARTURE_DELAY": "", "AIRLINE": "AA"})
        df = self.spark.createDataFrame([(data,)], ["value"])
        
        parsed = df.select(from_json(col("value").cast("string"), self.raw_schema).alias("data")).select("data.*")
        
        # Apply the fix logic: Cast to Integer
        casted = parsed.withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast(IntegerType()))
        
        result = casted.collect()[0]
        self.assertIsNone(result["DEPARTURE_DELAY"])
        self.assertEqual(result["AIRLINE"], "AA")

    def test_parsing_valid_number(self):
        data = json.dumps({"DEPARTURE_DELAY": "15", "AIRLINE": "DL"})
        df = self.spark.createDataFrame([(data,)], ["value"])
        
        parsed = df.select(from_json(col("value").cast("string"), self.raw_schema).alias("data")).select("data.*")
        casted = parsed.withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast(IntegerType()))
        
        result = casted.collect()[0]
        self.assertEqual(result["DEPARTURE_DELAY"], 15)
        self.assertEqual(result["AIRLINE"], "DL")

    def test_parsing_negative_number(self):
        data = json.dumps({"DEPARTURE_DELAY": "-10", "AIRLINE": "UA"})
        df = self.spark.createDataFrame([(data,)], ["value"])
        
        parsed = df.select(from_json(col("value").cast("string"), self.raw_schema).alias("data")).select("data.*")
        casted = parsed.withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast(IntegerType()))
        
        result = casted.collect()[0]
        self.assertEqual(result["DEPARTURE_DELAY"], -10)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
