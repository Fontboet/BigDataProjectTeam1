# Hanoi University of Science and Technology
# BigDataProjectTeam1
Big Data Storage and Processing 2025 Project Team 1

# How to Run the Kafka Pipeline

## Step-by-Step Execution Order

### Step 0: Requirements
Start the bash setup file : `setup.sh`

### Step 1: Start Docker Services
Start Kafka, Zookeeper, and MongoDB: 

docker-compose up -d

Wait 10-15 seconds for all services to initialize.

---

### Step 2: Run the Producer
In a terminal, run the producer : `python kafka/kafka_producer.py`

This will send messages from your CSV file to Kafka topic `bdsp_topic_test`.

Let it run for a few seconds, then stop it with `Ctrl+C`.

---

### Step 3: Run the Consumer
In another terminal (keep the first one open if needed):

conda activate datalab
python kafka_consumer_mongodb.py


This consumer will:
- Read messages from Kafka topic `bdsp_topic_test`
- Insert them into MongoDB collection `flights_db.raw_flights`

The consumer will continue running and processing new messages in real-time.

---

### Step 4: Run Spark :
Go to http://localhost:8888. \
Upload the notebook `spark\spark_testing.ipynb` in the online Jupyter Notebook.

---

Alternative
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.cassandra.connection.port=9042 \
  spark/streaming.py
```


## Verify the Pipeline

Check messages in MongoDB:

docker exec -it mongodb_project mongosh -u admin -p password123


Then in MongoDB shell:

use flights_db
db.raw_flights.countDocuments()
db.raw_flights.find().limit(5)


---

## Summary

**Order:** Docker → Producer → Consumer

**Files:**
1. `docker-compose.yml` - Infrastructure (Kafka, Zookeeper, MongoDB)
2. `kafka_producer.py` - Sends data to Kafka
3. `kafka_consumer_mongodb.py` - Reads from Kafka and stores in MongoDB

---

## Stop Everything

Stop consumer: `Ctrl+C`

Stop Docker services:

docker-compose down





