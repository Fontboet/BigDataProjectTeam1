# Hanoi University of Science and Technology
# BigDataProjectTeam1
Big Data Storage and Processing 2025 Project Team 1

# How to Run the Pipeline
Start the stack:
```bash
docker compose up -d
```
Check service status:
```bash
docker compose ps
```
Note :
```bash
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" # can be more readable
```

## Verify the Pipeline
Confirm topic creation:
```bash
docker compose logs kafka-init --no-log-prefix | tail -n 100
```
Validate producer -> Kafka:
```bash
docker compose logs producer --no-log-prefix | tail -n 100
docker compose exec kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic flights_topic --from-beginning --max-messages 3 --property print.value=true"
```
Validate consumer -> Cassandra:
```bash
docker compose exec kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic flights_topic --from-beginning --max-messages 5 --property print.value=true"
```
Start Spark submit (if not auto-started):
```bash
docker compose up -d spark-submit
```
Watch logs:
```bash
docker compose logs spark-submit --no-log-prefix | tail -n 300
docker compose logs spark-submit --no-log-prefix | grep -E "Kafka source created|Writing airline_stats|Writing route_stats|Writing geo_analysis" | tail -n 50
```
Spark shows “Kafka source created” and connector jars downloading/resolved.
Validate Cassandra schema and counts:
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='flights_db';\""
```
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT airline, updated_at, total_flights
FROM flights_db.airline_stats
LIMIT 10;\""
```
Validate route_stats:
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT origin_airport, destination_airport, updated_at, flight_count
FROM flights_db.route_stats
LIMIT 10;\""
```
Validate geo_analysis:
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT origin_city, origin_state, updated_at, flight_count
FROM flights_db.geo_analysis
LIMIT 10;\""
```
Confirm HDFS cluster status :
```bash
docker exec -it namenode hdfs dfsadmin -report
```
Check HDFS Web UI : http://localhost:9870


Grafana:
    - Open http://localhost:3000 (default admin/admin)
    - Install Cassandra datasource plugin is auto-enabled
    - Add datasource: type "Apache Cassandra", contact point `cassandra:9042`, keyspace `flights_db`
    - Create dashboards:
      * Time-series: query editor with SELECT airline, total_flights, updated_at FROM flights_db.airline_stats WHERE updated_at > $__timeFrom AND updated_at < $__timeTo
      * Table: SELECT * FROM flights_db.route_stats LIMIT 20
      * Aggregations: SELECT airline, avg_departure_delay, avg_arrival_delay FROM flights_db.airline_stats
    - Set refresh every 5s to observe streaming updates

## Data Mapping
- CSV -> Kafka: `kafka/kafka_producer.py` reads `data/flights.csv` and sends JSON per row
- Kafka -> Spark schema: `spark/streaming.py` defines fields like `AIRLINE`, `ORIGIN_AIRPORT`, delays, distance
- Joins: airlines by `IATA_CODE`; airports by origin/destination IATA codes
- Cassandra tables:
  * `airline_stats(airline PK, airlines, totals, delays, cancelled_flights, updated_at)`
  * `route_stats((origin_airport, destination_airport) PK, cities/states, counts, avg_distance, avg_delay, updated_at)`
  * `geo_analysis((origin_city, origin_state) PK + coords, flight_count, updated_at)`

## Stop Everything
```bash
docker compose down
```

# Configs
## Kafka
Short guide to choose Kafka producer settings (tradeoffs + examples).

Key principles

Durability vs throughput vs latency: acks and retries impact durability; batch_size and linger_ms impact throughput and latency; compression reduces network at cost of CPU.
Broker config matters: acks='all' requires replication and min.insync.replicas settings. Topic auto-recovery / re-creation can interfere with deletes.
Measure & iterate: benchmark with your payload size and target throughput, monitor producer/broker metrics (request-latency, record-send-rate, batch-size, record-error-rate).
Parameter-by-parameter (quick)

retries: number of retry attempts for transient errors. Use >=5 for production; very large for strong durability. Pair with idempotence if available to avoid duplicates.
acks: "0" = fastest, "1" = leader ack (default), "all" = wait for ISR → best durability. Use "all" in production when you need no data loss.
batch_size (bytes): max size per batch. Larger → fewer requests, better throughput, more memory. Start 64KB–256KB depending on record size.
linger_ms: time to wait to accumulate a batch. 0 = no wait (low latency). 50–200ms for throughput; smaller for low-latency.
compression_type: 'lz4' or 'snappy' typical. LZ4 gives good CPU/throughput; gzip best compression but higher CPU/latency.
flush_every (app-level): how often you call producer.flush() when streaming from files. Frequent flush → less memory and quicker persistence but higher overhead. For bulk load use larger flush_every (e.g., 5k–50k); for streaming/low-loss use small (100–1k).
max_request_size: must be >= largest message size + overhead. Increase if you produce large JSON payloads.
max_in_flight_requests_per_connection: set <=5 (or 1) if using retries without idempotence to avoid reordering; if idempotence enabled the client may require <=5.
Monitoring & testing

Run small benchmarks: measure total time and msg/s for different batch_size/linger_ms combos.
Watch metrics (broker and producer): record-send-rate, request-latency, batch-size, queue-time-ms, outgoing-byte-rate, record-error-rate.
Use kafka-topics --describe to confirm ISR and replication; ensure min.insync.replicas configured if using acks='all'.





