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
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT COUNT(*) FROM flights_db.airline_stats;\""
```
Validate route_stats:
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT COUNT(*) FROM flights_db.route_stats;\""
```
Validate geo_analysis:
```bash
docker compose exec cassandra bash -lc "cqlsh -e \"SELECT COUNT(*) FROM flights_db.geo_analysis;\""
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





