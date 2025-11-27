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

## Verify the Pipeline
Confirm topic creation:
```bash
docker compose logs kafka-init --no-log-prefix | tail -n 100
```
Validate producer -> Kafka:
```bash
docker compose logs producer --no-log-prefix | tail -n 100
```
Validate consumer -> Cassandra:
```bash
docker compose exec kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic bdsp_topic_test --from-beginning --max-messages 5 --property print.value=true"
```
Start Spark submit (if not auto-started):
```bash
docker compose up -d spark-submit
```
Watch logs:
```bash
docker compose logs spark-submit --no-log-prefix | tail -n 300
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
Grafana:
    - Open http://localhost:3000 (default admin/admin)

## Stop Everything
```bash
docker compose down
```





