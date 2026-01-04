CREATE KEYSPACE IF NOT EXISTS flights_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS flights_db.airline_stats;
CREATE TABLE flights_db.airline_stats (
    airline text PRIMARY KEY,
    cancelled_flights bigint,
    on_time_flights bigint,
    delayed_flights bigint,
    avg_departure_delay double,
    avg_arrival_delay double,
    updated_at timestamp
);

DROP TABLE IF EXISTS flights_db.delay_by_reason;
CREATE TABLE flights_db.delay_by_reason (
    delay_reason text PRIMARY KEY,
    count bigint,
    avg_duration double,
    updated_at timestamp
);

DROP TABLE IF EXISTS flights_db.route_stats;
CREATE TABLE flights_db.route_stats (
    original_airport text,
    destination_airport text,
    original_city text,
    original_state text,
    destination_city text,
    destination_state text,
    original_latitude double,
    original_longitude double,
    destination_latitude double,
    destination_longitude double,
    avg_delay double,
    updated_at timestamp,
    PRIMARY KEY ((original_airport, destination_airport))
);

CREATE INDEX IF NOT EXISTS ON flights_db.route_stats (original_city);
CREATE INDEX IF NOT EXISTS ON flights_db.route_stats (destination_city);

DROP TABLE IF EXISTS flights_db.hourly_delay_trend;

DROP TABLE IF EXISTS flights_db.hourly_stats;
CREATE TABLE flights_db.hourly_stats (
    scheduled_hour int PRIMARY KEY,
    flight_count bigint,
    avg_delay double,
    stddev_delay double,
    median_delay double,
    p95_delay double,
    cancelled_count bigint,
    delayed_count bigint,
    updated_at timestamp
);

