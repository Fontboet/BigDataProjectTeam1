CREATE KEYSPACE IF NOT EXISTS flights_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS flights_db.airline_stats;
CREATE TABLE flights_db.airline_stats (
    airline text PRIMARY KEY,
    cancelled_flights int,
    on_time_flights int,
    delayed_flights int,
    avg_departure_delay float,
    avg_arrival_delay float,
    updated_at timestamp
);

DROP TABLE IF EXISTS flights_db.delay_by_reason;
CREATE TABLE flights_db.delay_by_reason (
    delay_reason text PRIMARY KEY,
    count int,
    avg_duration float,
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
    original_latitude float,
    original_longitude float,
    avg_delay float,
    updated_at timestamp,
    PRIMARY KEY ((original_airport, destination_airport))
);

CREATE INDEX IF NOT EXISTS ON flights_db.route_stats (original_city);
CREATE INDEX IF NOT EXISTS ON flights_db.route_stats (destination_city);
