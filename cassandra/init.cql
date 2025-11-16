-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS flights_db
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flights_db;

-- Table 1: Airline Statistics
CREATE TABLE IF NOT EXISTS airline_stats (
    airline TEXT,
    airlines TEXT,
    total_flights BIGINT,
    avg_departure_delay DOUBLE,
    avg_arrival_delay DOUBLE,
    cancelled_flights BIGINT,
    PRIMARY KEY (airline)
);

-- Table 2: Route Statistics
CREATE TABLE IF NOT EXISTS route_stats (
    origin_airport TEXT,
    destination_airport TEXT,
    origin_city TEXT,
    origin_state TEXT,
    destination_city TEXT,
    destination_state TEXT,
    flight_count BIGINT,
    avg_distance DOUBLE,
    avg_delay DOUBLE,
    PRIMARY KEY ((origin_airport, destination_airport))
);

-- Table 3: Geographic Analysis
CREATE TABLE IF NOT EXISTS geo_analysis (
    origin_latitude DOUBLE,
    origin_longitude DOUBLE,
    origin_city TEXT,
    origin_state TEXT,
    flight_count BIGINT,
    PRIMARY KEY ((origin_latitude, origin_longitude))
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_airline_name ON airline_stats (airlines);
CREATE INDEX IF NOT EXISTS idx_origin_city ON route_stats (origin_city);
CREATE INDEX IF NOT EXISTS idx_dest_city ON route_stats (destination_city);
CREATE INDEX IF NOT EXISTS idx_geo_city ON geo_analysis (origin_city);