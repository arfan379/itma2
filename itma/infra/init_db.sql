-- Initialize Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Enable TimescaleDB extension if available
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Bronze Tables (Raw Layer)

-- 1. GTFS Routes
CREATE TABLE IF NOT EXISTS raw.raw_gtfs_routes (
    route_id         TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name  TEXT,
    route_type       INTEGER,
    feed_version     TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 2. GTFS Stops
CREATE TABLE IF NOT EXISTS raw.raw_gtfs_stops (
    stop_id      TEXT PRIMARY KEY,
    stop_name    TEXT,
    stop_lat     DOUBLE PRECISION,
    stop_lon     DOUBLE PRECISION,
    feed_version TEXT,
    ingested_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 3. GTFS Trips
CREATE TABLE IF NOT EXISTS raw.raw_gtfs_trips (
    trip_id       TEXT PRIMARY KEY,
    route_id      TEXT,
    service_id    TEXT,
    trip_headsign TEXT,
    direction_id  INTEGER,
    feed_version  TEXT,
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

-- 4. GTFS Stop Times (arrival_time & departure_time as TEXT to allow > 24:00:00)
CREATE TABLE IF NOT EXISTS raw.raw_gtfs_stop_times (
    trip_id        TEXT,
    stop_sequence  INTEGER,
    stop_id        TEXT,
    arrival_time   TEXT,
    departure_time TEXT,
    feed_version   TEXT,
    ingested_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trip_id, stop_sequence)
);

-- 5. Simulated Actual Schedules
CREATE TABLE IF NOT EXISTS raw.raw_schedules (
    trip_id                TEXT,
    route_id               TEXT,
    stop_id                TEXT,
    service_date           DATE,
    scheduled_arrival_time TEXT,
    actual_arrival_time    TIMESTAMPTZ,
    is_synthetic           BOOLEAN DEFAULT TRUE,
    ingested_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trip_id, stop_id, service_date)
);

-- 6. Simulated Traffic Telemetry
CREATE TABLE IF NOT EXISTS raw.raw_traffic (
    segment_id           TEXT NOT NULL,
    event_time           TIMESTAMPTZ NOT NULL,
    speed_kmh            DOUBLE PRECISION NOT NULL,
    free_flow_speed_kmh  DOUBLE PRECISION NOT NULL,
    is_synthetic         BOOLEAN DEFAULT TRUE,
    ingested_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (segment_id, event_time)
);

-- Convert raw.raw_traffic to Timescale hypertable if timescaledb is active
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable('raw.raw_traffic', 'event_time', if_not_exists => TRUE);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping create_hypertable: %', SQLERRM;
END $$;
