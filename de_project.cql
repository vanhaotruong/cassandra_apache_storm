DROP KEYSPACE IF EXISTS iotsolution;
  
CREATE KEYSPACE IF NOT EXISTS iotsolution
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

USE iotsolution;

CREATE TABLE wind_brone_table (
    id UUID,
    recorded_date DATE,
    device_id TEXT,
    window TIMESTAMP,
    rpm FLOAT,
    angle FLOAT,
    PRIMARY KEY((device_id), window)
);

CREATE TABLE weather_brone_table (
    id UUID,
    device_id TEXT,
    recorded_date DATE,
    window TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT,
    PRIMARY KEY((device_id), window)
);

CREATE TABLE wind_silver_table (
    id UUID,
    window_hourly TEXT,
    device_id TEXT,
    avg_rpm FLOAT,
    avg_angle FLOAT,
    PRIMARY KEY((device_id), window_hourly)
);

CREATE TABLE weather_silver_table (
    id UUID,
    device_id TEXT,
    window_hourly TEXT,
    avg_temperature FLOAT,
    avg_humidity FLOAT,
    avg_windspeed FLOAT,
    PRIMARY KEY((device_id), window_hourly)
);

CREATE TABLE golden_table (
    id UUID,
    device_id TEXT,
    window_hourly TEXT,
    avg_rpm FLOAT,
    avg_angle FLOAT,
    avg_temperature FLOAT,
    avg_humidity FLOAT,
    avg_windspeed FLOAT,
    PRIMARY KEY((device_id), window_hourly)
);
