CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_readings (
  ts_utc       TIMESTAMP PRIMARY KEY,
  temp_c       DOUBLE,
  rh_pct       DOUBLE,
  ingestion_ts TIMESTAMP
);
