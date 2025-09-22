CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.weather_daily (
  day         DATE PRIMARY KEY,
  temp_min_c  DOUBLE,
  temp_max_c  DOUBLE,
  temp_avg_c  DOUBLE,
  rh_avg_pct  DOUBLE
);
