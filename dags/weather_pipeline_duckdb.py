
from datetime import datetime, timedelta
import io
import json
import os
import pandas as pd
import requests
import duckdb

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

# -----------------------
# Paramètres & helpers
# -----------------------
DEFAULT_ARGS = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)}
LOCATION = {"lat": 48.8566, "lon": 2.3522}  # Paris

# Emplacement du fichier DuckDB (volume monté)
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/duckdb/data.duckdb")

# S3/MinIO export
S3_BUCKET = "data-lake"
S3_PREFIX = "weather/export"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")

SCHEMA_RAW = "raw"
SCHEMA_MART = "mart"

def get_conn(configure_s3: bool = False):
    """Ouvre une connexion DuckDB et charge httpfs si besoin."""
    con = duckdb.connect(DUCKDB_PATH)
    if configure_s3:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # Config S3/MinIO pour DuckDB
        con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://','').replace('https://','')}';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=false;")
        con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    return con

with DAG(
    dag_id="weather_pipeline_duckdb",
    description="Ingestion météo -> DuckDB -> Transform -> QA -> Export -> Notif",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # tous les jours à 06:00
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["weather", "duckdb", "demo", "portfolio"],
    max_active_runs=1,
) as dag:

    @task
    def ensure_tables():
        con = get_conn()
        with open("/opt/airflow/sql/create_raw.sql") as f:
            con.execute(f.read())
        with open("/opt/airflow/sql/create_mart.sql") as f:
            con.execute(f.read())
        con.close()

    @task(retries=3, retry_delay=timedelta(minutes=2), sla=timedelta(minutes=20))
    def extract_weather():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={LOCATION['lat']}&longitude={LOCATION['lon']}"
            "&hourly=temperature_2m,relative_humidity_2m&timezone=UTC"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        payload = r.json()
        hours = payload["hourly"]["time"]
        temps = payload["hourly"]["temperature_2m"]
        rh = payload["hourly"]["relative_humidity_2m"]
        df = pd.DataFrame(
            {"ts_utc": pd.to_datetime(hours), "temp_c": temps, "rh_pct": rh}
        )
        df["ingestion_ts"] = pd.Timestamp.utcnow()
        # On sérialise pour passer entre tasks
        return df.to_json(orient="records", date_format="iso")

    @task
    def load_raw(json_records: str):
        df = pd.DataFrame(json.loads(json_records))
        # DuckDB : on enregistre un view temporaire depuis pandas, puis MERGE
        con = get_conn()
        con.register("staging_df", df)
        con.execute(f"""
            CREATE TEMP TABLE staging AS
            SELECT CAST(ts_utc AS TIMESTAMP) AS ts_utc,
                   CAST(temp_c AS DOUBLE)     AS temp_c,
                   CAST(rh_pct AS DOUBLE)     AS rh_pct,
                   CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts
            FROM staging_df;
        """)
        con.execute(f"""
            MERGE INTO {SCHEMA_RAW}.weather_readings t
            USING staging s
            ON t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET
                temp_c = s.temp_c,
                rh_pct = s.rh_pct,
                ingestion_ts = s.ingestion_ts
            WHEN NOT MATCHED THEN INSERT (ts_utc, temp_c, rh_pct, ingestion_ts)
            VALUES (s.ts_utc, s.temp_c, s.rh_pct, s.ingestion_ts);
        """)
        con.close()

    @task
    def transform_to_mart():
        con = get_conn()
        con.execute(f"""
            CREATE TEMP TABLE agg AS
            SELECT DATE(ts_utc) AS day,
                   MIN(temp_c)  AS temp_min_c,
                   MAX(temp_c)  AS temp_max_c,
                   AVG(temp_c)  AS temp_avg_c,
                   AVG(rh_pct)  AS rh_avg_pct
            FROM {SCHEMA_RAW}.weather_readings
            GROUP BY 1;
        """)
        con.execute(f"""
            MERGE INTO {SCHEMA_MART}.weather_daily t
            USING agg s
            ON t.day = s.day
            WHEN MATCHED THEN UPDATE SET
              temp_min_c = s.temp_min_c,
              temp_max_c = s.temp_max_c,
              temp_avg_c = s.temp_avg_c,
              rh_avg_pct = s.rh_avg_pct
            WHEN NOT MATCHED THEN INSERT (day, temp_min_c, temp_max_c, temp_avg_c, rh_avg_pct)
            VALUES (s.day, s.temp_min_c, s.temp_max_c, s.temp_avg_c, s.rh_avg_pct);
        """)
        con.close()

    @task
    def data_quality_checks():
        con = get_conn()
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {SCHEMA_RAW}.weather_readings WHERE temp_c IS NULL OR rh_pct IS NULL;"
        ).fetchone()[0]
        bad_temp = con.execute(
            f"SELECT COUNT(*) FROM {SCHEMA_RAW}.weather_readings WHERE temp_c < -80 OR temp_c > 65;"
        ).fetchone()[0]
        bad_rh = con.execute(
            f"SELECT COUNT(*) FROM {SCHEMA_RAW}.weather_readings WHERE rh_pct < 0 OR rh_pct > 100;"
        ).fetchone()[0]
        con.close()
        assert nulls == 0, f"Nulls détectés: {nulls}"
        assert bad_temp == 0, f"Températures hors plage: {bad_temp}"
        assert bad_rh == 0, f"Humidité hors plage: {bad_rh}"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def export_latest_to_s3():
        con = get_conn(configure_s3=True)
        last_day = con.execute(f"SELECT MAX(day) FROM {SCHEMA_MART}.weather_daily;").fetchone()[0]
        if last_day is None:
            con.close()
            return "no_data"
        # Export CSV vers MinIO via httpfs (DuckDB COPY)
        s3_key = f"{S3_PREFIX}/day={last_day}/weather_daily.csv"
        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
        con.execute(f"""
            COPY (SELECT * FROM {SCHEMA_MART}.weather_daily WHERE day = DATE '{last_day}')
            TO '{s3_uri}' WITH (HEADER, DELIMITER ',');
        """)
        con.close()
        return s3_uri

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify(result: str):
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
            hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
            hook.send(text=f"✅ weather_pipeline_duckdb terminé. Export: {result}")
        except Exception as e:
            print(f"[notify] {result} (Slack non configuré: {e})")

    # Orchestration
    t0 = ensure_tables()
    raw_json = extract_weather()
    t1 = load_raw(raw_json)
    t2 = transform_to_mart()
    t3 = data_quality_checks()
    t4 = export_latest_to_s3()
    t5 = notify(t4)
    chain(t0, raw_json, t1, t2, t3, t4, t5)
