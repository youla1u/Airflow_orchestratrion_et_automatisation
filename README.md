# 🌦️Pipeline Météo avec Airflow (DuckDB + MinIO)

Ce projet est un **pipeline de données orchestré avec Airflow** qui illustre un cas concret de **data engineering**.  
Il ingère des données météo depuis une API publique, les stocke dans **DuckDB**, applique des transformations SQL, vérifie la qualité, puis exporte les résultats vers **MinIO (S3)**.  

L’objectif est de montrer comment construire un **workflow ETL moderne** avec des technologies légères et open source.

---

## 🚀 Fonctionnalités principales

- **Ingestion** : récupération des données horaires de l’API [Open-Meteo](https://open-meteo.com/).  
- **Stockage brut** : insertion dans DuckDB (`raw.weather_readings`) avec gestion d’upsert via `MERGE`.  
- **Transformation** : agrégations journalières dans `mart.weather_daily`.  
- **Qualité des données** : contrôles de nulls, valeurs aberrantes et plages de validité.  
- **Export** : sauvegarde quotidienne au format CSV dans un bucket S3/MinIO via DuckDB `COPY ... TO`.  
- **Orchestration** : DAG Airflow avec dépendances, retries et gestion des erreurs.   
- **Notifications** : intégration Slack optionnelle.  

---

## 🧱 Architecture

```mermaid
flowchart TD
    A[Extract API Open-Meteo] --> B[DuckDB raw.weather_readings]
    B --> C[Transform -> mart.weather_daily]
    C --> D[Data Quality Checks]
    D --> E[Export CSV -> MinIO S3 Bucket]
    E --> F[Notification Slack/Logs]
