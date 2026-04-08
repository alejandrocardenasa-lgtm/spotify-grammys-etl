import sys
import os
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Apunta al directorio src montado en el contenedor
sys.path.insert(0, "/opt/airflow/src")

from extract import extract_spotify_csv, extract_grammys_db
from clean import clean_spotify_data, clean_grammys_data
from transform import transform_data
from dimensional_model import build_dimensional_model
from load import load_data
from load_grammys_to_db import load_grammys_to_db
from upload_to_drive import upload_csv_to_drive

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="spotify_grammys_etl",
    default_args=default_args,
    description="ETL pipeline: Spotify + Grammys → Data Warehouse",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "spotify", "grammys"],
) as dag:

    # Task 0: Load Grammys CSV - DB
    def task_load_grammys_to_db(**context):
        load_grammys_to_db()
        print("Grammys CSV cargado a SQLite")

    load_grammys_db = PythonOperator(
        task_id="load_grammys_to_db",
        python_callable=task_load_grammys_to_db,
    )

    # Task 1: Extract Spotify CSV
    def task_extract_spotify(**context):
        spotify_df = extract_spotify_csv()
        context["ti"].xcom_push(key="spotify_df", value=spotify_df.to_json())
        print(f"Spotify extraído: {len(spotify_df)} filas")

    extract_spotify = PythonOperator(
        task_id="extract_spotify",
        python_callable=task_extract_spotify,
    )

    # Task 2: Extract Grammys DB
    def task_extract_grammys(**context):
        grammys_df = extract_grammys_db()
        context["ti"].xcom_push(key="grammys_df", value=grammys_df.to_json())
        print(f"Grammys extraído: {len(grammys_df)} filas")

    extract_grammys = PythonOperator(
        task_id="extract_grammys",
        python_callable=task_extract_grammys,
    )

    # Task 3: Clean
    def task_clean(**context):
        ti = context["ti"]

        spotify_json = ti.xcom_pull(task_ids="extract_spotify", key="spotify_df")
        grammys_json = ti.xcom_pull(task_ids="extract_grammys", key="grammys_df")

        spotify_df = pd.read_json(StringIO(spotify_json))
        grammys_df = pd.read_json(StringIO(grammys_json))

        spotify_df = clean_spotify_data(spotify_df)
        grammys_df = clean_grammys_data(grammys_df)

        os.makedirs("/opt/airflow/data/processed", exist_ok=True)

        spotify_clean_path = "/opt/airflow/data/processed/spotify_clean.csv"
        grammys_clean_path = "/opt/airflow/data/processed/grammys_clean.csv"

        spotify_df.to_csv(spotify_clean_path, index=False)
        grammys_df.to_csv(grammys_clean_path, index=False)

        ti.xcom_push(key="spotify_clean_path", value=spotify_clean_path)
        ti.xcom_push(key="grammys_clean_path", value=grammys_clean_path)

        print(f"Limpieza completada — Spotify: {len(spotify_df)} filas | Grammys: {len(grammys_df)} filas")

    clean = PythonOperator(
        task_id="clean",
        python_callable=task_clean,
    )

    # Task 4: Transform and Merge
    def task_transform(**context):
        ti = context["ti"]

        spotify_clean_path = ti.xcom_pull(task_ids="clean", key="spotify_clean_path")
        grammys_clean_path = ti.xcom_pull(task_ids="clean", key="grammys_clean_path")

        spotify_df = pd.read_csv(spotify_clean_path)
        grammys_df = pd.read_csv(grammys_clean_path)

        spotify_df, grammys_df, merged_df = transform_data(spotify_df, grammys_df)

        spotify_transformed_path = "/opt/airflow/data/processed/spotify_transformed.csv"
        grammys_transformed_path = "/opt/airflow/data/processed/grammys_transformed.csv"
        merged_path = "/opt/airflow/data/processed/merged_dataset.csv"

        spotify_df.to_csv(spotify_transformed_path, index=False)
        grammys_df.to_csv(grammys_transformed_path, index=False)
        merged_df.to_csv(merged_path, index=False)

        ti.xcom_push(key="spotify_path", value=spotify_transformed_path)
        ti.xcom_push(key="grammys_path", value=grammys_transformed_path)
        ti.xcom_push(key="merged_path", value=merged_path)

        print(f"Transform completado: {len(merged_df)} filas en merged_df")

    transform = PythonOperator(
        task_id="transform_merge",
        python_callable=task_transform,
    )

    # Task 5: Load to Google Drive
    def task_load_drive(**context):
        ti = context["ti"]

        merged_path = ti.xcom_pull(task_ids="transform_merge", key="merged_path")

        upload_csv_to_drive(merged_path)
        print("CSV subido a Google Drive")

    load_drive = PythonOperator(
        task_id="load_google_drive",
        python_callable=task_load_drive,
    )

    # Task 6: Load to Data Warehouse
    def task_load_warehouse(**context):
        ti = context["ti"]

        spotify_path = ti.xcom_pull(task_ids="transform_merge", key="spotify_path")
        grammys_path = ti.xcom_pull(task_ids="transform_merge", key="grammys_path")
        merged_path = ti.xcom_pull(task_ids="transform_merge", key="merged_path")

        spotify_df = pd.read_csv(spotify_path)
        grammys_df = pd.read_csv(grammys_path)
        merged_df = pd.read_csv(merged_path)

        tables = build_dimensional_model(spotify_df, grammys_df, merged_df)
        load_data(merged_df, tables)

        print("Data Warehouse cargado en SQLite")

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
    )

    # Dependencies
    load_grammys_db >> [extract_spotify, extract_grammys] >> clean >> transform >> [load_drive, load_warehouse]