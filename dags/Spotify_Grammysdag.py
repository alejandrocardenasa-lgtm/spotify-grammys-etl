import sys
import os
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

    # Task 0: Load Grammys CSV to source DB
    def task_load_grammys_to_db(**context):
        load_grammys_to_db()
        print("Grammys CSV cargado a SQLite")

    load_grammys_db = PythonOperator(
        task_id="load_grammys_to_db",
        python_callable=task_load_grammys_to_db,
    )

    # Task 1: Extract Spotify CSV
    def task_extract_spotify(**context):
        ti = context["ti"]
        os.makedirs("/opt/airflow/data/processed", exist_ok=True)

        spotify_df = extract_spotify_csv()
        spotify_extract_path = "/opt/airflow/data/processed/spotify_extract.csv"
        spotify_df.to_csv(spotify_extract_path, index=False)

        ti.xcom_push(key="spotify_extract_path", value=spotify_extract_path)
        print(f"Spotify extraído: {len(spotify_df)} filas")

    extract_spotify = PythonOperator(
        task_id="extract_spotify",
        python_callable=task_extract_spotify,
    )

    # Task 2: Extract Grammys DB
    def task_extract_grammys(**context):
        ti = context["ti"]
        os.makedirs("/opt/airflow/data/processed", exist_ok=True)

        grammys_df = extract_grammys_db()
        grammys_extract_path = "/opt/airflow/data/processed/grammys_extract.csv"
        grammys_df.to_csv(grammys_extract_path, index=False)

        ti.xcom_push(key="grammys_extract_path", value=grammys_extract_path)
        print(f"Grammys extraído: {len(grammys_df)} filas")

    extract_grammys = PythonOperator(
        task_id="extract_grammys",
        python_callable=task_extract_grammys,
    )

    # Task 3: Clean
    def task_clean(**context):
        ti = context["ti"]

        spotify_extract_path = ti.xcom_pull(task_ids="extract_spotify", key="spotify_extract_path")
        grammys_extract_path = ti.xcom_pull(task_ids="extract_grammys", key="grammys_extract_path")

        spotify_df = pd.read_csv(spotify_extract_path)
        grammys_df = pd.read_csv(grammys_extract_path)

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

    # Task 4: Transform and build dimensional model
    def task_transform(**context):
        ti = context["ti"]

        spotify_clean_path = ti.xcom_pull(task_ids="clean", key="spotify_clean_path")
        grammys_clean_path = ti.xcom_pull(task_ids="clean", key="grammys_clean_path")

        spotify_df = pd.read_csv(spotify_clean_path)
        grammys_df = pd.read_csv(grammys_clean_path)

        # Transformación y merge
        spotify_df, grammys_df, merged_df, artist_level_df = transform_data(spotify_df, grammys_df)

        output_path = "/opt/airflow/data/processed"
        os.makedirs(output_path, exist_ok=True)

        spotify_transformed_path = f"{output_path}/spotify_transformed.csv"
        grammys_transformed_path = f"{output_path}/grammys_transformed.csv"
        merged_path = f"{output_path}/merged_dataset.csv"
        artist_level_path = f"{output_path}/artist_level_dataset.csv"

        spotify_df.to_csv(spotify_transformed_path, index=False)
        grammys_df.to_csv(grammys_transformed_path, index=False)
        merged_df.to_csv(merged_path, index=False)
        artist_level_df.to_csv(artist_level_path, index=False)

        # Construir modelo dimensional
        tables = build_dimensional_model(spotify_df, grammys_df, merged_df)

        # Guardar dimensiones y fact table en CSV
        tables["dim_artist"].to_csv(f"{output_path}/dim_artist.csv", index=False)
        tables["dim_track"].to_csv(f"{output_path}/dim_track.csv", index=False)
        tables["dim_genre"].to_csv(f"{output_path}/dim_genre.csv", index=False)
        tables["dim_award"].to_csv(f"{output_path}/dim_award.csv", index=False)
        tables["dim_date"].to_csv(f"{output_path}/dim_date.csv", index=False)
        tables["fact_music_awards"].to_csv(f"{output_path}/fact_music_awards.csv", index=False)

        # Rutas pequeñas por XCom
        ti.xcom_push(key="merged_path", value=merged_path)
        ti.xcom_push(key="artist_level_path", value=artist_level_path)

        print(f"Transform completado: {len(merged_df)} filas en merged_df")
        print(f"Artist level dataset creado: {len(artist_level_df)} filas")
        print("Modelo dimensional creado y guardado en CSV.")


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
        load_data()
        print("Data Warehouse cargado en PostgreSQL")

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
    )

    # Task 7: Cleanup temporary files
    def task_cleanup_files(**context):
        temp_files = [
            "/opt/airflow/data/processed/spotify_extract.csv",
            "/opt/airflow/data/processed/grammys_extract.csv",
            "/opt/airflow/data/processed/spotify_clean.csv",
            "/opt/airflow/data/processed/grammys_clean.csv",
            "/opt/airflow/data/processed/spotify_transformed.csv",
            "/opt/airflow/data/processed/grammys_transformed.csv",
            "/opt/airflow/data/processed/merged_dataset.csv",
            "/opt/airflow/data/processed/dim_artist.csv",
            "/opt/airflow/data/processed/dim_track.csv",
            "/opt/airflow/data/processed/dim_genre.csv",
            "/opt/airflow/data/processed/dim_award.csv",
            "/opt/airflow/data/processed/dim_date.csv",
            "/opt/airflow/data/processed/fact_music_awards.csv",
        ]

        for file_path in temp_files:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Archivo eliminado: {file_path}")
            else:
                print(f"No existe: {file_path}")

    cleanup_files = PythonOperator(
        task_id="cleanup_files",
        python_callable=task_cleanup_files,
    )

    # Dependencies
    load_grammys_db >> [extract_spotify, extract_grammys] >> clean >> transform >> [load_drive, load_warehouse] >> cleanup_files