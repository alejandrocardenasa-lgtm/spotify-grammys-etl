import pandas as pd
from sqlalchemy import create_engine, text


def load_data():
    input_path = "/opt/airflow/data/processed"

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/music_dw"
    )

    with engine.begin() as conn:
        # Truncar tablas del modelo dimensional
        conn.execute(text("TRUNCATE TABLE fact_music_awards CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_date CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_award CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_genre CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_track CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_artist CASCADE;"))

        # Si existen tablas analíticas auxiliares, borrarlas y recrearlas con to_sql
        conn.execute(text("DROP TABLE IF EXISTS merged_music_data CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS artist_level_data CASCADE;"))

    # Cargar dimensiones
    dim_artist = pd.read_csv(f"{input_path}/dim_artist.csv")
    dim_artist.to_sql("dim_artist", engine, if_exists="append", index=False, chunksize=1000)
    del dim_artist

    dim_track = pd.read_csv(f"{input_path}/dim_track.csv")
    dim_track.to_sql("dim_track", engine, if_exists="append", index=False, chunksize=1000)
    del dim_track

    dim_genre = pd.read_csv(f"{input_path}/dim_genre.csv")
    dim_genre.to_sql("dim_genre", engine, if_exists="append", index=False, chunksize=1000)
    del dim_genre

    dim_award = pd.read_csv(f"{input_path}/dim_award.csv")
    dim_award.to_sql("dim_award", engine, if_exists="append", index=False, chunksize=1000)
    del dim_award

    dim_date = pd.read_csv(f"{input_path}/dim_date.csv")
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False, chunksize=1000)
    del dim_date

    # Cargar fact table
    fact_music_awards = pd.read_csv(f"{input_path}/fact_music_awards.csv")
    fact_music_awards.to_sql("fact_music_awards", engine, if_exists="append", index=False, chunksize=1000)
    del fact_music_awards

    # Cargar dataset merged a nivel track
    merged_music_data = pd.read_csv(f"{input_path}/merged_dataset.csv")
    merged_music_data.to_sql("merged_music_data", engine, if_exists="replace", index=False, chunksize=1000)
    del merged_music_data

    # Cargar dataset agregado a nivel artista
    artist_level_data = pd.read_csv(f"{input_path}/artist_level_dataset.csv")
    artist_level_data.to_sql("artist_level_data", engine, if_exists="replace", index=False, chunksize=1000)
    del artist_level_data

    engine.dispose()
    print("Data Warehouse cargado correctamente en PostgreSQL.")