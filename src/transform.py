import pandas as pd

def transform_spotify_data(spotify_df):
    spotify_df = spotify_df.copy()

    # Estandarizar texto
    spotify_df["track_id"] = spotify_df["track_id"].astype(str).str.lower().str.strip()
    spotify_df["artists"] = spotify_df["artists"].astype(str).str.lower().str.strip()
    spotify_df["album_name"] = spotify_df["album_name"].astype(str).str.lower().str.strip()
    spotify_df["track_name"] = spotify_df["track_name"].astype(str).str.lower().str.strip()
    spotify_df["track_genre"] = spotify_df["track_genre"].astype(str).str.lower().str.strip()

    # Convertir explicit a entero
    spotify_df["explicit"] = spotify_df["explicit"].astype(int)

    # Crear columna clave para cruce
    spotify_df["merge_artist"] = spotify_df["artists"]

    return spotify_df


def transform_grammys_data(grammys_df):
    grammys_df = grammys_df.copy()

    # Estandarizar texto
    grammys_df["title"] = grammys_df["title"].astype(str).str.lower().str.strip()
    grammys_df["category"] = grammys_df["category"].astype(str).str.lower().str.strip()
    grammys_df["nominee"] = grammys_df["nominee"].astype(str).str.lower().str.strip()
    grammys_df["artist"] = grammys_df["artist"].astype(str).str.lower().str.strip()
    grammys_df["workers"] = grammys_df["workers"].astype(str).str.lower().str.strip()

    # Convertir fechas
    grammys_df["published_at"] = pd.to_datetime(
        grammys_df["published_at"], errors="coerce", utc=True
    )
    grammys_df["updated_at"] = pd.to_datetime(
        grammys_df["updated_at"], errors="coerce", utc=True
    )

    # Crear año
    grammys_df["award_year"] = grammys_df["published_at"].dt.year

    # Convertir fechas a string para el merge con dim_date
    grammys_df["published_at"] = grammys_df["published_at"].astype(str)
    grammys_df["updated_at"] = grammys_df["updated_at"].astype(str)

    # Reemplazar fechas nulas
    grammys_df["published_at"] = grammys_df["published_at"].replace("NaT", "no_date")
    grammys_df["updated_at"] = grammys_df["updated_at"].replace("NaT", "no_date")
    grammys_df["award_year"] = grammys_df["award_year"].fillna(0)

    # Convertir winner a entero
    grammys_df["winner"] = pd.to_numeric(
        grammys_df["winner"], errors="coerce"
    ).fillna(0).astype(int)

    # Crear columna clave para cruce
    grammys_df["merge_artist"] = grammys_df["artist"]

    return grammys_df


def merge_datasets(spotify_df, grammys_df):
    spotify_df = spotify_df.copy()
    grammys_df = grammys_df.copy()

    # Dejar solo columnas necesarias de Grammys
    grammys_clean = grammys_df[
        ["merge_artist", "category", "nominee", "workers", "winner", "award_year", "published_at", "updated_at"]
    ].drop_duplicates()

    # Merge por artista
    merged_df = pd.merge(
        spotify_df,
        grammys_clean,
        on="merge_artist",
        how="left",
        suffixes=("_spotify", "_grammys")
    )

    # Completar nulos
    merged_df["winner"] = merged_df["winner"].fillna(0).astype(int)
    merged_df["category"] = merged_df["category"].fillna("no_award")
    merged_df["nominee"] = merged_df["nominee"].fillna("no_nominee")
    merged_df["workers"] = merged_df["workers"].fillna("no_workers")
    merged_df["award_year"] = merged_df["award_year"].fillna(0).astype(int)
    merged_df["published_at"] = merged_df["published_at"].fillna("no_date")
    merged_df["updated_at"] = merged_df["updated_at"].fillna("no_date")

    return merged_df

    


def transform_data(spotify_df, grammys_df):

    spotify_df = transform_spotify_data(spotify_df)
    grammys_df = transform_grammys_data(grammys_df)

    merged_df = merge_datasets(spotify_df, grammys_df)

    return spotify_df, grammys_df, merged_df
