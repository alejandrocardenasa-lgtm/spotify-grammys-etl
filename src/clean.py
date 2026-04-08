import pandas as pd

def clean_spotify_data(spotify_df):
    spotify_df = spotify_df.copy()

    # Eliminar duplicados
    spotify_df = spotify_df.drop_duplicates()

    # Llenar nulos
    spotify_df["track_id"] = spotify_df["track_id"].fillna("unknown")
    spotify_df["artists"] = spotify_df["artists"].fillna("Unknown")
    spotify_df["album_name"] = spotify_df["album_name"].fillna("Unknown")
    spotify_df["track_name"] = spotify_df["track_name"].fillna("Unknown")
    spotify_df["track_genre"] = spotify_df["track_genre"].fillna("unknown")

    # Limpiar espacios en columnas de texto
    text_cols = ["track_id", "artists", "album_name", "track_name", "track_genre"]
    for col in text_cols:
        spotify_df[col] = spotify_df[col].astype(str).str.strip()

    return spotify_df


def clean_grammys_data(grammys_df):
    grammys_df = grammys_df.copy()

    # Eliminar duplicados
    grammys_df = grammys_df.drop_duplicates()

    # Llenar nulos
    grammys_df["title"] = grammys_df["title"].fillna("Unknown")
    grammys_df["category"] = grammys_df["category"].fillna("Unknown")
    grammys_df["nominee"] = grammys_df["nominee"].fillna("Unknown")
    grammys_df["artist"] = grammys_df["artist"].fillna("Unknown")
    grammys_df["workers"] = grammys_df["workers"].fillna("Unknown")
    grammys_df["img"] = grammys_df["img"].fillna("No Image")

    # Limpiar espacios en columnas de texto
    text_cols = ["title", "category", "nominee", "artist", "workers", "img"]
    for col in text_cols:
        grammys_df[col] = grammys_df[col].astype(str).str.strip()

    return grammys_df