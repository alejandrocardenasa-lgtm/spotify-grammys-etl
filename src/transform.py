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
    spotify_df["explicit"] = pd.to_numeric(
        spotify_df["explicit"], errors="coerce"
    ).fillna(0).astype(int)

    # Crear clave de merge por artista
    spotify_df["merge_artist"] = spotify_df["artists"]

    # Rellenar nulos en columnas numéricas importantes
    numeric_cols = [
        "popularity",
        "duration_ms",
        "danceability",
        "energy",
        "key",
        "loudness",
        "mode",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
        "time_signature",
    ]

    for col in numeric_cols:
        if col in spotify_df.columns:
            spotify_df[col] = pd.to_numeric(spotify_df[col], errors="coerce").fillna(0)

    return spotify_df


def transform_grammys_data(grammys_df):
    grammys_df = grammys_df.copy()

    # Estandarizar texto
    text_cols = ["title", "category", "nominee", "artist", "workers"]
    for col in text_cols:
        if col in grammys_df.columns:
            grammys_df[col] = grammys_df[col].astype(str).str.lower().str.strip()

    # Fechas
    grammys_df["published_at"] = pd.to_datetime(
        grammys_df["published_at"], errors="coerce", utc=True
    )
    grammys_df["updated_at"] = pd.to_datetime(
        grammys_df["updated_at"], errors="coerce", utc=True
    )

    # Año del Grammy
    grammys_df["award_year"] = grammys_df["published_at"].dt.year

    # Reemplazar nulos
    grammys_df["award_year"] = grammys_df["award_year"].fillna(0).astype(int)

    grammys_df["published_at"] = grammys_df["published_at"].astype(str).replace("NaT", "no_date")
    grammys_df["updated_at"] = grammys_df["updated_at"].astype(str).replace("NaT", "no_date")

    # Winner a entero
    grammys_df["winner"] = pd.to_numeric(
        grammys_df["winner"], errors="coerce"
    ).fillna(0).astype(int)

    # Crear clave de merge por artista
    grammys_df["merge_artist"] = grammys_df["artist"]

    return grammys_df


def merge_datasets_track_level(spotify_df, grammys_df):
    """
    Dataset enriquecido a nivel track.
    Cumple con el requerimiento de unir Spotify + Grammys,
    pero puede duplicar registros si un artista tiene muchas canciones y muchos premios.
    """
    spotify_df = spotify_df.copy()
    grammys_df = grammys_df.copy()

    grammys_clean = grammys_df[
        [
            "merge_artist",
            "category",
            "nominee",
            "workers",
            "winner",
            "award_year",
            "published_at",
            "updated_at",
        ]
    ].drop_duplicates()

    merged_df = pd.merge(
        spotify_df,
        grammys_clean,
        on="merge_artist",
        how="left",
        suffixes=("_spotify", "_grammys"),
    )

    merged_df["winner"] = merged_df["winner"].fillna(0).astype(int)
    merged_df["category"] = merged_df["category"].fillna("no_award")
    merged_df["nominee"] = merged_df["nominee"].fillna("no_nominee")
    merged_df["workers"] = merged_df["workers"].fillna("no_workers")
    merged_df["award_year"] = merged_df["award_year"].fillna(0).astype(int)
    merged_df["published_at"] = merged_df["published_at"].fillna("no_date")
    merged_df["updated_at"] = merged_df["updated_at"].fillna("no_date")

    return merged_df


def build_artist_level_spotify(spotify_df):
    """
    Agrega Spotify a nivel artista.
    """
    spotify_artist = (
        spotify_df.groupby("merge_artist", as_index=False)
        .agg(
            total_tracks=("track_id", "nunique"),
            avg_popularity=("popularity", "mean"),
            avg_duration_ms=("duration_ms", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_energy=("energy", "mean"),
            avg_speechiness=("speechiness", "mean"),
            avg_acousticness=("acousticness", "mean"),
            avg_instrumentalness=("instrumentalness", "mean"),
            avg_liveness=("liveness", "mean"),
            avg_valence=("valence", "mean"),
            avg_tempo=("tempo", "mean"),
            top_genre=("track_genre", lambda x: x.mode().iloc[0] if not x.mode().empty else "unknown"),
        )
    )

    # Redondear métricas promedio
    avg_cols = [
        "avg_popularity",
        "avg_duration_ms",
        "avg_danceability",
        "avg_energy",
        "avg_speechiness",
        "avg_acousticness",
        "avg_instrumentalness",
        "avg_liveness",
        "avg_valence",
        "avg_tempo",
    ]

    for col in avg_cols:
        spotify_artist[col] = spotify_artist[col].round(2)

    return spotify_artist


def build_artist_level_grammys(grammys_df):
    """
    Agrega Grammys a nivel artista.
    """
    grammys_artist = (
        grammys_df.groupby("merge_artist", as_index=False)
        .agg(
            total_nominations=("category", "count"),
            total_wins=("winner", "sum"),
            first_award_year=("award_year", lambda x: x[x > 0].min() if (x > 0).any() else 0),
            last_award_year=("award_year", lambda x: x[x > 0].max() if (x > 0).any() else 0),
        )
    )

    grammys_artist["total_nominations"] = grammys_artist["total_nominations"].fillna(0).astype(int)
    grammys_artist["total_wins"] = grammys_artist["total_wins"].fillna(0).astype(int)
    grammys_artist["first_award_year"] = grammys_artist["first_award_year"].fillna(0).astype(int)
    grammys_artist["last_award_year"] = grammys_artist["last_award_year"].fillna(0).astype(int)

    return grammys_artist


def merge_datasets_artist_level(spotify_df, grammys_df):
    """
    Dataset final a nivel artista.
    Este es el mejor para dashboard y análisis.
    """
    spotify_artist = build_artist_level_spotify(spotify_df)
    grammys_artist = build_artist_level_grammys(grammys_df)

    artist_level_df = pd.merge(
        spotify_artist,
        grammys_artist,
        on="merge_artist",
        how="left",
    )

    artist_level_df["total_nominations"] = artist_level_df["total_nominations"].fillna(0).astype(int)
    artist_level_df["total_wins"] = artist_level_df["total_wins"].fillna(0).astype(int)
    artist_level_df["first_award_year"] = artist_level_df["first_award_year"].fillna(0).astype(int)
    artist_level_df["last_award_year"] = artist_level_df["last_award_year"].fillna(0).astype(int)

    artist_level_df["has_award"] = (artist_level_df["total_wins"] > 0).astype(int)

    return artist_level_df


def transform_data(spotify_df, grammys_df):
    spotify_df = transform_spotify_data(spotify_df)
    grammys_df = transform_grammys_data(grammys_df)

    # Dataset enriquecido a nivel track
    merged_df = merge_datasets_track_level(spotify_df, grammys_df)

    # Dataset agregado a nivel artista
    artist_level_df = merge_datasets_artist_level(spotify_df, grammys_df)

    return spotify_df, grammys_df, merged_df, artist_level_df