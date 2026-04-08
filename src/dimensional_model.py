import pandas as pd


def build_dim_artist(merged_df):
    df = merged_df.copy()

    df["merge_artist"] = (
        df["merge_artist"]
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    dim_artist = df[["merge_artist"]].drop_duplicates().reset_index(drop=True)
    dim_artist = dim_artist.rename(columns={"merge_artist": "artist_name"})

    default_row = pd.DataFrame([{"artist_name": "unknown"}])
    dim_artist = pd.concat([default_row, dim_artist], ignore_index=True)

    dim_artist = dim_artist.drop_duplicates().reset_index(drop=True)
    dim_artist["artist_id"] = dim_artist.index
    dim_artist = dim_artist[["artist_id", "artist_name"]]

    return dim_artist


def build_dim_track(spotify_df):
    df = spotify_df.copy()

    df["track_id"] = df["track_id"].fillna("unknown").astype(str)
    df["track_name"] = df["track_name"].fillna("unknown").astype(str).str.strip().str.lower()
    df["album_name"] = df["album_name"].fillna("unknown").astype(str).str.strip().str.lower()
    df["explicit"] = pd.to_numeric(df["explicit"], errors="coerce").fillna(0).astype(int)

    dim_track = df[
        ["track_id", "track_name", "album_name", "explicit"]
    ].drop_duplicates().reset_index(drop=True)

    default_row = pd.DataFrame({
        "track_id": ["unknown"],
        "track_name": ["unknown"],
        "album_name": ["unknown"],
        "explicit": [0]
    })

    dim_track = pd.concat([default_row, dim_track], ignore_index=True)
    dim_track = dim_track.drop_duplicates().reset_index(drop=True)

    return dim_track


def build_dim_genre(spotify_df):
    df = spotify_df.copy()

    df["track_genre"] = (
        df["track_genre"]
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    dim_genre = df[["track_genre"]].drop_duplicates().reset_index(drop=True)

    default_row = pd.DataFrame([{"track_genre": "unknown"}])
    dim_genre = pd.concat([default_row, dim_genre], ignore_index=True)

    dim_genre = dim_genre.drop_duplicates().reset_index(drop=True)
    dim_genre["genre_id"] = dim_genre.index
    dim_genre = dim_genre[["genre_id", "track_genre"]]

    return dim_genre


def build_dim_award(grammys_df):
    df = grammys_df.copy()

    df["category"] = (
        df["category"]
        .fillna("no_award")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["nominee"] = (
        df["nominee"]
        .fillna("no_nominee")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["workers"] = (
        df["workers"]
        .fillna("no_workers")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["winner"] = pd.to_numeric(df["winner"], errors="coerce").fillna(0).astype(int)

    dim_award = df[
        ["category", "nominee", "workers", "winner"]
    ].drop_duplicates().reset_index(drop=True)

    default_row = pd.DataFrame([{
        "category": "no_award",
        "nominee": "no_nominee",
        "workers": "no_workers",
        "winner": 0
    }])

    dim_award = pd.concat([default_row, dim_award], ignore_index=True)
    dim_award = dim_award.drop_duplicates().reset_index(drop=True)

    dim_award["award_id"] = dim_award.index
    dim_award = dim_award[["award_id", "category", "nominee", "workers", "winner"]]

    return dim_award


def build_dim_date(grammys_df):
    df = grammys_df.copy()

    df["award_year"] = pd.to_numeric(df["award_year"], errors="coerce").fillna(0).astype(int)
    df["published_at"] = df["published_at"].fillna("no_date").astype(str)
    df["updated_at"] = df["updated_at"].fillna("no_date").astype(str)

    dim_date = df[
        ["award_year", "published_at", "updated_at"]
    ].drop_duplicates().reset_index(drop=True)

    default_row = pd.DataFrame({
        "award_year": [0],
        "published_at": ["no_date"],
        "updated_at": ["no_date"]
    })

    dim_date = pd.concat([default_row, dim_date], ignore_index=True)
    dim_date = dim_date.drop_duplicates().reset_index(drop=True)

    dim_date["date_id"] = dim_date.index
    dim_date = dim_date[["date_id", "award_year", "published_at", "updated_at"]]

    return dim_date


def build_fact_table(merged_df, dim_artist, dim_genre, dim_award, dim_date):
    fact = merged_df.copy()

    fact = fact.drop(columns=["artist_id", "genre_id", "award_id", "date_id"], errors="ignore")
    
    # Estandarizar columnas clave
    fact["merge_artist"] = (
        fact["merge_artist"]
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fact["track_genre"] = (
        fact["track_genre"]
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fact["category"] = (
        fact["category"]
        .fillna("no_award")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fact["nominee"] = (
        fact["nominee"]
        .fillna("no_nominee")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fact["workers"] = (
        fact["workers"]
        .fillna("no_workers")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fact["winner"] = pd.to_numeric(fact["winner"], errors="coerce").fillna(0).astype(int)
    fact["award_year"] = pd.to_numeric(fact["award_year"], errors="coerce").fillna(0).astype(int)

    fact["published_at"] = fact["published_at"].fillna("no_date").astype(str)
    fact["updated_at"] = fact["updated_at"].fillna("no_date").astype(str)

    fact["track_id"] = fact["track_id"].fillna("unknown").astype(str)

    # Mapear artist_id
    fact = fact.merge(
        dim_artist,
        left_on="merge_artist",
        right_on="artist_name",
        how="left"
    )

    fact = fact.drop(columns=["genre_id"], errors="ignore")

    # Mapear genre_id
    fact = fact.merge(
        dim_genre,
        on="track_genre",
        how="left"
    )

    # Mapear award_id
    fact = fact.merge(
        dim_award,
        on=["category", "nominee", "workers", "winner"],
        how="left"
    )

    # Mapear date_id
    fact = fact.merge(
        dim_date,
        on=["award_year", "published_at", "updated_at"],
        how="left"
    )

    # Rellenar IDs nulos
    fact["artist_id"] = fact["artist_id"].fillna(0).astype(int)
    fact["genre_id"] = fact["genre_id"].fillna(0).astype(int)
    fact["award_id"] = fact["award_id"].fillna(0).astype(int)
    fact["date_id"] = fact["date_id"].fillna(0).astype(int)

    # Convertir métricas numéricas
    metric_cols = [
        "popularity",
        "duration_ms",
        "danceability",
        "energy",
        "loudness",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo"
    ]

    for col in metric_cols:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").fillna(0)

    # Construir fact table
    fact_table = fact[
        [
            "artist_id",
            "track_id",
            "genre_id",
            "award_id",
            "date_id",
            "popularity",
            "duration_ms",
            "danceability",
            "energy",
            "loudness",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo"
        ]
    ].copy()

    fact_table = fact_table.reset_index(drop=True)
    fact_table["fact_id"] = fact_table.index + 1

    fact_table = fact_table[
        [
            "fact_id",
            "artist_id",
            "track_id",
            "genre_id",
            "award_id",
            "date_id",
            "popularity",
            "duration_ms",
            "danceability",
            "energy",
            "loudness",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo"
        ]
    ]

    return fact_table


def build_dimensional_model(spotify_df, grammys_df, merged_df):
    dim_artist = build_dim_artist(merged_df)
    dim_track = build_dim_track(spotify_df)
    dim_genre = build_dim_genre(spotify_df)
    dim_award = build_dim_award(grammys_df)
    dim_date = build_dim_date(grammys_df)

    fact_music_awards = build_fact_table(
        merged_df,
        dim_artist,
        dim_genre,
        dim_award,
        dim_date
    )

    return {
        "dim_artist": dim_artist,
        "dim_track": dim_track,
        "dim_genre": dim_genre,
        "dim_award": dim_award,
        "dim_date": dim_date,
        "fact_music_awards": fact_music_awards
    }