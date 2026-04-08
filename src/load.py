import sqlite3
import os


def load_data(merged_df, tables):

    # Crear carpeta processed si no existe
    os.makedirs("data/processed", exist_ok=True)

    # Paths
    dw_path = "data/processed/data_warehouse.db"
    csv_path = "data/processed/merged_dataset.csv"

    # 1. Guardar merged dataset como CSV (para Google Drive)
    merged_df.to_csv(csv_path, index=False)

    # 2. Conexion al Data Warehouse
    conn = sqlite3.connect(dw_path)

    # 3. Guardar merged dataset en el DW
    merged_df.to_sql("merged_music_data", conn, if_exists="replace", index=False)

    # 4. Guardar dimensiones
    tables["dim_artist"].to_sql("dim_artist", conn, if_exists="replace", index=False)
    tables["dim_track"].to_sql("dim_track", conn, if_exists="replace", index=False)
    tables["dim_genre"].to_sql("dim_genre", conn, if_exists="replace", index=False)
    tables["dim_award"].to_sql("dim_award", conn, if_exists="replace", index=False)
    tables["dim_date"].to_sql("dim_date", conn, if_exists="replace", index=False)

    # 5. Guardar fact table
    tables["fact_music_awards"].to_sql("fact_music_awards", conn, if_exists="replace", index=False)

    conn.close()

    print("Data Warehouse creado en SQLite.")
    print("Merged dataset exportado como CSV.")