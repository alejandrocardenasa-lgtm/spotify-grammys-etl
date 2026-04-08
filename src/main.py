from extract import extract_spotify_csv, extract_grammys_db
from transform import transform_data
from dimensional_model import build_dimensional_model
from load import load_data
from load_grammys_to_db import load_grammys_to_db
from upload_to_drive import upload_csv_to_drive

def main():
    # 0. Crear base de datos staging de Grammys
    load_grammys_to_db()

    # 1. Extract
    spotify_df = extract_spotify_csv()
    grammys_df = extract_grammys_db()

    # 2. Transform
    spotify_df, grammys_df, merged_df = transform_data(spotify_df, grammys_df)

    # 3. Modelo dimensional
    tables = build_dimensional_model(spotify_df, grammys_df, merged_df)

    # 4. Load local
    load_data(merged_df, tables)

    # 5. Subir CSV a Google Drive
    upload_csv_to_drive("data/processed/merged_dataset.csv")

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()