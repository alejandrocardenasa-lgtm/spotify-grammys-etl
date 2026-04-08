import pandas as pd
import sqlite3
import os

def load_grammys_to_db():
    csv_path = "data/raw/the_grammy_awards.csv"
    db_path = "data/processed/the_grammy_awards.db"

    os.makedirs("data/processed", exist_ok=True)

    df = pd.read_csv(csv_path)

    conn = sqlite3.connect(db_path)
    df.to_sql("grammys", conn, if_exists="replace", index=False)
    conn.close()

    print("the_grammy_awards cargado a la base de datos")

