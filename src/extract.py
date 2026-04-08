import pandas as pd
import sqlite3

def extract_spotify_csv():
    spotify_df = pd.read_csv("data/raw/spotify_dataset.csv", index_col=0)
    return spotify_df

def extract_grammys_db():
    conn = sqlite3.connect("data/processed/the_grammy_awards.db")
    grammys_df = pd.read_sql_query("SELECT * FROM grammys", conn)
    conn.close()
    return grammys_df