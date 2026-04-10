# Spotify & Grammys ETL Pipeline

## Project Overview
This project implements an ETL pipeline with Apache Airflow to integrate Spotify track data and Grammy Awards data into an analytical data warehouse. The pipeline extracts data from CSV and SQLite sources, cleans and standardizes the information, transforms and merges both datasets, builds a dimensional model, and loads the results into PostgreSQL for reporting and dashboarding.

The project was developed as a portfolio ETL project focused on data engineering concepts such as orchestration, data cleaning, dimensional modeling, reproducibility, and analytical visualization.

---

## Objectives
- Integrate Spotify and Grammy Awards datasets into a single analytical workflow.
- Automate the ETL process using Apache Airflow.
- Build a star-schema dimensional model for reporting.
- Generate cleaned and transformed datasets for dashboard creation.
- Store final outputs in a data warehouse for analysis.

---

## Repository Structure

```text
spotify-grammys-etl/
├── config/
├── dags/
│   └── Spotify_Grammysdag.py
├── data/
│   ├── raw/
│   │   ├── spotify_dataset.csv
│   │   └── the_grammy_awards.csv
│   └── processed/
├── notebooks/
│   ├── eda_grammys.ipynb
│   └── eda_spotify.ipynb
├── src/
│   ├── clean.py
│   ├── dimensional_model.py
│   ├── extract.py
│   ├── load.py
│   ├── load_grammys_to_db.py
│   ├── main.py
│   ├── transform.py
│   └── upload_to_drive.py
├── visualizations/
├── docker-compose.yaml
├── requirements.txt
└── .gitignore
```

---

## ETL Pipeline Steps

### 1. Extract

The extraction phase reads data from two different sources:

Spotify dataset: loaded from data/raw/spotify_dataset.csv  
Grammys dataset: first loaded from CSV into a local SQLite staging database, then extracted from that database  

This design simulates a more realistic ETL scenario where data may come from heterogeneous sources instead of a single flat file.

### 2. Clean

In the cleaning phase:

Duplicate records are removed  
Missing values are filled with default values such as unknown, no_award, no_nominee, and no_workers  
Text columns are stripped of extra spaces  

This step ensures both datasets have consistent values before transformation and merge operations.

### 3. Transform

The transformation phase standardizes both datasets and prepares them for integration:

Text fields are converted to lowercase and trimmed  
Spotify numeric columns are converted to numeric types and missing numeric values are filled with zero  
Grammy date columns are parsed and the award_year field is derived  
A common field called merge_artist is created in both datasets to support dataset integration  

Two transformed outputs are created:

Track-level merged dataset: joins Spotify tracks with Grammy information by artist  
Artist-level dataset: aggregates Spotify metrics and Grammy statistics by artist for dashboarding  

## 4. Dimensional Model

The transformed data is organized using a **star schema**, designed to support analytical queries combining Spotify track metrics with Grammy award information.

### Grain
The grain of the fact table is defined as:

> **One row represents a Spotify track associated with a Grammy record for the same artist, category, and year.**

This means:
- A track can appear multiple times if the artist has multiple Grammy nominations or categories.
- If no Grammy record exists, the track is still included with default values.
- This design enables analysis at the intersection of **music performance (Spotify)** and **award recognition (Grammys)**.

---

## Dimensions

### 1. dim_artist
Stores unique artist information.

**Fields:**
- `artist_id` (PK)
- `artist_name`

---

### 2. dim_track
Stores Spotify track-level metadata.

**Fields:**
- `track_id` (PK)
- `track_name`
- `album_name`
- `explicit`

---

### 3. dim_genre
Stores genre classification for tracks.

**Fields:**
- `genre_id` (PK)
- `track_genre`

---

### 4. dim_award
Stores Grammy award details.

**Fields:**
- `award_id` (PK)
- `category`
- `nominee`
- `workers`
- `winner`

This dimension allows analysis of:
- nominations vs winners
- award categories
- relationships between Spotify metrics and Grammy outcomes

---

### 5. dim_date
Stores time-related attributes of Grammy records.

**Fields:**
- `date_id` (PK)
- `award_year`
- `published_at`
- `updated_at`

This enables time-based analysis such as:
- trends by year
- evolution of awards over time

---

## Fact Table

### 6. fact_music_awards
Central fact table containing Spotify metrics and foreign keys to all dimensions.

**Fields:**
- `fact_id` (PK)
- `artist_id` (FK)
- `track_id` (FK)
- `genre_id` (FK)
- `award_id` (FK)
- `date_id` (FK)

**Measures (Spotify features):**
- `popularity`
- `duration_ms`
- `danceability`
- `energy`
- `loudness`
- `speechiness`
- `acousticness`
- `instrumentalness`
- `liveness`
- `valence`
- `tempo`

---

## Design Rationale

- A **star schema** was chosen to simplify analytical queries and improve performance.
- Dimensions separate descriptive attributes from measurable metrics.
- The fact table centralizes Spotify numerical features for analysis.
- Grammy data is integrated through `dim_award` and `dim_date`, enabling cross-domain insights.
- Surrogate keys (`*_id`) ensure consistency and efficient joins.

This model supports queries such as:
- Which genres have higher Grammy-winning tracks?
- Do more popular songs tend to win awards?
- How do audio features differ between winners and non-winners?
- Trends of awards and music characteristics over time
### 5. Load

The final load phase writes data into PostgreSQL:

All dimension tables are loaded first  
The fact table is loaded next  
Additional analytical tables are also loaded:  
merged_music_data  
artist_level_data  

To avoid duplicated data across DAG runs, the dimensional tables are truncated before reloading, and auxiliary analytical tables are dropped and recreated.

### 6. Optional Output to Google Drive

The pipeline also includes an optional step to upload the merged CSV output to Google Drive. This functionality requires personal credentials and is not included in the repository for security reasons.

---

## Airflow DAG Design

The ETL pipeline is orchestrated in Airflow through the DAG spotify_grammys_etl.

### DAG Tasks

load_grammys_to_db  
extract_spotify  
extract_grammys  
clean  
transform_merge  
load_google_drive  
load_warehouse  
cleanup_files  

### DAG Flow

The DAG first loads the Grammy CSV into a staging SQLite database. Then it extracts both Spotify and Grammy data, cleans them, transforms and merges them, builds the dimensional model, and finally loads the results into PostgreSQL. After loading, the pipeline removes temporary processed files.

### DAG Design Decisions

Task modularity: each ETL phase is separated into a dedicated Airflow task for clarity and maintainability  

Intermediate CSV files: temporary files are stored in data/processed so that each stage can pass lightweight file paths through XCom instead of large datasets  

Parallel extraction: Spotify and Grammy extraction tasks run after the SQLite load step  

Cleanup step: temporary files are removed at the end to reduce storage usage  

Retry configuration: the DAG includes retries and retry delays to improve robustness  

---

## Assumptions and Decisions Made During Transformations

Several assumptions and transformation decisions were made to ensure the pipeline could run consistently and produce analytical outputs:

### 1. Artist-based merge logic

The integration between Spotify and Grammy data is based on a standardized artist key called merge_artist. This assumes that artist names can be matched after lowercasing and trimming spaces.

### 2. Default values for missing data

Missing values are replaced with business defaults such as:

unknown  
no_award  
no_nominee  
no_workers  
no_date  

### 3. Numeric coercion

Spotify audio and popularity metrics are converted to numeric values, and any invalid or missing values are replaced with zero. This allows the fact table to store complete numeric measures for analysis.

### 4. Grammy year derivation

The field award_year is derived from published_at. If the date cannot be parsed, the year is set to 0 as a default placeholder.

### 5. Two analytical granularities

The project creates:

a track-level merged dataset, useful for detailed record-level analysis  
an artist-level aggregated dataset, better suited for dashboards and summary metrics  

### 6. Surrogate keys in dimensions

Each dimension table uses generated surrogate keys such as artist_id, genre_id, award_id, and date_id. This supports a proper warehouse design and simplifies fact table relationships.

### 7. Idempotent loading

The load process truncates dimensional tables and reloads them to prevent duplication when the DAG is executed multiple times.

---

## Setup Instructions

### 1. Clone the repository

git clone https://github.com/alejandrocardenasa-lgtm/spotify-grammys-etl.git  
cd spotify-grammys-etl  

### 2. Install dependencies

pip install -r requirements.txt  

### 3. Start the environment

docker compose up -d  

### 4. Run the Airflow DAG

Open Airflow in your browser and trigger the DAG:

DAG name: spotify_grammys_etl  

### 5. Explore the results

After the pipeline runs successfully, the warehouse tables will be available in PostgreSQL and the generated outputs can be used in Metabase or another BI tool for dashboards.

---

## Technologies Used

Python  
Apache Airflow  
Pandas  
PostgreSQL  
SQLite  
SQLAlchemy  
Docker  
Google Drive API  
Jupyter Notebooks  
Metabase  

---

## Visualizations

The repository includes a visualizations/ folder with dashboard screenshots and charts created from the final analytical outputs. These visualizations summarize relationships between Spotify metrics, genres, artists, and Grammy recognition.

Examples include:

average song popularity by year  
average energy by genre  
artists with the most songs  
happiest music genres  
most popular genres  
top artists by nominations  

---

## Key Decisions

Airflow was used to orchestrate the pipeline because it clearly separates ETL stages and supports reproducible workflows.  
SQLite was used as a staging layer for Grammy data before warehouse loading.  
PostgreSQL was used as the final warehouse for dimensional and analytical tables.  
The project stores both detailed and aggregated outputs to support different reporting needs.  
A cleanup task was added to remove temporary files generated during execution.  
Sensitive credential files were excluded from version control for security.  

---

## Security Note

Google Drive upload functionality is optional. Credential files and tokens are intentionally excluded from the repository for security reasons.

---

## Author

Gonoalejo
