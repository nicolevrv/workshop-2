# 🎵 Spotify × Grammy Awards — ETL Pipeline

> ETL pipeline built with Apache Airflow that extracts data from two sources (CSV + MySQL), transforms and merges them, loads the result into a star-schema Data Warehouse and Google Drive, and powers a Power BI dashboard with music industry insights.

---

## 📁 Project Structure

```
WORKSHOP2/
├── airflow/
│   ├── config/
│   ├── dags/
│   ├── logs/
│   └── docker-compose.yaml
├── data/
│   ├── processed/
│   ├── raw/
│   └── star_schema/
├── notebooks/
│   └── data_profiling.ipynb
├── sql/
│   └── create_schema.sql
├── src/
│   ├── dimensional_model.py
│   ├── extract.py
│   ├── load_dw.py
│   ├── load_grammys_db.py
│   ├── main.py
│   ├── merge_data.py
│   ├── transform_grammys.py
│   └── transform_spotify.py
├── .gitignore
├── ETL_Workshop-2.pdf
├── README.md
├── setup.bat
├── setup.ps1
└── setup.sh
```

---

## ⚙️ Technologies

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.x (Docker) |
| Language | Python 3.11 |
| Source DB | MySQL 8 |
| Data Warehouse | MySQL 8 (star schema) |
| Cloud Storage | Google Drive API v3 |
| Visualization | Power BI |
| Libraries | pandas, SQLAlchemy, mysql-connector-python, google-api-python-client |

---

## 🚀 Setup Instructions

### Prerequisites

- Docker + Docker Compose
- MySQL 8 running locally (or accessible remotely)
- Python 3.11+ with `pip`
- A GCP service account with Drive API enabled (for Google Drive upload)

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/workshop2-etl.git
cd workshop2-etl
```

### 2. Add the raw datasets

Download both datasets and place them in `data/raw/`:

- `spotify_dataset.csv` — [Kaggle: Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)
- `the_grammy_awards.csv` — [Kaggle: Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards)

### 3. Configure environment variables

Copy this example to `.env` and fill in your credentials:

```env
DB_HOST=localhost
DB_USER=etl_user
DB_PASSWORD=your_password
DB_PORT=3306
DB_NAME=music_dw
GOOGLE_CREDENTIALS_PATH=/opt/etl/credentials/service_account.json
GOOGLE_DRIVE_FOLDER_ID=your_folder_id   
```

### 4. Run initial setup

```bash
# Linux / macOS / Git Bash
bash setup.sh

# Windows PowerShell
.\setup.ps1
```

This will: install dependencies, and load the Grammy CSV into MySQL (`grammys_raw` table).

### 5. run 'create_db.sql' manually once to create the music_dw DB.

On CMD.
```
mysql -u root -p < sql/create_db.sql
```
On Powershell.

```
Get-Content sql/create_db.sql | mysql -u root -p
```


### 6. Start Airflow

```bash
cd airflow
docker-compose up -d
```

Open the Airflow UI at `http://localhost:8080` and trigger the `spotify_grammys_etl` DAG manually.



### 7. (Optional) Run locally without Airflow

```bash
# Skip Google Drive and DW load if credentials aren't set up
python src/main.py --skip-drive --skip-dw --export-csv
```

---

## 🔄 ETL Pipeline

### Block Diagram

```
[spotify_dataset.csv] ──► read_csv ──► transform_csv ──┐
                                                         ├──► merge ──► load (MySQL DW) ──► store (Drive)
[MySQL: grammys_raw]  ──► read_db  ──► transform_db  ──┘
```

### Airflow DAG (`spotify_grammys_etl`)

```
read_csv ──► transform_csv ──┐
                              ├──► merge ──► load ──► store
read_db  ──► transform_db  ──┘
```

| Task | Responsibility |
|---|---|
| `read_csv` | Extract Spotify CSV → serialize to `/tmp/etl/<run_id>/spotify_raw.csv` |
| `read_db` | Extract Grammy table from MySQL → serialize to temp CSV |
| `transform_csv` | Clean Spotify: drop dupes, normalize `explicit`, build `artist_norm` |
| `transform_db` | Clean Grammys: parse dates, cast `winner` to bool, build `artist_norm` |
| `merge` | LEFT JOIN Spotify ← Grammy stats per artist on `artist_norm` |
| `load` | Build star schema, create MySQL tables, insert via `INSERT IGNORE` |
| `store` | Upload merged CSV to Google Drive (skipped if credentials missing) |

**XCom strategy:** DataFrames are serialized to temp CSV files scoped to the DAG run ID. Only the file path (a short string) is pushed via XCom, staying well within Airflow's XCom size limits.

---

## 🗃️ Data Model — Star Schema

The Data Warehouse follows a **star schema** optimized for analytical queries on music tracks enriched with Grammy data.

### Grain

One row in `fact_track` = one unique `(track_id, genre)` combination.

### Tables

```
                    ┌─────────────────┐
                    │   dim_artist    │
                    │─────────────────│
                    │ artist_key (PK) │
                    │ artist_name     │
                    │ grammy_noms     │
                    │ grammy_wins     │
                    │ first_grammy_yr │
                    │ last_grammy_yr  │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌──────────────┐
│  dim_album   │    │   fact_track    │    │  dim_genre   │
│──────────────│    │─────────────────│    │──────────────│
│ album_key PK │◄───│ track_id        │───►│ genre_key PK │
│ album_name   │    │ track_name      │    │ genre_name   │
│ artist_norm  │    │ artist_key  FK  │    └──────────────┘
└──────────────┘    │ album_key   FK  │
                    │ genre_key   FK  │    ┌──────────────┐
                    │ time_key    FK  │───►│  dim_time    │
                    │ danceability    │    │──────────────│
                    │ energy          │    │ time_key  PK │
                    │ valence         │    │ year         │
                    │ tempo           │    └──────────────┘
                    │ loudness        │
                    │ popularity      │
                    │ duration_ms     │
                    │ explicit        │
                    │ grammy_noms     │
                    │ grammy_wins     │
                    └─────────────────┘
```

### Design Decisions

- **LEFT JOIN on Spotify** — every track is preserved regardless of Grammy presence. Tracks with no Grammy history receive `grammy_nominations = 0`, `grammy_wins = 0`, and `NA` for year fields (not `0`, since year=0 is semantically meaningless).
- **`artist_norm` as join key** — a lowercase, stripped version of the first-billed artist name (split on `;`). Ensures consistent matching across both datasets.
- **Sentinel `time_key = 0`** — artists never nominated map to `time_key = 0` (year = 0), cleanly distinguishing "no Grammy history" from actual Grammy years.
- **Grammy stats denormalized into `fact_track`** — `grammy_nominations` and `grammy_wins` are stored in the fact table to allow quick aggregation without joining `dim_artist` for every query.
- **`INSERT IGNORE` for idempotency** — running the pipeline twice does not produce duplicate rows.
- **Fan-out guard** — after the LEFT JOIN, the code asserts `len(merged) == len(spotify_df)`, catching any accidental row multiplication from duplicate `artist_norm` values in the Grammy aggregation.

---

## 📊 Dashboard (Power BI)

The Power BI dashboard connects directly to the `music_dw` MySQL database and queries the star schema. It does **not** use the CSV file.

### KPIs

| # | KPI | Source |
|---|---|---|
| 1 | Total Grammy Wins by Artist | `fact_track` + `dim_artist` |
| 2 | Average Spotify Popularity — Grammy Winners vs. Non-Winners | `fact_track` |
| 3 | Most danceable genres | `fact_track` + `dim_genre` |

### Charts

| # | Chart | Description |
|---|---|---|
| 1 | Grammy Nominations vs. Spotify Popularity (scatter) | Correlation between award recognition and streaming popularity |
| 2 | Audio Features Radar by Genre | Danceability, energy, valence, acousticness per genre |
| 3 | Grammy Wins Over Time (line) | Award trends from `dim_time` |
| 4 | Top 10 Artists by Grammy Wins (bar) | Ranked by `grammy_wins` |

---

## 🔑 Key Assumptions & Decisions

1. **Only the first-billed Spotify artist** is used as the join key. Collaborators listed after `;` do not inherit Grammy stats from the lead artist — this avoids inflating match rates with partial credits.
2. **Grammy nominations are counted per row** in the raw dataset, regardless of win status. One row = one nomination.
3. **`unknown` Grammy artists are excluded** from aggregation to avoid polluting the unknown bucket with unrelated nominees.
4. **`explicit` and `winner` columns** are normalized to boolean at transform time, not at load time.
5. **The Grammy CSV is loaded into MySQL once** via `load_grammys_db.py` before the pipeline runs. This reflects a real-world pattern where operational data lives in a transactional database.

---

## 📓 Data Profiling

See `notebooks/data_profiling.ipynb` for the full EDA, including:

- Shape and data types of both datasets
- Missing value rates per column
- Distribution of audio features (danceability, energy, valence, tempo)
- Top Grammy categories and most nominated artists
- Duplicate analysis by `track_id` and `track_genre`

---

## 🔒 Security Notes

- All credentials are read from environment variables — **never hardcoded**.
- `.env` files and `credentials/*.json` are excluded via `.gitignore`.
- The `FERNET_KEY` in the Airflow `.env` is for local development only — rotate before any shared deployment.

---
