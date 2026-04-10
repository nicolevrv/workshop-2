"""
extract.py - Task a: Data Extraction
Responsibility: load raw datasets into DataFrames and apply minimal
type casting.
No cleaning or transformations are performed here.

Sources
-------
- Spotify  : CSV file read directly with pandas.
- Grammys  : MySQL table (grammys_raw) — loaded there by load_grammys_db.py
             before the pipeline / DAG runs.

This separation reflects a real-world pattern where operational data
(Grammys) lives in a transactional database, while flat files (Spotify)
are ingested directly.
"""

import os
from pathlib import Path

from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
SPOTIFY_PATH = ROOT / "data" / "raw" / "spotify_dataset.csv"

# ── DB config ─────────────────────────────────────────────────────────────────
def _get_engine():
    """Build a SQLAlchemy engine from environment variables."""
    host     = os.environ.get("DB_HOST",     "localhost")
    user     = os.environ.get("DB_USER",     "etl_user")
    password = os.environ.get("DB_PASSWORD", "")
    port     = int(os.environ.get("DB_PORT", 3306))
    database = os.environ.get("DB_NAME",     "music_dw")
    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
GRAMMYS_TABLE = "grammys_raw"

# ── Numeric columns expected in the Spotify dataset ───────────────────────────
SPOTIFY_NUMERIC_COLS = [
    "popularity", "duration_ms", "danceability", "energy",
    "loudness", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence", "tempo",
]

# ── Required columns for schema assertion ─────────────────────────────────────
REQUIRED_SPOTIFY = {"track_id", "artists", "album_name", "track_name", "track_genre"}
REQUIRED_GRAMMY  = {"year", "category", "nominee", "artist", "winner"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _validate_spotify_path(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Spotify CSV not found: {path}\n"
            f"Make sure 'spotify_dataset.csv' is in data/raw/"
        )


def _cast_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Coerce columns to numeric, replacing non-parseable values and
    infinities with NA so downstream code handles them uniformly.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].replace([float("inf"), float("-inf")], pd.NA)
    return df


def _assert_schema(df: pd.DataFrame, required: set, name: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{name} dataset is missing expected columns: {missing}"
        )


# ── Spotify extractor ─────────────────────────────────────────────────────────
def extract_spotify(spotify_path=None) -> pd.DataFrame:
    """
    Load the raw Spotify dataset from CSV.

    All columns are read as strings first; numeric columns are cast
    explicitly afterward to avoid silent coercion.
    """
    path = Path(spotify_path or SPOTIFY_PATH)
    _validate_spotify_path(path)

    print(f"\nSpotify  ->  {path}")
    df = pd.read_csv(path, dtype=str)
    df = _cast_numeric(df, SPOTIFY_NUMERIC_COLS)

    _assert_schema(df, REQUIRED_SPOTIFY, "Spotify")

    print(f"  Shape  :  {df.shape[0]:,} rows x {df.shape[1]} columns")
    print("\n-- Spotify sample (3 rows) --")
    print(df.head(3).to_string(index=False))

    return df


# ── Grammy extractor ──────────────────────────────────────────────────────────
def extract_grammys() -> pd.DataFrame:
    """
    Load the Grammy Awards dataset from MySQL (grammys_raw table).

    Reads all columns as-is; type casting happens in transform.py.
    The table must have been populated by load_grammys_db.py beforehand.
    """
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    database = os.environ.get("DB_NAME", "music_dw")
    print(f"\nGrammys  ->  MySQL [{host}:{port}]  db={database}  table={GRAMMYS_TABLE}")

    try:
        engine = _get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {GRAMMYS_TABLE};", conn)
    except Exception as e:
        raise ConnectionError(
            f"Could not read from MySQL: {e}\n"
            f"Make sure MySQL is running and DB_* env vars are set correctly."
        ) from e

    if df.empty:
        raise ValueError(
            f"Table '{GRAMMYS_TABLE}' is empty.\n"
            f"Run load_grammys_db.py before starting the pipeline."
        )

    # Drop the auto-increment surrogate added by load_grammys_db.py
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # Cast year to numeric (stored as VARCHAR in grammys_raw)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

    _assert_schema(df, REQUIRED_GRAMMY, "Grammy")

    print(f"  Shape  :  {df.shape[0]:,} rows x {df.shape[1]} columns")
    print("\n-- Grammy sample (3 rows) --")
    print(df.head(3).to_string(index=False))

    return df


# ── Runner ────────────────────────────────────────────────────────────────────
def run(spotify_path=None):
    """
    Entry point called by main.py and the Airflow DAG.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (spotify_df, grammy_df) -- raw, minimally typed DataFrames.
    """
    print("\n" + "=" * 70)
    print("TASK a -- EXTRACTION")
    print("=" * 70)

    spotify_df = extract_spotify(spotify_path)
    grammy_df  = extract_grammys()

    print("\nSchema assertions passed")

    return spotify_df, grammy_df


if __name__ == "__main__":
    run()