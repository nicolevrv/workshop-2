"""
transform_spotify.py - Task b.1: Data Cleaning for Spotify (CSV)
Responsibility: clean and normalize Spotify dataset from raw CSV.
"""

import pandas as pd

SPOTIFY_CRITICAL_COLS = ["artists", "album_name", "track_name"]


def _normalize_artist(series: pd.Series) -> pd.Series:
    """
    Build a lowercase, stripped join key from artist string.
    Takes first artist when multiple are separated by ';'.
    """
    return (
        series.fillna("unknown")
        .astype(str)
        .str.split(";")
        .str[0]
        .str.strip()
        .str.lower()
    )


def clean_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the raw Spotify DataFrame.

    Steps
    -----
    1. Drop the auto-index column if present ('Unnamed: 0').
    2. Drop rows missing any critical identifier column.
    3. Deduplicate on (track_id, track_genre).
    4. Convert 'explicit' to boolean.
    5. Build the 'artist_norm' join key.
    """
    print("\nCleaning Spotify data...")
    out = df.copy()

    # 1. Remove pandas auto-index artifact
    if "Unnamed: 0" in out.columns:
        out = out.drop(columns=["Unnamed: 0"])

    # 2. Drop rows with missing critical identifiers
    before = len(out)
    out = out.dropna(subset=SPOTIFY_CRITICAL_COLS)
    dropped = before - len(out)
    if dropped:
        print(f"  Dropped {dropped} rows with null critical columns {SPOTIFY_CRITICAL_COLS}")

    # 3. Deduplicate — respect multi-genre structure
    before = len(out)
    out = out.drop_duplicates(subset=["track_id", "track_genre"])
    dropped = before - len(out)
    if dropped:
        print(f"  Dropped {dropped} exact duplicate (track_id, track_genre) rows")

    # 4. Normalize 'explicit' to boolean
    if "explicit" in out.columns:
        out["explicit"] = (
            out["explicit"].astype(str).str.strip().str.lower().eq("true")
        )

    # 5. Build join key
    out["artist_norm"] = _normalize_artist(out["artists"])

    print(f"  Spotify cleaned shape: {out.shape}")
    return out.reset_index(drop=True)


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Entry point called by main.py and the Airflow DAG."""
    return clean_spotify(df)


if __name__ == "__main__":
    from extract import extract_spotify
    raw = extract_spotify()
    clean = run(raw)
    print(f"\nFinal shape: {clean.shape}")