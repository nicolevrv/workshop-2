"""
extract.py - Task a: Data Extraction

Responsibility: load raw datasets into DataFrames and apply minimal type casting.
No cleaning or transformations are performed here.

This is the ONLY place where CSV files are read.
"""

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent

SPOTIFY_PATH = ROOT / "data" / "raw" / "spotify_dataset.csv"
GRAMMY_PATH  = ROOT / "data" / "raw" / "the_grammy_awards.csv"


def run(spotify_path=None, grammy_path=None):
    """
    Load raw Spotify and Grammy datasets.

    All columns are read as strings first to preserve original values.
    Minimal type casting is applied where appropriate.
    """
    sp_path = spotify_path or SPOTIFY_PATH
    gr_path = grammy_path or GRAMMY_PATH

    print("\n" + "=" * 70)
    print("TASK a — EXTRACTION")
    print("=" * 70)

    # ── Load as strings to avoid unwanted coercion ─────────────────────────────
    spotify_df = pd.read_csv(sp_path, dtype=str)
    grammy_df  = pd.read_csv(gr_path, dtype=str)

    # ── Minimal type casting (Spotify only where it matters) ───────────────────
    numeric_cols = [
        "popularity", "duration_ms", "danceability", "energy",
        "loudness", "speechiness", "acousticness",
        "instrumentalness", "liveness", "valence", "tempo"
    ]

    for col in numeric_cols:
        if col in spotify_df.columns:
            spotify_df[col] = pd.to_numeric(spotify_df[col], errors="coerce")

    # Optional: cast year in Grammys
    if "year" in grammy_df.columns:
        grammy_df["year"] = pd.to_numeric(grammy_df["year"], errors="coerce")

    # ── Info prints ────────────────────────────────────────────────────────────
    print(f"\nSpotify file : {sp_path}")
    print(f"Shape        : {spotify_df.shape[0]:,} rows × {spotify_df.shape[1]} columns")

    print(f"\nGrammy file  : {gr_path}")
    print(f"Shape        : {grammy_df.shape[0]:,} rows × {grammy_df.shape[1]} columns")

    print("\nSpotify sample:")
    print(spotify_df.head(3).to_string(index=False))

    print("\nGrammy sample:")
    print(grammy_df.head(3).to_string(index=False))

    return spotify_df, grammy_df


if __name__ == "__main__":
    run()