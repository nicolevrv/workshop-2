"""
dimensional_model.py - Task d: Dimensional Modeling (Star Schema)
Responsibility: transform the merged DataFrame into a star schema
suitable for analytical queries and dashboard KPIs.

Star Schema Design
------------------

    dim_time ──┐
    dim_artist ─┤
                ├── fact_track (grain: one row per track × genre)
    dim_album ──┤
    dim_genre ──┘

Grain
-----
One row in fact_track represents one unique (track_id, genre_key) pair.
Spotify publishes the same track under multiple genres; each is a valid
analytical dimension slice (e.g. "how does danceability distribute across
genres?").

Dimensions
----------
- dim_artist : artist identity + Grammy career summary
- dim_album  : album identity linked to its lead artist
- dim_genre  : genre taxonomy
- dim_time   : year-level time dimension (sourced from Grammy years + Spotify
               release years when available)

Measures in fact_track
-----------------------
Audio features  : danceability, energy, valence, tempo, loudness,
                  acousticness, instrumentalness, liveness, speechiness
Track metadata  : popularity, duration_ms, explicit
Grammy denorm   : grammy_nominations, grammy_wins
                  (denormalized onto fact for convenience — avoids extra
                   joins in dashboard queries that need both stream and
                   award metrics together)

Assumptions (for README)
-------------------------
- When an artist_norm maps to multiple raw artist strings (e.g. different
  capitalizations), the first encountered string is kept as artist_name.
- Tracks with no release_year are excluded from dim_time but kept in
  fact_track (time_key = NA).
- Grammy year columns in dim_artist can be NA for artists with no Grammy
  history; this is intentional (see merge_data.py).
"""

import pandas as pd


# ── Constants ─────────────────────────────────────────────────────────────────
FACT_AUDIO_COLS = [
    "danceability", "energy", "valence", "tempo", "loudness",
    "acousticness", "instrumentalness", "liveness", "speechiness",
]

FACT_METADATA_COLS = [
    "popularity", "duration_ms", "explicit",
]

FACT_GRAMMY_DENORM_COLS = [
    "grammy_nominations", "grammy_wins",
]


# ── Dimension builders ────────────────────────────────────────────────────────
def _build_dim_artist(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per normalized artist name.
    Includes Grammy career summary for KPIs like
    'top Grammy-winning artists by stream count'.
    """
    cols = [
        "artist_norm", "artists",
        "grammy_nominations", "grammy_wins",
        "first_grammy_year", "last_grammy_year",
    ]
    existing = [c for c in cols if c in df.columns]

    dim = (
        df[existing]
        .drop_duplicates(subset=["artist_norm"])
        .copy()
        .rename(columns={"artists": "artist_name"})
        .reset_index(drop=True)
    )
    dim.insert(0, "artist_key", range(1, len(dim) + 1))
    return dim


def _build_dim_album(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (album_name, artist_norm) pair.
    artist_norm is kept as a natural FK to dim_artist.
    """
    dim = (
        df[["album_name", "artist_norm"]]
        .drop_duplicates()
        .copy()
        .reset_index(drop=True)
    )
    dim.insert(0, "album_key", range(1, len(dim) + 1))
    return dim


def _build_dim_genre(df: pd.DataFrame) -> pd.DataFrame:
    if "track_genre" not in df.columns:
        print("⚠️ 'track_genre' column not found — using 'unknown'")
        return pd.DataFrame({"genre_key": [1], "genre_name": ["unknown"]})
    
    genres = (
        df["track_genre"]
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
        .str.lower()
        .unique()
    )
    dim = pd.DataFrame({"genre_name": genres}).reset_index(drop=True)
    dim.insert(0, "genre_key", range(1, len(dim) + 1))
    return dim


def _build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Year-level time dimension.
    Sources years from:
      1. 'release_year' if present in the merged Spotify data.
      2. 'first_grammy_year' and 'last_grammy_year' from Grammy history.
    Deduplicates and assigns surrogate keys.
    """
    year_series = []

    if "release_year" in df.columns:
        year_series.append(pd.to_numeric(df["release_year"], errors="coerce"))

    for col in ["first_grammy_year", "last_grammy_year"]:
        if col in df.columns:
            year_series.append(pd.to_numeric(df[col], errors="coerce"))

    if not year_series:
        print("  ⚠️  No year columns found — dim_time will be empty.")
        return pd.DataFrame({"time_key": pd.Series(dtype=int),
                             "year": pd.Series(dtype=int)})

    all_years = (
        pd.concat(year_series)
        .dropna()
        .astype(int)
        .unique()
    )
    dim = (
        pd.DataFrame({"year": sorted(all_years)})
        .reset_index(drop=True)
    )
    dim.insert(0, "time_key", range(1, len(dim) + 1))
    return dim


# ── Fact table builder ────────────────────────────────────────────────────────
def _build_fact_track(
    df: pd.DataFrame,
    dim_artist: pd.DataFrame,
    dim_album: pd.DataFrame,
    dim_genre: pd.DataFrame,
    dim_time: pd.DataFrame,
) -> pd.DataFrame:
    """
    Grain: one row per (track_id, genre_key).

    FK mapping
    ----------
    artist_key : via artist_norm
    album_key  : via (album_name, artist_norm)
    genre_key  : via first genre token of track_genre
    time_key   : via release_year (NA if not present)

    Measures
    --------
    Audio features, popularity, duration, explicit flag,
    Grammy nominations & wins (denormalized for dashboard convenience).
    """
    fact = df.copy()

    # ── FK: artist ────────────────────────────────────────────────────────────
    artist_map = dict(zip(dim_artist["artist_norm"], dim_artist["artist_key"]))
    fact["artist_key"] = fact["artist_norm"].map(artist_map)

    # ── FK: album ─────────────────────────────────────────────────────────────
    album_map = dict(
        zip(
            zip(dim_album["album_name"], dim_album["artist_norm"]),
            dim_album["album_key"],
        )
    )
    fact["album_key"] = list(zip(fact["album_name"], fact["artist_norm"]))
    fact["album_key"] = fact["album_key"].map(album_map)

    # ── FK: genre (first genre token only for the FK) ─────────────────────────
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_key"]))
    if "track_genre" in fact.columns:
        fact["genre_name_norm"] = (
            fact["track_genre"]
            .astype(str)
            .str.split(",")
            .str[0]
            .str.strip()
            .str.lower()
        )
    else:
        fact["genre_name_norm"] = "unknown"
    fact["genre_key"] = fact["genre_name_norm"].map(genre_map)

    # ── FK: time ──────────────────────────────────────────────────────────────
    if not dim_time.empty and "release_year" in fact.columns:
        time_map = dict(zip(dim_time["year"], dim_time["time_key"]))
        fact["release_year_int"] = pd.to_numeric(
            fact["release_year"], errors="coerce"
        ).astype("Int64")
        fact["time_key"] = fact["release_year_int"].map(time_map)
    else:
        fact["time_key"] = pd.NA

    # ── Select final columns ──────────────────────────────────────────────────
    base_cols = ["track_id", "track_name", "artist_key", "album_key",
                 "genre_key", "time_key"]

    audio_cols     = [c for c in FACT_AUDIO_COLS     if c in fact.columns]
    metadata_cols  = [c for c in FACT_METADATA_COLS  if c in fact.columns]
    grammy_cols    = [c for c in FACT_GRAMMY_DENORM_COLS if c in fact.columns]

    final_cols = base_cols + audio_cols + metadata_cols + grammy_cols
    fact = fact[final_cols].drop_duplicates(subset=["track_id", "genre_key"])

    return fact.reset_index(drop=True)


# ── Star schema runner ────────────────────────────────────────────────────────
def run(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Build all star schema tables from the merged DataFrame.

    Parameters
    ----------
    df : merged DataFrame from merge_data.merge_spotify_grammys

    Returns
    -------
    dict with keys:
        'dim_artist', 'dim_album', 'dim_genre', 'dim_time', 'fact_track'
    """
    print("\n" + "=" * 70)
    print("TASK d — DIMENSIONAL MODELING (Star Schema)")
    print("=" * 70)

    dim_artist = _build_dim_artist(df)
    dim_album  = _build_dim_album(df)
    dim_genre  = _build_dim_genre(df)
    dim_time   = _build_dim_time(df)
    fact_track = _build_fact_track(df, dim_artist, dim_album, dim_genre, dim_time)

    tables = {
        "dim_artist": dim_artist,
        "dim_album":  dim_album,
        "dim_genre":  dim_genre,
        "dim_time":   dim_time,
        "fact_track": fact_track,
    }

    print("\nStar schema tables:")
    for name, table in tables.items():
        print(f"  {name:<15} {table.shape[0]:>7,} rows × {table.shape[1]} columns")

    # ── Referential integrity checks ──────────────────────────────────────────
    print("\nReferential integrity checks:")

    orphan_artist = fact_track["artist_key"].isna().sum()
    orphan_album  = fact_track["album_key"].isna().sum()
    orphan_genre  = fact_track["genre_key"].isna().sum()

    for label, count in [
        ("Fact rows with null artist_key", orphan_artist),
        ("Fact rows with null album_key",  orphan_album),
        ("Fact rows with null genre_key",  orphan_genre),
    ]:
        flag = " ⚠️" if count > 0 else " ✔"
        print(f"  {label:<40} {count:>6,}{flag}")

    return tables


if __name__ == "__main__":
    from extract import run as extract_run
    from transform_spotify import run as transform_spotify_run
    from transform_grammys import run as transform_grammys_run
    from merge_data import run as merge_run

    spotify_raw, grammy_raw = extract_run()
    spotify_clean = transform_spotify_run(spotify_raw)
    grammy_clean = transform_grammys_run(grammy_raw)
    merged = merge_run(spotify_clean, grammy_clean)
    tables = run(merged)