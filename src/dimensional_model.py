"""
dimensional_model.py - Task d: Dimensional Modeling (Star Schema)

Responsibility: Build star schema tables from merged Spotify + Grammy data.
Grain: One row per (track_id, genre) combination in fact_track.
"""

import pandas as pd


# ── Constants ─────────────────────────────────────────────────────────────────
FACT_AUDIO_COLS = [
    "danceability", "energy", "valence", "tempo", "loudness",
    "acousticness", "instrumentalness", "liveness", "speechiness",
]

FACT_METADATA_COLS = ["popularity", "duration_ms", "explicit"]
FACT_GRAMMY_DENORM_COLS = ["grammy_nominations", "grammy_wins"]


# ── Dimension builders ─────────────────────────────────────────────────────────

def _build_dim_artist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build artist dimension. One row per unique artist_norm.
    Includes denormalized Grammy stats for fast reporting.
    """
    cols = ["artist_norm", "artists", "grammy_nominations", "grammy_wins",
            "first_grammy_year", "last_grammy_year"]
    existing = [c for c in cols if c in df.columns]

    dim = (
        df[existing]
        .drop_duplicates(subset=["artist_norm"])
        .rename(columns={"artists": "artist_name"})
        .reset_index(drop=True)
    )
    dim.insert(0, "artist_key", range(1, len(dim) + 1))
    return dim


def _build_dim_album(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build album dimension. Grain: one row per (album_name, artist_norm).
    """
    dim = (
        df[["album_name", "artist_norm"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.insert(0, "album_key", range(1, len(dim) + 1))
    return dim


def _build_dim_genre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build genre dimension from unique track_genre values.
    Input df has one genre per row (already normalized).
    """
    if "track_genre" not in df.columns:
        return pd.DataFrame({"genre_key": [1], "genre_name": ["unknown"]})

    # Get unique genres (no explode needed - already one genre per row)
    unique_genres = df["track_genre"].dropna().unique()

    dim = pd.DataFrame({"genre_name": sorted(unique_genres)})
    dim.insert(0, "genre_key", range(1, len(dim) + 1))
    return dim


def _build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build time dimension from Grammy years.
    Sentinel row (time_key=0, year=0) for artists with no Grammy history.
    """
    year_series = []

    for col in ["first_grammy_year", "last_grammy_year"]:
        if col in df.columns:
            year_series.append(pd.to_numeric(df[col], errors="coerce"))

    if not year_series:
        return pd.DataFrame({"time_key": [0], "year": [0]})

    all_years = (
        pd.concat(year_series)
        .dropna()
        .astype(int)
        .unique()
    )

    dim = pd.DataFrame({"year": sorted(all_years)})
    dim.insert(0, "time_key", range(1, len(dim) + 1))

    # Sentinel row for "no Grammy history"
    sentinel = pd.DataFrame({"time_key": [0], "year": [0]})
    dim = pd.concat([sentinel, dim], ignore_index=True)

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
    Build fact table with foreign key mappings.
    Grain: ONE ROW PER (track_id, track_genre) - already exploded in input.
    """
    print(f"\n[_build_fact_track] Input: {len(df):,} rows")

    fact = df.copy()

    # ── FK: artist ───────────────────────────────────────────────────────────
    artist_map = dict(zip(dim_artist["artist_norm"], dim_artist["artist_key"]))
    fact["artist_key"] = fact["artist_norm"].map(artist_map)

    # ── FK: album ────────────────────────────────────────────────────────────
    # Composite key: album_name + artist_norm
    dim_album_lookup = dim_album.copy()
    dim_album_lookup["_join"] = (
        dim_album_lookup["album_name"].astype(str).str.strip()
        + "|"
        + dim_album_lookup["artist_norm"].astype(str).str.strip()
    )
    album_map = dict(zip(dim_album_lookup["_join"], dim_album_lookup["album_key"]))

    fact_album_join = (
        fact["album_name"].astype(str).str.strip()
        + "|"
        + fact["artist_norm"].astype(str).str.strip()
    )
    fact["album_key"] = fact_album_join.map(album_map)

    # ── FK: genre ────────────────────────────────────────────────────────────
    # Direct map - genres already normalized and exploded
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_key"]))
    fact["genre_key"] = fact["track_genre"].map(genre_map)

    # ── FK: time ─────────────────────────────────────────────────────────────
    time_map = dict(zip(dim_time["year"], dim_time["time_key"]))
    year_numeric = pd.to_numeric(fact["first_grammy_year"], errors="coerce")
    fact["time_key"] = year_numeric.map(time_map).fillna(0).astype(int)

    # ── Validation ────────────────────────────────────────────────────────────
    required_fks = ["artist_key", "album_key", "genre_key", "time_key"]
    missing_fks = [fk for fk in required_fks if fk not in fact.columns]
    if missing_fks:
        raise KeyError(f"Missing FK columns: {missing_fks}")

    for fk in required_fks:
        null_count = fact[fk].isna().sum()
        if null_count > 0:
            print(f"  ⚠️  {fk}: {null_count:,} nulls ({null_count/len(fact)*100:.1f}%)")

    # ── Select final columns ───────────────────────────────────────────────────
    base_cols = ["track_id", "track_name", "artist_key", "album_key", "genre_key", "time_key"]
    audio_cols = [c for c in FACT_AUDIO_COLS if c in fact.columns]
    metadata_cols = [c for c in FACT_METADATA_COLS if c in fact.columns]
    grammy_cols = [c for c in FACT_GRAMMY_DENORM_COLS if c in fact.columns]

    final_cols = base_cols + audio_cols + metadata_cols + grammy_cols

    missing_cols = [c for c in final_cols if c not in fact.columns]
    if missing_cols:
        raise KeyError(f"Missing columns: {missing_cols}. Available: {list(fact.columns)}")

    fact = fact[final_cols].copy()

    # ── CRITICAL: Row count validation ───────────────────────────────────────
    input_rows = len(df)
    output_rows = len(fact)

    print(f"[_build_fact_track] Output: {output_rows:,} rows")

    # Allow 0% loss (exact match) or small gain (if any)
    if output_rows < input_rows * 0.99:
        raise ValueError(
            f"DATA LOSS: {input_rows:,} input → {output_rows:,} output "
            f"({(1 - output_rows/input_rows)*100:.1f}% lost)"
        )
    if output_rows > input_rows * 1.01:
        raise ValueError(
            f"ROW MULTIPLICATION: {input_rows:,} input → {output_rows:,} output "
            f"({(output_rows/input_rows - 1)*100:.1f}% gained - check for duplicates)"
        )

    return fact.reset_index(drop=True)


# ── Star schema runner ─────────────────────────────────────────────────────────

def run(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Orchestrate full star schema build.

    Returns
    -------
    dict with keys: dim_artist, dim_album, dim_genre, dim_time, fact_track
    """
    print("\n" + "=" * 70)
    print("TASK d — DIMENSIONAL MODELING (Star Schema)")
    print("=" * 70)
    print(f"Input merged data: {len(df):,} rows")

    dim_artist = _build_dim_artist(df)
    dim_album = _build_dim_album(df)
    dim_genre = _build_dim_genre(df)
    dim_time = _build_dim_time(df)

    # Debug dimensions before fact table
    print(f"\nDimensions built:")
    print(f"  dim_artist: {len(dim_artist):,} rows")
    print(f"  dim_album:  {len(dim_album):,} rows")
    print(f"  dim_genre:  {len(dim_genre):,} rows")
    print(f"  dim_time:   {len(dim_time):,} rows")

    fact_track = _build_fact_track(df, dim_artist, dim_album, dim_genre, dim_time)

    tables = {
        "dim_artist": dim_artist,
        "dim_album": dim_album,
        "dim_genre": dim_genre,
        "dim_time": dim_time,
        "fact_track": fact_track,
    }

    print("\nStar schema summary:")
    for name, table in tables.items():
        print(f"  {name:<15} {table.shape[0]:>7,} rows × {table.shape[1]} columns")

    return tables