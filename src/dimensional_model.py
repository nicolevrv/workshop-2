import pandas as pd


# ── Constants ─────────────────────────────────────────────────────────────────
# Audio features stored in the fact table (continuous measurements per track)
FACT_AUDIO_COLS = [
    "danceability", "energy", "valence", "tempo", "loudness",
    "acousticness", "instrumentalness", "liveness", "speechiness",
]

# Track metadata stored in the fact table
FACT_METADATA_COLS = [
    "popularity", "duration_ms", "explicit",
]

# Grammy stats denormalized into the fact table for fast aggregation
# without always joining dim_artist
FACT_GRAMMY_DENORM_COLS = [
    "grammy_nominations", "grammy_wins",
]


# ── Dimension builders ────────────────────────────────────────────────────────

def _build_dim_artist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the artist dimension from the merged DataFrame.

    One row per unique artist_norm (normalized artist name used as join key).
    Grammy stats are included here so reports can filter/group by artist
    without touching the fact table.
    """
    cols = [
        "artist_norm", "artists",
        "grammy_nominations", "grammy_wins",
        "first_grammy_year", "last_grammy_year",
    ]
    # Only keep columns that actually exist in the input (defensive)
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
    Build the album dimension from the merged DataFrame.

    Grain: one row per (album_name, artist_norm) pair — the same album
    released by different artists counts as different albums.
    artist_norm is kept here so the fact table can resolve album_key
    via a composite lookup without ambiguity.
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
    """
    Build the genre dimension from the track_genre column.

    Handles multi-genre strings (comma-separated) by exploding them
    into individual genre rows. Returns a single 'unknown' row if the
    column is missing from the input.
    """
    if "track_genre" not in df.columns:
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
    Build the time dimension based solely on Grammy years.

    Design decisions:
    - Only Grammy-related years are modeled (first_grammy_year,
      last_grammy_year). Release years are not available in this dataset.
    - Sentinel row: time_key = 0, year = 0 represents artists that were
      never nominated. This avoids NULL foreign keys in the fact table
      and makes it easy to filter Grammy vs non-Grammy artists.
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

    dim = pd.DataFrame({"year": sorted(all_years)}).reset_index(drop=True)
    dim.insert(0, "time_key", range(1, len(dim) + 1))

    # Prepend sentinel row so time_key=0 always means "no Grammy history"
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
    Build the central fact table with all foreign key mappings.

    Grain: one row per (track_id, genre) combination — a track that
    belongs to multiple genres appears once per genre.

    FK strategy:
    - artist_key : direct map on artist_norm
    - album_key  : composite string key "album_name|artist_norm" to avoid
                   collisions between albums with the same name by different
                   artists. The join key is built on a COPY of dim_album so
                   the dimension table itself is never mutated (which would
                   cause MySQL to try to insert the helper column).
    - genre_key  : first genre token from track_genre (comma-separated)
    - time_key   : mapped from first_grammy_year; NA → 0 (sentinel)
    """
    fact = df.copy()

    # ── FK: artist ───────────────────────────────────────────────────────────
    # Simple 1:1 map — artist_norm is already unique in dim_artist
    artist_map = dict(zip(dim_artist["artist_norm"], dim_artist["artist_key"]))
    fact["artist_key"] = fact["artist_norm"].map(artist_map)

    # ── FK: album ────────────────────────────────────────────────────────────
    # Build a composite string key on a COPY of dim_album so that the
    # temporary helper column never leaks into the dimension DataFrame.
    # Mutating dim_album directly was the root cause of the
    # "Unknown column '_album_join_key'" MySQL error.
    dim_album_lookup = dim_album.copy()
    dim_album_lookup["_join"] = (
        dim_album_lookup["album_name"].astype(str).str.strip()
        + "|"
        + dim_album_lookup["artist_norm"].astype(str).str.strip()
    )
    album_map = dict(zip(dim_album_lookup["_join"], dim_album_lookup["album_key"]))

    # Build the same composite key on the fact side (no column added to fact)
    fact_album_join = (
        fact["album_name"].astype(str).str.strip()
        + "|"
        + fact["artist_norm"].astype(str).str.strip()
    )
    fact["album_key"] = fact_album_join.map(album_map)

    # ── FK: genre ────────────────────────────────────────────────────────────
    # Take the first genre token; tracks with no genre fall back to "unknown"
    if "track_genre" in fact.columns:
        fact_genre = (
            fact["track_genre"]
            .astype(str)
            .str.split(",")
            .str[0]
            .str.strip()
            .str.lower()
        )
    else:
        fact_genre = pd.Series("unknown", index=fact.index)

    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_key"]))
    fact["genre_key"] = fact_genre.map(genre_map)

    # ── FK: time ─────────────────────────────────────────────────────────────
    # Map first_grammy_year → time_key.
    # Tracks whose artist was never nominated get time_key = 0 (sentinel).
    time_map = dict(zip(dim_time["year"], dim_time["time_key"]))
    year_numeric = pd.to_numeric(fact["first_grammy_year"], errors="coerce")
    fact["time_key"] = year_numeric.map(time_map).fillna(0).astype(int)

    # ── FK validation ────────────────────────────────────────────────────────
    # All four FK columns must exist; nulls are logged but not fatal
    # (album_key can be null for tracks whose album isn't in the dimension —
    #  rare edge case from dirty data).
    required_fks = ["artist_key", "album_key", "genre_key", "time_key"]
    missing_fks = [fk for fk in required_fks if fk not in fact.columns]
    if missing_fks:
        raise KeyError(f"Missing FK columns after mapping: {missing_fks}")

    for fk in required_fks:
        null_count = fact[fk].isna().sum()
        if null_count > 0:
            print(f"  ⚠️  {fk}: {null_count:,} nulls ({null_count / len(fact) * 100:.1f}%)")

    # ── Select final columns ──────────────────────────────────────────────────
    # Only the columns defined in the constants at the top of this module
    # are included. No temporary helper columns survive into the output.
    base_cols = ["track_id", "track_name", "artist_key", "album_key", "genre_key", "time_key"]
    audio_cols    = [c for c in FACT_AUDIO_COLS         if c in fact.columns]
    metadata_cols = [c for c in FACT_METADATA_COLS      if c in fact.columns]
    grammy_cols   = [c for c in FACT_GRAMMY_DENORM_COLS if c in fact.columns]

    final_cols = base_cols + audio_cols + metadata_cols + grammy_cols

    missing_cols = [c for c in final_cols if c not in fact.columns]
    if missing_cols:
        raise KeyError(
            f"Columns missing from fact table: {missing_cols}\n"
            f"Available: {list(fact.columns)}"
        )

    fact = fact[final_cols].copy()

    # Sanity check: we should not lose more than 1% of rows
    if len(fact) < len(df) * 0.99:
        raise ValueError(
            f"Unexpected data loss: {len(df):,} input rows → {len(fact):,} output rows"
        )

    return fact.reset_index(drop=True)


# ── Star schema runner ────────────────────────────────────────────────────────

def run(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Orchestrate the full star schema build.

    Parameters
    ----------
    df : merged DataFrame produced by merge_data.run()

    Returns
    -------
    dict with keys: dim_artist, dim_album, dim_genre, dim_time, fact_track
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

    return tables