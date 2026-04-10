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
    Time dimension basada SOLO en años de Grammy.

    Sentinel:
        0 = artista nunca nominado
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

    # sentinel row
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
    """Build fact table with proper grain preservation."""
    
    fact = df.copy()
    
    # ── FK mappings (tu código actual está bien)
    artist_map = dict(zip(dim_artist["artist_norm"], dim_artist["artist_key"]))
    fact["artist_key"] = fact["artist_norm"].map(artist_map)
    
    # ... resto de FKs ...
    
    # ── Select final columns (SIN drop_duplicates agresivo)
    base_cols = [
        "track_id", "track_name",
        "artist_key", "album_key", 
        "genre_key", "time_key"
    ]
    
    audio_cols = [c for c in FACT_AUDIO_COLS if c in fact.columns]
    metadata_cols = [c for c in FACT_METADATA_COLS if c in fact.columns]
    grammy_cols = [c for c in FACT_GRAMMY_DENORM_COLS if c in fact.columns]
    
    final_cols = base_cols + audio_cols + metadata_cols + grammy_cols
    
    # ✅ SOLO eliminar duplicados exactos si existen (no por subset)
    fact = fact[final_cols].drop_duplicates()
    
    # Validación crítica
    original_tracks = len(df)
    final_tracks = len(fact)
    
    if final_tracks < original_tracks * 0.99:
        raise ValueError(
            f"Data loss in fact table: {original_tracks:,} → {final_tracks:,} "
            f"({(1-final_tracks/original_tracks)*100:.1f}% lost)"
        )
    
    return fact.reset_index(drop=True)


# ── Star schema runner ────────────────────────────────────────────────────────
def run(df: pd.DataFrame) -> dict[str, pd.DataFrame]:

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