"""
merge_data.py - Task c: Dataset Merging

Responsibility
--------------
Merge the cleaned Spotify and Grammy datasets into a single enriched
DataFrame ready for dimensional modeling.

Merge strategy
--------------
- Join key: 'artist_norm' (normalized artist name built in transform.py).
- Join type: LEFT join on Spotify — every track is preserved regardless
  of Grammy presence. Tracks with no Grammy history get 0 nominations/wins
  and NA for year fields (not 0, since year=0 is semantically meaningless).
- Grammy records with artist_norm == 'unknown' are excluded from
  aggregation to avoid polluting the 'unknown' bucket with unrelated nominees.
- The Grammy side is aggregated to one row per artist before joining,
  so the merge never fans-out Spotify rows.

Assumptions
-----------
- A Grammy nomination is counted once per row in the Grammy dataset for
  that artist, regardless of whether they won.
- 'first_grammy_year' and 'last_grammy_year' are set to NA (not 0) for
  artists with no Grammy history, to distinguish "no data" from year zero.
- Only the first-billed Spotify artist is used as join key; collaborators
  listed after ';' do not inherit Grammy stats from the lead artist.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


# ── Grammy aggregation────────────────────────────────────────────────────────
def _build_grammy_stats(grammy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate Grammy dataset into one summary row per artist.
    Excludes 'unknown' artists to avoid merging noise.

    Returns a DataFrame with columns:
        artist_norm, grammy_nominations, grammy_wins,
        first_grammy_year, last_grammy_year
    """
    unknown_count = (grammy_df["artist_norm"] == "unknown").sum()
    total_count = len(grammy_df)
    logger.info(f"Grammy records: {total_count:,} total, {unknown_count:,} unknown (excluded)")

    usable = grammy_df[grammy_df["artist_norm"] != "unknown"].copy()

    if usable.empty:
        logger.warning("No usable Grammy records after filtering 'unknown' artists.")
        return pd.DataFrame(columns=[
            "artist_norm",
            "grammy_nominations",
            "grammy_wins",
            "first_grammy_year",
            "last_grammy_year",
        ])

    grammy_stats = (
        usable.groupby("artist_norm", as_index=False)
        .agg(
            grammy_nominations=("artist_norm", "size"),
            grammy_wins=("winner", "sum"),
            first_grammy_year=("year", "min"),
            last_grammy_year=("year", "max"),
        )
    )

    grammy_stats["grammy_nominations"] = grammy_stats["grammy_nominations"].astype(int)
    grammy_stats["grammy_wins"] = grammy_stats["grammy_wins"].astype(int)

    # Nullable Int64 — NA means "no Grammy history", not year zero
    for col in ["first_grammy_year", "last_grammy_year"]:
        grammy_stats[col] = grammy_stats[col].astype("Int64")

    return grammy_stats


# ── Merge─────────────────────────────────────────────────────────────────────
def merge_spotify_grammys(
    spotify_df: pd.DataFrame,
    grammys_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join Spotify tracks with aggregated Grammy stats per artist.

    Parameters
    ----------
    spotify_df  : cleaned Spotify DataFrame (from transform.clean_spotify)
    grammys_df  : cleaned Grammy DataFrame (from transform.clean_grammys)

    Returns
    -------
    pd.DataFrame
        Enriched dataset with all Spotify columns plus Grammy stats.
        Tracks with no Grammy history have:
            grammy_nominations = 0
            grammy_wins        = 0
            first_grammy_year  = <NA>
            last_grammy_year   = <NA>
    """
    print("\n" + "=" * 70)
    print("TASK c — MERGE")
    print("=" * 70)

    logger.info(f"Input shapes — Spotify: {spotify_df.shape}, Grammys: {grammys_df.shape}")

    # ── Build Grammy summary
    grammy_stats = _build_grammy_stats(grammys_df)
    print(f"\n✓ Grammy stats built: {len(grammy_stats):,} unique artists")

    # ── LEFT JOIN — preserves all Spotify tracks
    merged = spotify_df.merge(grammy_stats, on="artist_norm", how="left")

    # ── Fan-out guard: a correct LEFT JOIN must never add rows
    if len(merged) != len(spotify_df):
        raise ValueError(
            f"Fan-out detected after merge: "
            f"{len(spotify_df):,} Spotify rows → {len(merged):,} merged rows. "
            f"Check for duplicate artist_norm values in grammy_stats."
        )

    # ── Fill nulls for non-Grammy artists (numeric only)
    merged["grammy_nominations"] = merged["grammy_nominations"].fillna(0).astype(int)
    merged["grammy_wins"] = merged["grammy_wins"].fillna(0).astype(int)

    # Year columns stay as NA — do NOT fill with 0
    for col in ["first_grammy_year", "last_grammy_year"]:
        if col in merged.columns:
            merged[col] = merged[col].astype("Int64")

    # ── Quality report
    total_tracks = len(merged)
    with_grammys = (merged["grammy_nominations"] > 0).sum()
    without_grammys = total_tracks - with_grammys
    match_rate = (with_grammys / total_tracks) * 100 if total_tracks else 0

    print("\n" + "-" * 50)
    print("MERGE QUALITY REPORT")
    print("-" * 50)
    print(f"  Total Spotify tracks   : {total_tracks:,}")
    print(f"  With Grammy data       : {with_grammys:,} ({match_rate:.2f}%)")
    print(f"  Without Grammy data    : {without_grammys:,} ({100 - match_rate:.2f}%)")
    print(f"  Unique Grammy artists  : {len(grammy_stats):,}")

    if match_rate < 1:
        logger.warning("Match rate < 1% — verify that artist_norm is built consistently in both datasets.")

    print("-" * 50)

    return merged


# ── Runner────────────────────────────────────────────────────────────────────
def run(spotify_clean: pd.DataFrame, grammy_clean: pd.DataFrame) -> pd.DataFrame:
    """Entry point called by main.py and the Airflow DAG."""
    return merge_spotify_grammys(spotify_clean, grammy_clean)


if __name__ == "__main__":
    from extract import run as extract_run
    from transform import run as transform_run

    spotify_raw, grammy_raw = extract_run()
    spotify_clean, grammy_clean = transform_run(spotify_raw, grammy_raw)

    merged = run(spotify_clean, grammy_clean)
    print(f"\nFinal merged shape: {merged.shape}")
    print(merged.head(3).to_string(index=False))