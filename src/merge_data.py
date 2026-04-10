"""
merge_data.py - Task c: Dataset Merging
Responsibility: merge the cleaned Spotify and Grammy datasets into a
single enriched DataFrame ready for dimensional modeling.

Merge strategy
--------------
- Join key: 'artist_norm' (normalized artist name built in transform.py).
- Join type: LEFT join on Spotify — every track is preserved regardless
  of Grammy presence. Tracks with no Grammy history get 0 nominations/wins
  and NA for year fields (not 0, since year=0 is semantically meaningless).
- Grammy records with artist_norm == 'unknown' are excluded from
  aggregation to avoid polluting the 'unknown' bucket with unrelated nominees.
- The Grammy side is aggregated to one row per artist before joining,
  so the merge never fan-outs Spotify rows.

Assumptions (for README)
------------------------
- A Grammy nomination is counted once per row in the Grammy dataset for
  that artist, regardless of whether they won.
- 'first_grammy_year' and 'last_grammy_year' are set to NA (not 0) for
  artists with no Grammy history, to distinguish "no data" from year zero.
- Only the first-billed Spotify artist is used as join key; collaborators
  listed after ';' do not inherit Grammy stats from the lead artist.
"""

import pandas as pd


# ── Grammy aggregation ────────────────────────────────────────────────────────
def _build_grammy_stats(grammy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate Grammy data to one summary row per artist.

    Excludes 'unknown' artists to avoid merging noise.
    Returns a DataFrame with columns:
        artist_norm, grammy_nominations, grammy_wins,
        first_grammy_year, last_grammy_year
    """
    usable = grammy_df[grammy_df["artist_norm"] != "unknown"].copy()

    if usable.empty:
        print("  ⚠️  No usable Grammy records after filtering 'unknown' artists.")
        return pd.DataFrame(columns=[
            "artist_norm", "grammy_nominations", "grammy_wins",
            "first_grammy_year", "last_grammy_year",
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

    # Cast win count to int (sum of booleans can be float if NAs exist)
    grammy_stats["grammy_wins"] = grammy_stats["grammy_wins"].astype(int)

    # Keep year columns as nullable Int64 — NA means "no Grammy history",
    # which is semantically distinct from year 0.
    for col in ["first_grammy_year", "last_grammy_year"]:
        grammy_stats[col] = grammy_stats[col].astype("Int64")

    return grammy_stats


# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_spotify_grammys(
    spotify_df: pd.DataFrame,
    grammys_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join Spotify tracks with aggregated Grammy stats per artist.

    Parameters
    ----------
    spotify_df : cleaned Spotify DataFrame (from transform.clean_spotify)
    grammys_df : cleaned Grammy DataFrame (from transform.clean_grammys)

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

    # ── Build Grammy summary ──────────────────────────────────────────────────
    grammy_stats = _build_grammy_stats(grammys_df)
    print(f"\n  Grammy stats built: {len(grammy_stats):,} unique artists")

    # ── Left join ────────────────────────────────────────────────────────────
    merged = spotify_df.merge(grammy_stats, on="artist_norm", how="left")

    # ── Fill nulls for non-Grammy artists ────────────────────────────────────
    merged["grammy_nominations"] = merged["grammy_nominations"].fillna(0).astype(int)
    merged["grammy_wins"]        = merged["grammy_wins"].fillna(0).astype(int)

    # Year columns stay as NA (not 0) for artists with no Grammy history
    for col in ["first_grammy_year", "last_grammy_year"]:
        if col in merged.columns:
            merged[col] = merged[col].astype("Int64")

    # ── Merge quality report ─────────────────────────────────────────────────
    total          = len(merged)
    with_grammys   = (merged["grammy_nominations"] > 0).sum()
    without        = total - with_grammys
    match_rate     = with_grammys / total * 100 if total else 0

    print(f"\n  Merged shape          : {merged.shape}")
    print(f"  Tracks with Grammy data : {with_grammys:,} ({match_rate:.1f}%)")
    print(f"  Tracks without Grammy   : {without:,} ({100 - match_rate:.1f}%)")

    # Warn if match rate is suspiciously low — could indicate a join key problem
    if match_rate < 5:
        print(
            "  ⚠️  Match rate below 5% — verify that artist_norm is built "
            "consistently in both datasets."
        )

    return merged


# ── Runner ────────────────────────────────────────────────────────────────────
def run(spotify_clean: pd.DataFrame, grammy_clean: pd.DataFrame) -> pd.DataFrame:
    """Entry point called by main.py and the Airflow DAG."""
    return merge_spotify_grammys(spotify_clean, grammy_clean)


if __name__ == "__main__":
    from extract import run as extract_run
    from transform_spotify import run as transform_run

    spotify_raw, grammy_raw = extract_run()
    spotify_clean, grammy_clean = transform_run(spotify_raw, grammy_raw)
    merged = run(spotify_clean, grammy_clean)
    print(f"\nFinal merged shape: {merged.shape}")
    print(merged.head(3).to_string(index=False))