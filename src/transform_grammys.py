"""
transform_grammys.py - Task b.2: Data Cleaning for Grammys (DB)
Responsibility: clean and normalize Grammy dataset from MySQL.
"""

import pandas as pd

GRAMMY_DROP_COLS = ["workers", "img"]
GRAMMY_TEXT_NORMALIZE_COLS = ["category", "nominee"]


def _normalize_artist(series: pd.Series) -> pd.Series:
    """
    Build a lowercase, stripped join key from artist string.
    For Grammys: single artist per row (already single in source).
    """
    return (
        series.fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )


def clean_grammys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the raw Grammy Awards DataFrame.

    Steps
    -----
    1. Drop columns with no analytical value ('workers', 'img').
    2. Replace empty strings with NA for uniform null handling.
    3. Parse date columns to datetime.
    4. Convert 'winner' to boolean.
    5. Lowercase and strip key text columns.
    6. Build the 'artist_norm' join key.
    7. Log null rates for key columns.
    """
    print("\nCleaning Grammys data...")
    out = df.copy()

    # 1. Drop low-value columns
    out = out.drop(columns=GRAMMY_DROP_COLS, errors="ignore")

    # 2. Empty strings → NA
    text_cols = out.select_dtypes(include="object").columns
    out[text_cols] = out[text_cols].replace("", pd.NA)

    # 3. Parse dates
    for date_col in ["published_at", "updated_at"]:
        if date_col in out.columns:
            out[date_col] = (
                pd.to_datetime(out[date_col], errors="coerce", utc=True)
                .dt.tz_localize(None)
            )

    # 4. Boolean winner flag
    out["winner"] = (
        out["winner"].astype(str).str.strip().str.lower().eq("true")
    )

    # 5. Normalize text columns
    for col in GRAMMY_TEXT_NORMALIZE_COLS:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip().str.lower()

    # 6. Build join key
    out["artist_norm"] = _normalize_artist(out["artist"])

    # 7. Null report for key columns
    key_cols = ["artist", "artist_norm", "category", "nominee", "year", "winner"]
    existing = [c for c in key_cols if c in out.columns]
    null_rates = out[existing].isnull().mean().mul(100).round(2)
    print("  Null rates (%) for key columns:")
    for col, rate in null_rates.items():
        flag = " ⚠️" if rate > 10 else ""
        print(f"    {col:<20} {rate:>6.2f}%{flag}")

    print(f"  Grammys cleaned shape: {out.shape}")
    return out.reset_index(drop=True)


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Entry point called by main.py and the Airflow DAG."""
    return clean_grammys(df)


if __name__ == "__main__":
    from extract import extract_grammys
    raw = extract_grammys()
    clean = run(raw)
    print(f"\nFinal shape: {clean.shape}")