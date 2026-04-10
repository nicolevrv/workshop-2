"""
main.py - Full ETL Pipeline Runner
Responsibility: orchestrate the complete ETL pipeline locally.
This file is the local equivalent of the Airflow DAG — useful for 
development, testing, and running the pipeline without Airflow.

Pipeline steps:
    a. Extract — load raw CSVs into DataFrames
    b. Transform — clean and normalize each dataset (separado por fuente)
    c. Merge — join Spotify + Grammys by artist_norm
    d. Model — build star schema tables
    e. Load DW — create MySQL schema and insert tables
    f. Store — save merged CSV to Google Drive (optional)

Usage:
    # Run full pipeline
    python src/main.py
    
    # Skip Google Drive upload (e.g., no credentials locally)
    python src/main.py --skip-drive
    
    # Skip DW load (e.g., no MySQL locally)
    python src/main.py --skip-dw
    
    # Save star schema tables as local CSVs for inspection
    python src/main.py --export-csv
"""

import argparse
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Project root on sys.path so sibling imports work ─────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from extract import run as extract_run
from transform_spotify import run as transform_spotify_run
from transform_grammys import run as transform_grammys_run
from merge_data import run as merge_run
from dimensional_model import run as dim_run
from load_dw import load_star_schema, save_to_google_drive

# ── Output paths ────────────────────────────────────────────────────────────
PROCESSED_PATH = ROOT / "data" / "processed"
STAR_PATH = ROOT / "data" / "star_schema"


# ── Optional local CSV export ───────────────────────────────────────────────
def export_csv(tables: dict, base_path: Path) -> None:
    """Save all star schema tables to CSV files under base_path."""
    print("\n" + "=" * 70)
    print("OPTIONAL — Exporting star schema tables to CSV")
    print("=" * 70)
    
    base_path.mkdir(parents=True, exist_ok=True)
    
    for name, df in tables.items():
        path = base_path / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"Saved → {path} ({len(df):,} rows)")


# ── Step timer ──────────────────────────────────────────────────────────────
class _Timer:
    def __init__(self, label: str):
        self.label = label
    
    def __enter__(self):
        self._start = time.perf_counter()
        return self
    
    def __exit__(self, *_):
        elapsed = time.perf_counter() - self._start
        print(f"⏱ {self.label} completed in {elapsed:.1f}s")


# ── Main pipeline ───────────────────────────────────────────────────────────
def run(skip_dw: bool = False, skip_drive: bool = False, export: bool = False) -> None:
    """
    Execute the full ETL pipeline.
    
    Parameters
    ----------
    skip_dw : if True, skips the MySQL Data Warehouse load step.
    skip_drive : if True, skips the Google Drive CSV upload step.
    export : if True, saves star schema tables as local CSVs.
    """
    pipeline_start = time.perf_counter()
    
    print("\n" + "█" * 70)
    print("  RUNNING FULL ETL PIPELINE")
    print("█" * 70)
    
    # ── a. Extract ──────────────────────────────────────────────────────────
    with _Timer("Extract"):
        spotify_df, grammy_df = extract_run()
    
    # ── b. Transform (separado por fuente de datos) ──────────────────────────
    with _Timer("Transform Spotify (CSV)"):
        spotify_clean = transform_spotify_run(spotify_df)
    
    with _Timer("Transform Grammys (DB)"):
        grammy_clean = transform_grammys_run(grammy_df)
    
    # ── c. Merge ────────────────────────────────────────────────────────────
    with _Timer("Merge"):
        merged_df = merge_run(spotify_clean, grammy_clean)
    
    # ── d. Dimensional modeling ─────────────────────────────────────────────
    with _Timer("Dimensional modeling"):
        tables = dim_run(merged_df)
    
    # ── e. Load Data Warehouse ───────────────────────────────────────────────
    if skip_dw:
        print("\n⏭  Skipping DW load (--skip-dw)")
    else:
        with _Timer("Load DW"):
            load_star_schema(tables)
    
    # ── f. Save to Google Drive ──────────────────────────────────────────────
    if skip_drive:
        print("⏭  Skipping Google Drive upload (--skip-drive)")
    else:
        with _Timer("Google Drive upload"):
            save_to_google_drive(merged_df)
    
    # ── Optional local CSV export ────────────────────────────────────────────
    if export:
        with _Timer("CSV export"):
            export_csv(tables, STAR_PATH)
    
    # ── Summary ──────────────────────────────────────────────────────────────
    total = time.perf_counter() - pipeline_start
    print("\n" + "█" * 70)
    print(f"  PIPELINE COMPLETED in {total:.1f}s ✔")
    print("█" * 70)
    
    print(f"\n  Spotify tracks   : {len(spotify_clean):,}")
    print(f"  Grammy records   : {len(grammy_clean):,}")
    print(f"  Merged rows      : {len(merged_df):,}")
    print()
    
    for name, df in tables.items():
        print(f"  {name:<20} {len(df):>7,} rows")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Spotify × Grammys ETL pipeline."
    )
    parser.add_argument(
        "--skip-dw",
        action="store_true",
        help="Skip the MySQL Data Warehouse load step.",
    )
    parser.add_argument(
        "--skip-drive",
        action="store_true",
        help="Skip the Google Drive CSV upload step.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Save star schema tables as local CSVs under data/star_schema/.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        skip_dw=args.skip_dw,
        skip_drive=args.skip_drive,
        export=args.export_csv,
    )