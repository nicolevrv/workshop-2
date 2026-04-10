"""
load_grammys_db.py - One-time Setup Script
Responsibility: load the raw Grammy Awards CSV into MySQL so that
Airflow can extract it from the database instead of reading the file directly.

This script runs ONCE before the ETL pipeline / DAG is triggered.
It is NOT part of the DAG itself.

Usage
-----
    python load_grammys_db.py

    # Force reload even if table already has data
    python load_grammys_db.py --force

Environment variables (set in .env)
-------------------------------------
    DB_HOST      (default: localhost)
    DB_USER      (default: etl_user)
    DB_PASSWORD
    DB_PORT      (default: 3306)
    DB_NAME      (default: music_dw)
"""

import argparse
import os
import sys
from pathlib import Path

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
GRAMMY_PATH = ROOT / "data" / "raw" / "the_grammy_awards.csv"

# ── DB config ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "user":     os.environ.get("DB_USER",     "etl_user"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "port":     int(os.environ.get("DB_PORT", 3306)),
}
DB_NAME      = os.environ.get("DB_NAME", "music_dw")
GRAMMYS_TABLE = "grammys_raw"

# ── DDL ───────────────────────────────────────────────────────────────────────
CREATE_DB_SQL = f"""
    CREATE DATABASE IF NOT EXISTS {DB_NAME}
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci;
"""

CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {GRAMMYS_TABLE} (
        id              INT             NOT NULL AUTO_INCREMENT,
        year            SMALLINT,
        title           VARCHAR(500),
        published_at    VARCHAR(100),
        updated_at      VARCHAR(100),
        category        VARCHAR(500),
        nominee         VARCHAR(500),
        artist          VARCHAR(500),
        workers         TEXT,
        img             VARCHAR(1000),
        winner          VARCHAR(10),
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

TRUNCATE_SQL = f"TRUNCATE TABLE {GRAMMYS_TABLE};"


# ── Helpers ───────────────────────────────────────────────────────────────────
def _connect(database: str | None = None) -> mysql.connector.MySQLConnection:
    cfg = dict(DB_CONFIG)
    if database:
        cfg["database"] = database
    return mysql.connector.connect(**cfg)


def _table_has_data(cursor) -> bool:
    cursor.execute(f"SELECT COUNT(*) FROM {GRAMMYS_TABLE};")
    return cursor.fetchone()[0] > 0


# ── Main ──────────────────────────────────────────────────────────────────────
def run(force: bool = False) -> None:
    print("\n" + "=" * 70)
    print("SETUP — Loading Grammy Awards CSV → MySQL")
    print("=" * 70)

    # 1. Validate CSV exists
    if not GRAMMY_PATH.exists():
        raise FileNotFoundError(
            f"Grammy CSV not found: {GRAMMY_PATH}\n"
            f"Make sure 'the_grammy_awards.csv' is in data/raw/"
        )

    # 2. Read CSV (all as strings — no coercion at this stage)
    print(f"\nReading {GRAMMY_PATH}...")
    df = pd.read_csv(GRAMMY_PATH, dtype=str).fillna("")
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # 3. Create DB if needed
    print("\nConnecting to MySQL...")
    conn = _connect()
    cur  = conn.cursor()
    cur.execute(CREATE_DB_SQL)
    conn.commit()
    cur.close()
    conn.close()

    # 4. Create table + optionally reload
    conn = _connect(database=DB_NAME)
    cur  = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    if _table_has_data(cur) and not force:
        print(
            f"\n  ℹ️  Table '{GRAMMYS_TABLE}' already has data.\n"
            f"  Run with --force to truncate and reload.\n"
            f"  Skipping insert."
        )
        cur.close()
        conn.close()
        return

    if force:
        print(f"\n  --force flag detected: truncating '{GRAMMYS_TABLE}'...")
        cur.execute(TRUNCATE_SQL)
        conn.commit()

    # 5. Insert — only columns that exist in the table DDL
    insert_cols = [
        "year", "title", "published_at", "updated_at",
        "category", "nominee", "artist", "workers", "img", "winner",
    ]
    # Keep only columns present in both the DDL and the CSV
    cols_to_insert = [c for c in insert_cols if c in df.columns]
    missing = set(insert_cols) - set(cols_to_insert)
    if missing:
        print(f"  ⚠️  Columns not found in CSV (will be NULL): {missing}")

    insert_df = df[cols_to_insert].copy()

    cols_sql   = ", ".join(cols_to_insert)
    placeholders = ", ".join(["%s"] * len(cols_to_insert))
    sql = f"INSERT INTO {GRAMMYS_TABLE} ({cols_sql}) VALUES ({placeholders})"

    # Convert empty strings back to None for proper SQL NULL
    data = [
        tuple(None if v == "" else v for v in row)
        for row in insert_df.itertuples(index=False, name=None)
    ]

    print(f"\nInserting {len(data):,} rows into '{GRAMMYS_TABLE}'...")
    cur.executemany(sql, data)
    conn.commit()

    # 6. Verify
    cur.execute(f"SELECT COUNT(*) FROM {GRAMMYS_TABLE};")
    count = cur.fetchone()[0]
    print(f"  Rows in table after insert: {count:,} ✔")

    cur.close()
    conn.close()

    print("\nGrammy dataset loaded into MySQL successfully ✔")
    print(f"  Database : {DB_NAME}")
    print(f"  Table    : {GRAMMYS_TABLE}")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load the Grammy Awards CSV into MySQL (one-time setup)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Truncate and reload the table even if it already has data.",
    )
    args = parser.parse_args()
    run(force=args.force)