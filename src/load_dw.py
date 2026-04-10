"""
load_dw.py - Task e: Load to Data Warehouse
Responsibility: create the star schema in MySQL and insert all
dimension and fact tables.

Design decisions
----------------
- Database credentials are read from environment variables, never
  hardcoded. A .env file (excluded via .gitignore) can hold them locally.
- INSERT IGNORE is used so the pipeline is idempotent: running it twice
  does not raise duplicate-key errors.
- Tables are inserted in FK-safe order:
    dims first (artist → album → genre → time), fact last.
- All DDL lives in a single SQL file:
"""

import os
import pandas as pd
import mysql.connector
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "user":     os.environ.get("DB_USER",     "etl_user"),
    "password": os.environ.get("DB_PASSWORD", ""),        # never hardcode
    "port":     int(os.environ.get("DB_PORT", 3306)),
}
DB_NAME = os.environ.get("DB_NAME", "music_dw")

# Insert order matters: dimensions before fact (FK constraints)
INSERT_ORDER = ["dim_artist", "dim_album", "dim_genre", "dim_time", "fact_track"]


# ── Connection helpers ────────────────────────────────────────────────────────
def _connect(database: str | None = None) -> mysql.connector.MySQLConnection:
    """Open a MySQL connection, optionally targeting a specific database."""
    cfg = dict(DB_CONFIG)
    if database:
        cfg["database"] = database
    return mysql.connector.connect(**cfg)


# ── SQL file executor ─────────────────────────────────────────────────────────
def _run_sql_file(cursor, path: Path) -> None:
    """Execute every non-empty statement in a SQL file."""
    sql = path.read_text(encoding="utf-8")
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


# ── Generic inserter ──────────────────────────────────────────────────────────
def _insert_df(conn, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        print(f"  ⚠️  {table}: DataFrame is empty, skipping insert.")
        return

    # Convertir a object primero, luego reemplazar TODOS los tipos de nulo
    # (np.nan, pd.NA, pd.NaT) a None para que mysql-connector los acepte
    df = df.astype(object).where(pd.notnull(df.astype(object)), None)

    cursor = conn.cursor()
    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT IGNORE INTO {table} ({cols}) VALUES ({placeholders})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    cursor.executemany(sql, data)
    conn.commit()
    print(f"  {table:<20} {cursor.rowcount:>7,} rows inserted")
    cursor.close()

# ── Main loader ───────────────────────────────────────────────────────────────
def load_star_schema(tables: dict[str, pd.DataFrame]) -> None:
    """
    Create the music_dw database, build all tables, and insert data.

    Parameters
    ----------
    tables : dict returned by dimensional_model.run()
             Expected keys: dim_artist, dim_album, dim_genre,
                            dim_time, fact_track
    """
    print("\n" + "=" * 70)
    print("TASK e — LOAD (Data Warehouse)")
    print("=" * 70)

    root       = Path(__file__).parent.parent
    schema_sql = root / "sql" / "create_schema.sql"

    if not schema_sql.exists():
        raise FileNotFoundError(
            f"SQL file not found: {schema_sql}\n"
            f"Make sure sql/create_schema.sql exists in the project root."
        )

    # ── 1. Create database + tables (single file, single connection) ──────────
    print("\n[1/2] Creating database and tables...")
    conn = _connect()
    cur  = conn.cursor()
    _run_sql_file(cur, schema_sql)
    conn.commit()
    cur.close()
    conn.close()

    # ── 2. Reconnect targeting the database, then insert ──────────────────────
    print("[2/2] Inserting data...")
    conn = _connect(database=DB_NAME)
    for table_name in INSERT_ORDER:
        if table_name not in tables:
            print(f"  ⚠️  '{table_name}' not found in tables dict — skipping.")
            continue
        _insert_df(conn, tables[table_name], table_name)

    conn.close()
    print("\nData Warehouse load completed ✔")


# ── Google Drive export ───────────────────────────────────────────────────────
def save_to_google_drive(merged_df: pd.DataFrame, filename: str = "merged_dataset.csv") -> None:
    """
    Upload the merged dataset to Google Drive as a CSV file.

    Strategy
    --------
    Uses the Google Drive API v3 via the google-api-python-client library.
    Credentials are read from the path in the GOOGLE_CREDENTIALS_PATH
    environment variable (a service account JSON key).

    The file is uploaded to the folder specified by GOOGLE_DRIVE_FOLDER_ID
    (env var). If not set, it uploads to the root of the Drive.

    Setup (one-time)
    ----------------
    1. Create a GCP service account with Drive API enabled.
    2. Share the target Drive folder with the service account email.
    3. Download the JSON key and set GOOGLE_CREDENTIALS_PATH to its path.
    4. pip install google-api-python-client google-auth

    Parameters
    ----------
    merged_df : the final merged DataFrame to export
    filename  : name of the CSV file on Google Drive
    """
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaInMemoryUpload
        from google.oauth2 import service_account
    except ImportError:
        raise ImportError(
            "Google API libraries not installed.\n"
            "Run: pip install google-api-python-client google-auth"
        )

    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH")
    if not creds_path or not Path(creds_path).exists():
        raise FileNotFoundError(
            "Google credentials not found.\n"
            "Set the GOOGLE_CREDENTIALS_PATH environment variable to the "
            "path of your service account JSON key."
        )

    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")  # optional

    print(f"\nUploading '{filename}' to Google Drive...")

    # Auth
    scopes = ["https://www.googleapis.com/auth/drive.file"]
    creds  = service_account.Credentials.from_service_account_file(
        creds_path, scopes=scopes
    )
    service = build("drive", "v3", credentials=creds)

    # Serialize to CSV in memory
    csv_bytes = merged_df.to_csv(index=False).encode("utf-8")
    media     = MediaInMemoryUpload(csv_bytes, mimetype="text/csv", resumable=False)

    # File metadata
    file_metadata = {"name": filename}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    # Check if file already exists — update instead of duplicating
    query = f"name='{filename}' and trashed=false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    existing = (
        service.files()
        .list(q=query, fields="files(id, name)")
        .execute()
        .get("files", [])
    )

    if existing:
        file_id = existing[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"  Updated existing file (id={file_id}) ✔")
    else:
        result = service.files().create(
            body=file_metadata, media_body=media, fields="id"
        ).execute()
        print(f"  Created new file (id={result['id']}) ✔")


if __name__ == "__main__":
    from extract import run as extract_run
    from transform_spotify import run as transform_run
    from merge_data import run as merge_run
    from dimensional_model import run as dim_run

    spotify_raw, grammy_raw     = extract_run()
    spotify_clean, grammy_clean = transform_run(spotify_raw, grammy_raw)
    merged                      = merge_run(spotify_clean, grammy_clean)
    tables                      = dim_run(merged)

    load_star_schema(tables)
    save_to_google_drive(merged)