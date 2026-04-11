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
- All DDL lives in a single SQL file: sql/create_schema.sql
"""

import os
from pathlib import Path

import mysql.connector
import pandas as pd

# ── Config────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "user":     os.environ.get("DB_USER", "etl_user"),
    "password": os.environ.get("DB_PASSWORD", ""),  # never hardcode
    "port":     int(os.environ.get("DB_PORT", 3306)),
}
DB_NAME = os.environ.get("DB_NAME", "music_dw")



# Insert order matters: dimensions before fact (FK constraints)
INSERT_ORDER = ["dim_artist", "dim_album", "dim_genre", "dim_time", "fact_track"]


# ── Connection helpers────────────────────────────────────────────────────────
def _connect(database: str | None = None) -> mysql.connector.MySQLConnection:
    """Open a MySQL connection, optionally targeting a specific database."""
    cfg = dict(DB_CONFIG)
    if database:
        cfg["database"] = database
    return mysql.connector.connect(**cfg)


# ── SQL file executor─────────────────────────────────────────────────────────
def _run_sql_file(cursor, path: Path) -> None:
    """Execute every non-empty statement in a SQL file."""
    sql = path.read_text(encoding="utf-8")
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


# ── Generic inserter──────────────────────────────────────────────────────────
def _insert_df(conn, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        print(f"  ⚠️  {table}: DataFrame is empty, skipping insert.")
        return

    # Convert to object first, then replace ALL null types
    # (np.nan, pd.NA, pd.NaT) to None so mysql-connector accepts them
    df = df.astype(object).where(pd.notnull(df.astype(object)), None)

    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT IGNORE INTO {table} ({cols}) VALUES ({placeholders})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(sql, data)
    conn.commit()
    print(f"  {table:<20} {cursor.rowcount:>7,} rows inserted")
    cursor.close()


# ── Main loader───────────────────────────────────────────────────────────────
def load_star_schema(tables: dict[str, pd.DataFrame]) -> None:
    """
    Create the music_dw database, build all tables, and insert data.

    Parameters
    ----------
    tables : dict returned by dimensional_model.run()
        Expected keys: dim_artist, dim_album, dim_genre, dim_time, fact_track
    """
    print("\n" + "=" * 70)
    print("TASK e — LOAD (Data Warehouse)")
    print("=" * 70)

    root = Path(__file__).parent.parent
    schema_sql = root / "sql" / "create_schema.sql"

    if not schema_sql.exists():
        raise FileNotFoundError(
            f"SQL file not found: {schema_sql}\n"
            f"Make sure sql/create_schema.sql exists in the project root."
        )

    # ── 1. Create database + tables
    print("\n[1/2] Creating database and tables...")
    conn = _connect()
    cur = conn.cursor()
    _run_sql_file(cur, schema_sql)
    conn.commit()
    cur.close()
    conn.close()

    # ── 2. Reconnect targeting the database, then insert
    print("[2/2] Inserting data...")
    conn = _connect(database=DB_NAME)
    for table_name in INSERT_ORDER:
        if table_name not in tables:
            print(f"  ⚠️  '{table_name}' not found in tables dict — skipping.")
            continue
        _insert_df(conn, tables[table_name], table_name)
    conn.close()

    print("\nData Warehouse load completed ✔")


def _get_credentials_path() -> Path:
    """
    Detects environment and returns correct path to Google credentials.
    
    Priority:
    1. GOOGLE_CREDENTIALS_PATH environment variable (if set and valid)
    2. Docker: /opt/etl/credentials/service_account.json
    3. Local: project_root/credentials/service_account.json
    """
    # Priority 1: Explicit environment variable
    if env_path := os.environ.get("GOOGLE_CREDENTIALS_PATH"):
        path = Path(env_path)
        if path.exists():
            print(f"✅ Using GOOGLE_CREDENTIALS_PATH: {path}")
            return path
        print(f"⚠️  GOOGLE_CREDENTIALS_PATH set but doesn't exist: {path}")
    
    # Priority 2: Detect Docker (/opt/etl exists)
    docker_path = Path("/opt/etl/credentials/service_account.json")
    if docker_path.exists():
        print(f"✅ Docker mode detected: {docker_path}")
        return docker_path
    
    # Priority 3: Local mode - path relative to project
    # This file is in src/load_dw.py, go up 2 levels = project_root
    project_root = Path(__file__).resolve().parent.parent
    local_path = project_root / "credentials" / "service_account.json"
    
    if local_path.exists():
        print(f"✅ Local mode detected: {local_path}")
        return local_path
    
    # Not found - raise informative error
    raise FileNotFoundError(
        f"\n🔴 service_account.json not found in any location:\n"
        f"   1. Env var: {os.environ.get('GOOGLE_CREDENTIALS_PATH', 'Not set')}\n"
        f"   2. Docker:  {docker_path} (exists: {docker_path.exists()})\n"
        f"   3. Local:   {local_path} (exists: {local_path.exists()})\n\n"
        f"Solutions:\n"
        f"   • Place service_account.json in {local_path.parent}/\n"
        f"   • Or set GOOGLE_CREDENTIALS_PATH=C:/full/path/to/file.json\n"
        f"   • Or run with --skip-drive to skip Google Drive"
    )

def save_to_google_drive(df, filename: str = "merged_dataset.csv"):
    """
    Uploads DataFrame to Google Drive as CSV.
    
    Automatically detects environment (Docker/Windows/Linux) to find
    service account credentials.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    import tempfile
    
    # ── Get dynamic path ─────────────────────────────────────────────
    try:
        creds_path = _get_credentials_path()
    except FileNotFoundError as e:
        # If no credentials, warn but don't fail the pipeline
        print(f"\n⚠️  Google Drive upload skipped: {e}")
        print("✅  Data Warehouse was loaded successfully.")
        print("✅  Use --export-csv to save locally if needed.")
        return None  # Pipeline doesn't fail
    
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        print("⚠️  GOOGLE_DRIVE_FOLDER_ID not set. Skipping Drive upload.")
        return None
    
    print(f"\n📤 Uploading to Google Drive...")
    print(f"   Credentials: {creds_path}")
    print(f"   Folder ID:   {folder_id[:15]}...")
    
    # ── Authentication ─────────────────────────────────────────────────
    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(creds_path),
            scopes=['https://www.googleapis.com/auth/drive']
        )
        service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None
    
    # ── Export to temporary CSV ────────────────────────────────────────
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            df.to_csv(tmp.name, index=False)
            tmp_path = tmp.name
            print(f"   Temporary CSV: {tmp_path}")
    except Exception as e:
        print(f"❌ Error creating temporary CSV: {e}")
        return None
    
    # ── Upload to Drive ───────────────────────────────────────────────
    try:
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(tmp_path, mimetype='text/csv', resumable=True)
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink',
            supportsAllDrives=True  # Important for shared folders
        ).execute()
        
        file_id = file.get('id')
        file_link = file.get('webViewLink')
        file_name = file.get('name')
        
        print(f"✅ Uploaded successfully:")
        print(f"   Name: {file_name}")
        print(f"   ID:   {file_id}")
        print(f"   Link: {file_link}")
        
        return file_link
        
    except Exception as e:
        print(f"❌ Error uploading to Drive: {e}")
        # If quota error, give specific hint
        if "storageQuotaExceeded" in str(e) or "do not have storage quota" in str(e):
            print("\n💡 Tip: Service accounts don't have Drive storage quota.")
            print("   Share the folder with the service account (Editor)")
            print("   or use --skip-drive to skip this step.")
        return None
        
    finally:
        # Clean up temporary file
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass

# ── Standalone runner (development only)──────────────────────────────────────
if __name__ == "__main__":
    from extract import run as extract_run
    from transform_spotify import run as transform_spotify_run
    from transform_grammys import run as transform_grammys_run
    from merge_data import run as merge_run
    from dimensional_model import run as dim_run

    spotify_raw, grammy_raw = extract_run()
    spotify_clean = transform_spotify_run(spotify_raw)
    grammy_clean = transform_grammys_run(grammy_raw)
    merged = merge_run(spotify_clean, grammy_clean)
    tables = dim_run(merged)
    load_star_schema(tables)
    save_to_google_drive(merged)