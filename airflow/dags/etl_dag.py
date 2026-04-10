"""
etl_dag.py — Airflow DAG for the Spotify × Grammys ETL
Task graph:
    read_csv ──► transform_csv ──┐
                                  ├──► merge ──► load ──► store
    read_db ───► transform_db ────┘

XCom strategy:
    DataFrames are serialized to temp CSV files under /tmp/etl/<run_id>/.
    Only the file path (short string) is pushed via XCom.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ───────────────────────────────────────────────────────────────────────────────
# CRÍTICO: Agregar path del proyecto para imports
# ───────────────────────────────────────────────────────────────────────────────
SRC_PATH = Path("/opt/etl/src")
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Imports de módulos ETL
from extract import extract_spotify, extract_grammys
from transform_spotify import run as transform_spotify_run
from transform_grammys import run as transform_grammys_run
from merge_data import merge_spotify_grammys
from dimensional_model import run as dim_run
from load_dw import load_star_schema, save_to_google_drive

# ───────────────────────────────────────────────────────────────────────────────
# Configuración de rutas temporales para XCom
# ───────────────────────────────────────────────────────────────────────────────
TMP_DIR = Path("/tmp/etl")

def _tmp_path(run_id: str, name: str) -> Path:
    """Return a stable temp file path scoped to the current DAG run."""
    safe_run_id = run_id.replace(":", "_").replace("+", "_").replace(" ", "_")
    d = TMP_DIR / safe_run_id
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{name}.csv"

# ───────────────────────────────────────────────────────────────────────────────
# Task callables
# ───────────────────────────────────────────────────────────────────────────────

def task_read_csv(**context):
    """
    Task a — Extract Spotify CSV and serialize to temp file.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    
    spotify_path = "/opt/etl/data/raw/spotify_dataset.csv"
    
    print(f"[read_csv] Extrayendo desde: {spotify_path}")
    df = extract_spotify(spotify_path)
    
    path = _tmp_path(run_id, "spotify_raw")
    df.to_csv(path, index=False)
    
    ti.xcom_push(key="spotify_raw_path", value=str(path))
    print(f"[read_csv] Guardado en: {path} ({len(df):,} rows)")


def task_read_db(**context):
    """
    Task b — Extract Grammys from MySQL and serialize to temp file.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    
    print("[read_db] Extrayendo desde MySQL...")
    df = extract_grammys()
    
    path = _tmp_path(run_id, "grammy_raw")
    df.to_csv(path, index=False)
    
    ti.xcom_push(key="grammy_raw_path", value=str(path))
    print(f"[read_db] Guardado en: {path} ({len(df):,} rows)")


def task_transform_csv(**context):
    """
    Task c — Transform Spotify CSV data.
    """
    import pandas as pd
    ti = context["ti"]
    run_id = context["run_id"]
    
    spotify_path = ti.xcom_pull(task_ids="read_csv", key="spotify_raw_path")
    
    print(f"[transform_csv] Leyendo desde: {spotify_path}")
    spotify_raw = pd.read_csv(spotify_path)
    
    print("[transform_csv] Limpiando Spotify...")
    spotify_clean = transform_spotify_run(spotify_raw)
    
    path = _tmp_path(run_id, "spotify_clean")
    spotify_clean.to_csv(path, index=False)
    
    ti.xcom_push(key="spotify_clean_path", value=str(path))
    print(f"[transform_csv] Guardado en: {path} ({len(spotify_clean):,} rows)")


def task_transform_db(**context):
    """
    Task d — Transform Grammys DB data.
    """
    import pandas as pd
    ti = context["ti"]
    run_id = context["run_id"]
    
    grammy_path = ti.xcom_pull(task_ids="read_db", key="grammy_raw_path")
    
    print(f"[transform_db] Leyendo desde: {grammy_path}")
    grammy_raw = pd.read_csv(grammy_path)
    
    print("[transform_db] Limpiando Grammys...")
    grammy_clean = transform_grammys_run(grammy_raw)
    
    path = _tmp_path(run_id, "grammy_clean")
    grammy_clean.to_csv(path, index=False)
    
    ti.xcom_push(key="grammy_clean_path", value=str(path))
    print(f"[transform_db] Guardado en: {path} ({len(grammy_clean):,} rows)")


def task_merge(**context):
    """
    Task e — Merge Spotify + Grammys on artist_norm.
    """
    import pandas as pd
    ti = context["ti"]
    run_id = context["run_id"]
    
    sp_path = ti.xcom_pull(task_ids="transform_csv", key="spotify_clean_path")
    gm_path = ti.xcom_pull(task_ids="transform_db", key="grammy_clean_path")
    
    print(f"[merge] Cargando Spotify desde: {sp_path}")
    spotify_clean = pd.read_csv(sp_path)
    
    print(f"[merge] Cargando Grammys desde: {gm_path}")
    grammy_clean = pd.read_csv(gm_path)
    
    print("[merge] Ejecutando merge...")
    merged = merge_spotify_grammys(spotify_clean, grammy_clean)
    
    path = _tmp_path(run_id, "merged")
    merged.to_csv(path, index=False)
    
    ti.xcom_push(key="merged_path", value=str(path))
    print(f"[merge] Guardado en: {path} ({len(merged):,} rows)")


def task_load(**context):
    """
    Task f — Build star schema and load into MySQL DW.
    """
    import pandas as pd
    ti = context["ti"]
    run_id = context["run_id"]
    
    merged_path = ti.xcom_pull(task_ids="merge", key="merged_path")
    
    print(f"[load] Cargando merged data desde: {merged_path}")
    merged = pd.read_csv(merged_path)
    
    print("[load] Construyendo star schema...")
    tables = dim_run(merged)
    
    print("[load] Cargando a MySQL...")
    load_star_schema(tables)
    
    for name, df in tables.items():
        print(f"[load] {name}: {len(df):,} rows")
    
    ti.xcom_push(key="tables_loaded", value="true")


def task_store(**context):
    """
    Task g — Upload merged CSV to Google Drive (optional).
    """
    import pandas as pd
    ti = context["ti"]
    run_id = context["run_id"]
    
    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", "")
    if not creds_path or not Path(creds_path).exists():
        print("[store] ⚠️  GOOGLE_CREDENTIALS_PATH no configurado. Skipping.")
        ti.xcom_push(key="store_skipped", value="true")
        return
    
    merged_path = ti.xcom_pull(task_ids="merge", key="merged_path")
    
    print(f"[store] Cargando merged data desde: {merged_path}")
    merged = pd.read_csv(merged_path)
    
    print("[store] Subiendo a Google Drive...")
    save_to_google_drive(merged, filename=f"merged_dataset_{run_id}.csv")
    
    ti.xcom_push(key="store_completed", value="true")
    print("[store] ✅ Upload completado")


# ───────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ───────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="spotify_grammys_etl",
    description="ETL: Spotify CSV + Grammys DB -> Star Schema DW + Google Drive",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "spotify", "grammys", "music"],
    max_active_runs=1,
) as dag:

    # Tasks
    read_csv = PythonOperator(
        task_id="read_csv",
        python_callable=task_read_csv,
    )
    
    read_db = PythonOperator(
        task_id="read_db",
        python_callable=task_read_db,
    )
    
    transform_csv = PythonOperator(
        task_id="transform_csv",
        python_callable=task_transform_csv,
    )
    
    transform_db = PythonOperator(
        task_id="transform_db",
        python_callable=task_transform_db,
    )
    
    merge = PythonOperator(
        task_id="merge",
        python_callable=task_merge,
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )
    
    store = PythonOperator(
        task_id="store",
        python_callable=task_store,
    )

    # Dependencies: cada transform espera su lectura, luego convergen
    read_csv >> transform_csv >> merge
    read_db >> transform_db >> merge
    merge >> load >> store