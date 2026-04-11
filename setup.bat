@echo off
chcp 65001 >nul
echo === Workshop 2 Initial Setup ===

:: 1. Verify datasets
set missing=0

if not exist "data\raw\spotify_dataset.csv" (
    echo ⚠️  Missing: data\raw\spotify_dataset.csv
    echo    Download the Spotify dataset and place it there
    set missing=1
)

if not exist "data\raw\the_grammy_awards.csv" (
    echo ⚠️  Missing: data\raw\the_grammy_awards.csv
    echo    Download the Grammy dataset and place it there
    set missing=1
)

if %missing%==1 (
    echo.
    echo ❌ Cannot load data because files are missing
    pause
    exit /b 1
)

:: 2. Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
)

:: 3. Install dependencies
echo Installing dependencies...
pip install -q mysql-connector-python pandas python-dotenv sqlalchemy

::: ── 1. CREATE DATABASE AND TABLES FIRST ───────────────────────────────
echo [1/3] Creating database and schema...
python -c "
import sys
sys.path.insert(0, 'src')
from extract import init_database
init_database()
"
if %errorlevel% neq 0 (
    echo ❌ Failed to create database schema
    exit /b 1
)

:: ── 2. LOAD GRAMMYS INTO MYSQL ───────────────────────────────────────
echo [2/3] Loading Grammys data into MySQL...
python src/load_grammys_db.py
if %errorlevel% neq 0 (
    echo ❌ Error loading Grammy data
    exit /b 1
)

:: ── 3. VERIFY ─────────────────────────────────────────────────────────
echo [3/3] Setup completed successfully!
echo.
echo Database 'music_dw' created with tables:
echo   - grammys_raw (source data)
echo   - dim_artist, dim_album, dim_genre, dim_time, fact_track (star schema)
echo.
echo Next steps:
echo   Test locally:  python src/main.py --skip-drive --export-csv
echo   Start Airflow: cd airflow ^&^& docker-compose up -d