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

:: 4. Load Grammys into MySQL
echo Loading Grammys into MySQL...
python src\load_grammys_db.py --force

if %errorlevel% neq 0 (
    echo.
    echo ❌ Error loading data into MySQL
    echo Verify that MySQL is running and credentials are correct in .env
    pause
    exit /b 1
)

echo.
echo ✅ Setup completed successfully
echo.
echo Next steps:
echo   1. Test locally:    python src\main.py --skip-drive
echo   2. Start Airflow:   cd airflow ^&^& docker-compose up -d
pause