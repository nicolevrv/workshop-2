# setup.ps1 - Windows PowerShell (more modern with colors)

Write-Host "=== Workshop 2 Initial Setup ===" -ForegroundColor Cyan

# 1. Verify datasets
$missing = $false
$files = @{
    "data/raw/spotify_dataset.csv" = "Spotify"
    "data/raw/the_grammy_awards.csv" = "Grammy"
}

foreach ($file in $files.Keys) {
    if (-not (Test-Path $file)) {
        Write-Host "⚠️  Missing: $file" -ForegroundColor Yellow
        Write-Host "   Download the $($files[$file]) dataset and place it there"
        $missing = $true
    }
}

if ($missing) {
    Write-Host ""
    Write-Host "❌ Cannot load data because files are missing" -ForegroundColor Red
    exit 1
}

# 2. Activate virtual environment
if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Host "Activating virtual environment..." -ForegroundColor Gray
    & "venv\Scripts\Activate.ps1"
}

# 3. Install dependencies
Write-Host "Installing dependencies..." -ForegroundColor Gray
pip install -q mysql-connector-python pandas python-dotenv sqlalchemy

# ── 2. LOAD GRAMMYS INTO MYSQL ───────────────────────────────────────
Write-Host "[2/3] Loading Grammys data into MySQL..."
python src/load_grammys_db.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error loading Grammy data" -ForegroundColor Red
    exit 1
}

# ── 3. VERIFY ──────────────────────────────────────────────────────
Write-Host "[3/3] Setup completed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Database 'music_dw' created with tables:"
Write-Host "  - grammys_raw (source data)"
Write-Host "  - dim_artist, dim_album, dim_genre, dim_time, fact_track (star schema)"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  Test locally:  python src/main.py --skip-drive --export-csv"
Write-Host "  Start Airflow: cd airflow && docker-compose up -d"