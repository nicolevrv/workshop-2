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

# 4. Load Grammys
Write-Host "Loading Grammys into MySQL..." -ForegroundColor Green
python src/load_grammys_db.py --force

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Error loading data into MySQL" -ForegroundColor Red
    Write-Host "Verify that MySQL is running and credentials are correct in .env"
    exit 1
}

Write-Host ""
Write-Host "✅ Setup completed successfully" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Test locally:    python src/main.py --skip-drive"
Write-Host "  2. Start Airflow:   cd airflow && docker-compose up -d"