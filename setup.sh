#!/bin/bash
# setup.sh - Linux, macOS, Git Bash, WSL

echo "=== Workshop 2 Initial Setup ==="

# Detect operating system
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    echo "Detected Windows (Git Bash)"
fi


# 1. Verify datasets
missing=0

if [ ! -f "data/raw/spotify_dataset.csv" ]; then
    echo "⚠️  Missing: data/raw/spotify_dataset.csv"
    echo "   Download the Spotify dataset and place it there"
    missing=1
fi

if [ ! -f "data/raw/the_grammy_awards.csv" ]; then
    echo "⚠️  Missing: data/raw/the_grammy_awards.csv"
    echo "   Download the Grammy dataset and place it there"
    missing=1
fi

if [ $missing -eq 1 ]; then
    echo ""
    echo "❌ Cannot load data because files are missing"
    exit 1
fi

# 2. Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# 3. Install dependencies if needed
pip install -q mysql-connector-python pandas python-dotenv sqlalchemy

# ── 1. CREATE DATABASE AND TABLES FIRST ───────────────────────────────
echo "[1/3] Creating database and schema..."
python -c "
import sys
sys.path.insert(0, 'src')
from extract import init_database
init_database()
"
if [ $? -ne 0 ]; then
    echo "❌ Failed to create database schema"
    exit 1
fi

# ── 2. LOAD GRAMMYS INTO MYSQL ───────────────────────────────────────
echo "[2/3] Loading Grammys data into MySQL..."
python src/load_grammys_db.py
if [ $? -ne 0 ]; then
    echo "❌ Error loading Grammy data"
    exit 1
fi

# ── 3. VERIFY ──────────────────────────────────────────────────────
echo "[3/3] Setup completed successfully!"
echo ""
echo "Database 'music_dw' created with tables:"
echo "  - grammys_raw (source data)"
echo "  - dim_artist, dim_album, dim_genre, dim_time, fact_track (star schema)"
echo ""
echo "Next steps:"
echo "  Test locally:  python src/main.py --skip-drive --export-csv"
echo "  Start Airflow: cd airflow && docker-compose up -d"