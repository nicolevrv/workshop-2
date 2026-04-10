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

# 4. Load Grammys into MySQL
echo "Loading Grammys into MySQL..."
python src/load_grammys_db.py --force

if [ $? -ne 0 ]; then
    echo "❌ Error loading data into MySQL"
    echo "Verify that MySQL is running and credentials are correct in .env"
    exit 1
fi

echo ""
echo "✅ Setup completed successfully"
echo ""
echo "Next steps:"
echo "  1. Test locally:    python src/main.py --skip-drive"
echo "  2. Start Airflow:   cd airflow && docker-compose up -d"