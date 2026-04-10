#!/bin/bash
# setup.sh - Ejecutar una vez antes del primer run

echo "=== Setup Inicial del Workshop 2 ==="

# 1. Crear estructura de carpetas si no existe
mkdir -p data/raw data/processed data/star_schema credentials

# 2. Verificar que los archivos de datos existen
if [ ! -f "data/raw/spotify_dataset.csv" ]; then
    echo "⚠️  Falta: data/raw/spotify_dataset.csv"
    echo "   Descarga el dataset de Spotify y colócalo ahí"
fi

if [ ! -f "data/raw/the_grammy_awards.csv" ]; then
    echo "⚠️  Falta: data/raw/the_grammy_awards.csv"
    echo "   Descarga el dataset de Grammys y colócalo ahí"
fi

# 3. Cargar datos iniciales a MySQL (fuera de Airflow)
echo "Cargando Grammys a MySQL..."
python src/load_grammys_db.py --force

echo "=== Setup completado ==="
echo "Ahora inicia Airflow: cd airflow && docker-compose up -d"