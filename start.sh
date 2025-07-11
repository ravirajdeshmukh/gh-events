#!/bin/bash

echo "[INFO] Starting gh-events pipeline..."

# Create directories if not present
mkdir -p data/bronze data/silver data/db

# Commenting this as I am facing difficulty trying to get lock on the dDB file.
# if [ "$LIVE" = "true" ]; then
#   echo "[INFO] Live mode enabled: streaming ingestion & processing"
#   poetry run python src/ingest.py &
#   poetry run watchmedo shell-command --patterns="*.json" --recursive --command='poetry run python src/transform.py' ./data/bronze &
#   poetry run watchmedo shell-command --patterns="*.parquet" --recursive --command='poetry run python src/materialize_duckdb.py' ./data/silver &
# else
  echo "[INFO] Running one-time materialization..."
  poetry run python src/ingest.py
  poetry run python src/transform.py
  poetry run python src/materialize_duckdb.py
# fi

# Start FastAPI and Streamlit (in parallel)
PYTHONPATH=src poetry run uvicorn api.main:app --host 0.0.0.0 --port 9000 &
poetry run streamlit run src/dashboard.py --server.port=8501 --server.address=0.0.0.0 


# Keep container alive
tail -f /dev/null