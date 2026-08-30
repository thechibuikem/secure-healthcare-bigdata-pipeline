#!/bin/bash
# Runs the full pipeline end to end for a given date.
# Usage: ./scripts/run_pipeline.sh [YYYY-MM-DD]
# Defaults to today if no date given.

set -e  # stop immediately on any failure, don't continue with a broken chain

DATE="${1:-$(date +%Y-%m-%d)}"

echo "=== Ensure you've exported your environmental variables ==="

echo "=== Running pipeline for $DATE ==="

echo "[1/4] Loading Synthea data into HDFS..."
python3 -m ingestion.load_to_hdfs --date "$DATE"

echo "[2/4] Cleaning and encrypting (Spark ETL)..."
python3 -m etl.validate_and_clean --date "$DATE"

echo "[3/4] Building summary reports..."
python3 -m analytics.aggregate --date "$DATE"

echo "[4/4] Pipeline complete for $DATE."
echo "Check results with: hdfs dfs -ls /curated  and  hdfs dfs -ls /marts"