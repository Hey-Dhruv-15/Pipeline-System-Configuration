#!/usr/bin/env bash
set -euo pipefail

echo "Starting hackathon demo stack..."
mkdir -p data/raw
cp samples/customer_data.csv data/raw/customer_data.csv

docker compose up -d postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler dashboard mcp-server
docker compose run --rm pipeline-runner

echo "Dashboard: http://localhost:8501"
echo "Airflow: http://localhost:8080"
echo "Airflow login: admin / admin"
