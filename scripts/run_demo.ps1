$ErrorActionPreference = "Stop"

Write-Host "Starting hackathon demo stack..."
New-Item -ItemType Directory -Force -Path "data/raw" | Out-Null
Copy-Item "samples/customer_data.csv" "data/raw/customer_data.csv" -Force

docker compose up -d postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler dashboard mcp-server
docker compose run --rm pipeline-runner

Write-Host "Dashboard: http://localhost:8501"
Write-Host "Airflow: http://localhost:8080"
Write-Host "Airflow login: admin / admin"
