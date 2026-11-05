.PHONY: help setup install test lint format clean docker-up docker-down docker-logs run-pipeline run-dashboard run-mcp

help:
	@echo "Self-Healing Lakehouse Pipeline System - Developer Commands"
	@echo "=========================================================="
	@echo "  setup         : Create virtualenv and install all dependencies"
	@echo "  test          : Run test suite with pytest"
	@echo "  lint          : Run Ruff lint checks"
	@echo "  format        : Format code with Black"
	@echo "  clean         : Remove python caches and temporary build artifacts"
	@echo "  docker-up     : Spin up complete Docker stack (Airflow, DB, Dashboard, MCP)"
	@echo "  docker-down   : Stop all Docker containers"
	@echo "  docker-logs   : Follow Docker logs"
	@echo "  run-pipeline  : Execute standalone Spark pipeline batch job"
	@echo "  run-dashboard : Start Streamlit Operations Dashboard"
	@echo "  run-mcp       : Start FastMCP Agent Server"

setup:
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v

lint:
	ruff check .

format:
	black .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.py[cod]" -delete
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov dist build *.egg-info

docker-up:
	docker compose up -d postgres airflow-init
	docker compose up -d airflow-webserver airflow-scheduler dashboard mcp-server

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

run-pipeline:
	python -m src.pipeline.data_pipeline

run-dashboard:
	streamlit run app/dashboard.py

run-mcp:
	python -m src.mcp_server.server

