#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "  Self-Healing Pipeline System - Developer Setup (Bash)"
echo "=========================================================="

# 1. Check Python
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 is not installed or not in PATH."
    exit 1
fi

echo "[1/4] Found Python: $(python3 --version)"

# 2. Virtual environment setup
if [ ! -d ".venv" ]; then
    echo "[2/4] Creating virtual environment (.venv)..."
    python3 -m venv .venv
else
    echo "[2/4] Virtual environment already exists."
fi

# 3. Environment file
if [ ! -f ".env" ]; then
    echo "[3/4] Copying .env.example to .env..."
    cp .env.example .env
else
    echo "[3/4] .env already exists."
fi

# 4. Instructions
echo "[4/4] Environment initialized successfully!"
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment:  source .venv/bin/activate"
echo "  2. Install dependencies:           pip install -r requirements.txt -r requirements-dev.txt"
echo "  3. Run test suite:                 pytest tests/ -v"
echo "  4. Start local Docker stack:       docker compose up -d"
echo ""
