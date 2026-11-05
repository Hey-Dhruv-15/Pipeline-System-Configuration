#!/usr/bin/env bash
set -euo pipefail

python -m compileall app src dags
docker compose config >/dev/null

echo "Smoke test passed: Python sources compile and docker compose is valid."
