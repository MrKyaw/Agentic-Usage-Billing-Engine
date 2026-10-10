#!/usr/bin/env bash
# CI-equivalent smoke: tests + duplicate proof + reconcile + curl the live API.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/src"
pytest -q --cov --cov-report=term-missing --cov-fail-under=100
bash tools/demo.sh
bash tools/chaos.sh
echo "SMOKE OK"
