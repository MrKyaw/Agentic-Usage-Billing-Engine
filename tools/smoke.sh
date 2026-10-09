#!/usr/bin/env bash
# CI-equivalent smoke: tests + duplicate proof + reconcile + curl the live API.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/src"
pytest -q
bash tools/demo.sh
echo "SMOKE OK"
