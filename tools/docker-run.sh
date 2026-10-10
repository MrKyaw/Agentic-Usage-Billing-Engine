#!/usr/bin/env bash
# Canonical local run: container `agentic-usage-billing-engine` on the first
# free host port in 8001..8010 (never assume a port — always probe first).
# Usage: bash tools/docker-run.sh [--rebuild] [--tag 0.1.2]
set -euo pipefail
TAG="0.1.2"
REBUILD=0
for a in "$@"; do
  case "$a" in
    --rebuild) REBUILD=1 ;;
    --tag) shift ;;
    --tag=*) TAG="${a#--tag=}" ;;
  esac
done
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NAME="agentic-usage-billing-engine"
if [ "$REBUILD" = 1 ]; then
  docker build -t "$NAME:$TAG" "$ROOT" >/dev/null && echo "built $NAME:$TAG"
fi
PORT=""
for p in $(seq 8001 8010); do
  if ! python3 -c "import socket,sys; sys.exit(socket.socket().connect_ex(('127.0.0.1',$p)))" 2>/dev/null; then
    PORT="$p"; break
  fi
done
[ -n "$PORT" ] || { echo "no free port in 8001..8010" >&2; exit 1; }
docker rm -f "$NAME" billing-demo Agentic-Usage-Billing-Engine >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "$PORT:8000" "$NAME:$TAG" >/dev/null
for _ in $(seq 1 30); do
  if curl -sf -m 2 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
echo "up: http://127.0.0.1:$PORT/health  ui: http://127.0.0.1:$PORT/ui"
curl -s "http://127.0.0.1:$PORT/health"; echo
