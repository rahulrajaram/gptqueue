#!/usr/bin/env bash
# Install (or upgrade) the local GPTQueue deployment:
# build from source and (re)start the shared HTTP MCP server.
#
# Usage: scripts/install.sh [port]        (default port: 8101)
# Env:   GPTQUEUE_REDIS_URL (default redis://127.0.0.1:6379)
#
# Redis-backed state (mailboxes, sessions, leases, claims) survives the
# restart; connected MCP clients reconnect automatically.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${1:-${GPTQUEUE_PORT:-8101}}"
LOG="${GPTQUEUE_LOG:-/tmp/gptqueue-http.log}"

echo "[install] building from source..."
npm run build

OLD_PID="$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
if [ -n "$OLD_PID" ]; then
  echo "[install] stopping previous server (pid $OLD_PID)..."
  kill "$OLD_PID"
  sleep 1
fi

echo "[install] starting gptqueue-http on port $PORT..."
REDIS_URL="${GPTQUEUE_REDIS_URL:-redis://127.0.0.1:6379}" \
  nohup node dist/transports/http.js --port "$PORT" >> "$LOG" 2>&1 &
NEW_PID=$!
disown || true

for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    echo "[install] healthy: http://127.0.0.1:$PORT/mcp (pid $NEW_PID, log $LOG)"
    exit 0
  fi
  sleep 0.5
done

echo "[install] ERROR: server did not become healthy; see $LOG" >&2
exit 1
