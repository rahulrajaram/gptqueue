#!/usr/bin/env bash
# Install (or upgrade) the local GPTQueue deployment:
# build from source and (re)start the shared HTTP MCP server.
#
# Usage: scripts/install.sh [port]        (default port: 8101)
# Env:   GPTQUEUE_REDIS_URL (default redis://127.0.0.1:6379)
#        GPTQUEUE_LOG       (default: ${XDG_STATE_HOME:-~/.local/state}/gptqueue/http.log)
#
# Redis-backed state (mailboxes, sessions, leases, claims) survives the
# restart; connected MCP clients reconnect automatically.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${1:-${GPTQUEUE_PORT:-8101}}"

# Resolve the server log into a per-user state directory (0700) with a 0600
# file — never a predictable world-writable /tmp path. A pre-planted symlink
# at the log path is refused: the shell append redirect cannot express
# O_NOFOLLOW, so the path is verified (and the file owned by us) before the
# server's redirect opens it, and the 0700 parent keeps anyone else out.
if [ -n "${GPTQUEUE_LOG:-}" ]; then
  LOG="${GPTQUEUE_LOG}"
  mkdir -p -- "$(dirname -- "$LOG")"
else
  STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/gptqueue"
  mkdir -p -- "$STATE_DIR"
  chmod 700 -- "$STATE_DIR"
  LOG="$STATE_DIR/http.log"
fi
if [ -L "$LOG" ]; then
  echo "[install] ERROR: log path $LOG is a symlink; refusing to write to it" >&2
  exit 1
fi
if [ -e "$LOG" ]; then
  chmod 600 -- "$LOG" 2>/dev/null || true
else
  install -m 0600 /dev/null "$LOG"
fi

echo "[install] building from source..."
npm run build

OLD_PID="$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
if [ -n "$OLD_PID" ]; then
  echo "[install] stopping previous server (pid $OLD_PID)..."
  kill "$OLD_PID"
  sleep 1
fi

echo "[install] starting gptqueue-http on port $PORT..."
# GPTQUEUE_HOST and GPTQUEUE_HTTP_TOKEN are inherited from the caller's
# environment (nothing sanitizes them), but we forward them explicitly so the
# security controls honored by dist/transports/http.js survive this nohup
# wrapper untouched. Empty values forward as empty strings, which the server
# treats as "not configured" (loopback bind / no token).
REDIS_URL="${GPTQUEUE_REDIS_URL:-redis://127.0.0.1:6379}" \
GPTQUEUE_HOST="${GPTQUEUE_HOST:-}" \
GPTQUEUE_HTTP_TOKEN=\
"${GPTQUEUE_HTTP_TOKEN:-}" \
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
