#!/usr/bin/env bash
# Claude Code hook script for GPTQueue reactivity
# Usage:
#   GPTQ_AGENT_NAME=alice claude   # pre-named
#   claude                          # agent will prompt user for name

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"

# Agent name may or may not be set
AGENT_NAME="${GPTQ_AGENT_NAME:-}"

get_queue_length() {
  local key="gptq:q:${1}"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" LLEN "$key" 2>/dev/null || echo "0"
}

case "${1:-}" in
  --startup)
    if [ -n "$AGENT_NAME" ]; then
      cat <<EOF
{
  "additionalContext": "You are agent '${AGENT_NAME}' in the GPTQueue inter-agent communication system. You have MCP tools: register_agent, send_message, receive_message, list_agents, get_queue_status, unregister_agent. Call register_agent with name '${AGENT_NAME}' and role 'both' to join. When you receive messages, process them and respond using send_message."
}
EOF
    else
      cat <<EOF
{
  "additionalContext": "You are connected to the GPTQueue inter-agent communication system. You have MCP tools: register_agent, send_message, receive_message, list_agents, get_queue_status, unregister_agent. You are NOT yet registered. Ask the user what name and description they'd like for this agent, then call register_agent with role 'both'. You MUST register before you can send or receive messages."
}
EOF
    fi
    ;;
  --stop)
    if [ -z "$AGENT_NAME" ]; then
      # Not registered via env -- can't check queue
      echo "{}"
      exit 0
    fi
    COUNT=$(get_queue_length "$AGENT_NAME")
    if [ "$COUNT" -gt 0 ] 2>/dev/null; then
      cat <<EOF
{
  "decision": "block",
  "additionalContext": "IMPORTANT: You have ${COUNT} pending message(s) in your GPTQueue inbox. Call the receive_message tool NOW to process them. Do not stop until all messages are handled."
}
EOF
    else
      echo "{}"
    fi
    ;;
  *)
    echo "Usage: $0 --startup|--stop" >&2
    exit 1
    ;;
esac
