/**
 * D5: the ONE source of truth for the occupancy guard set shared by the two
 * adoption state machines — `applyContinuity`'s `occupied()` (operator path,
 * core/mailbox-continuity.ts) and `adoptIdentity`'s target guard (runtime
 * path, mcp-server/redis-client.ts). The F5 drift this table prevents had
 * the runtime path missing the claims-index check entirely; hand-maintained
 * duplicates are how that happens.
 *
 * Each signal is a Lua statement operating on `agent` (an ARGV agent name)
 * and `wrapper` (the hashed wrapper-claim key passed as a KEY). A signal
 * reports occupied by returning true; the final entry returns the boolean
 * result of the unexpired claims-index check. The generated fragment is
 * interpolated into both scripts so the guard set can never diverge again.
 */
export interface OccupancySignal {
  readonly name: string;
  readonly lua: string;
}

export const OCCUPANCY_SIGNALS: ReadonlyArray<OccupancySignal> = [
  {
    name: "runtime_binding",
    // Lua cannot call TS: this literal must stay in sync with SESSION_KEYS.runtimeBinding (src/core/keys.ts).
    lua: "if redis.call('EXISTS','gptq:runtime-binding:'..agent) == 1 then return true end",
  },
  {
    name: "heartbeat",
    lua: "if redis.call('EXISTS','gptq:heartbeat:'..agent) == 1 then return true end",
  },
  {
    name: "wrapper_claim",
    lua: "if redis.call('EXISTS',wrapper) == 1 then return true end",
  },
  {
    name: "live_session_lease",
    lua: "for _, sid in ipairs(redis.call('SMEMBERS','gptq:agent-sessions:'..agent)) do\n    if redis.call('EXISTS','gptq:lease:'..sid) == 1 then return true end\n  end",
  },
  {
    name: "claims_index",
    // Expired claims retain their payloads for normal lazy recovery. They
    // no longer represent an owner, so must not prevent native reconnect.
    // Read Redis's clock inside the atomic guard, including for operator apply.
    lua: "local time = redis.call('TIME')\n  local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)\n  return redis.call('ZCOUNT','gptq:claims-index:'..agent,'('..now,'+inf') > 0",
  },
];

/** Generate the shared Lua `occupied(agent, wrapper)` guard function. */
export const occupancyGuardLua = (fnName: string): string =>
  `local function ${fnName}(agent, wrapper)\n  ${OCCUPANCY_SIGNALS.map((s) => s.lua).join("\n  ")}\nend`;
