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
 * result of the claims-index cardinality check. The generated fragment is
 * interpolated into both scripts so the guard set can never diverge again.
 */
export interface OccupancySignal {
  readonly name: string;
  readonly lua: string;
}

export const OCCUPANCY_SIGNALS: ReadonlyArray<OccupancySignal> = [
  {
    name: "runtime_binding",
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
    lua: "return redis.call('ZCARD','gptq:claims-index:'..agent) > 0",
  },
];

/** Generate the shared Lua `occupied(agent, wrapper)` guard function. */
export const occupancyGuardLua = (fnName: string): string =>
  `local function ${fnName}(agent, wrapper)\n  ${OCCUPANCY_SIGNALS.map((s) => s.lua).join("\n  ")}\nend`;
