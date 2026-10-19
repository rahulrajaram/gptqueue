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
 * reports occupied by returning its own name, so the generated guard returns
 * the FIRST blocking signal (table order) or false. Later signals may also
 * block; the name reports the first check that did, never the only one. The
 * generated fragment is interpolated into both scripts so the guard set can
 * never diverge again.
 */
export const OCCUPANCY_SIGNAL_NAMES = [
  "runtime_binding", "heartbeat", "wrapper_claim", "live_session_lease", "claims_index",
] as const;
export type OccupancySignalName = (typeof OCCUPANCY_SIGNAL_NAMES)[number];

export interface OccupancySignal {
  readonly name: OccupancySignalName;
  readonly lua: string;
}

/** `blocked(hit)` is a Lua statement that executes `return <hit>` when the signal blocks. */
const signal = (name: OccupancySignalName, blocked: (hit: string) => string): OccupancySignal =>
  Object.freeze({ name, lua: blocked(`'${name}'`) });

/** Lua: Redis's own clock in milliseconds, read inside the atomic script. */
export const REDIS_NOW_MS_LUA = "local time = redis.call('TIME')\n  local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)";

export const OCCUPANCY_SIGNALS: ReadonlyArray<OccupancySignal> = Object.freeze([
  // Lua cannot call TS: this literal must stay in sync with SESSION_KEYS.runtimeBinding (src/core/keys.ts).
  signal("runtime_binding", (hit) => `if redis.call('EXISTS','gptq:runtime-binding:'..agent) == 1 then return ${hit} end`),
  signal("heartbeat", (hit) => `if redis.call('EXISTS','gptq:heartbeat:'..agent) == 1 then return ${hit} end`),
  signal("wrapper_claim", (hit) => `if redis.call('EXISTS',wrapper) == 1 then return ${hit} end`),
  signal("live_session_lease", (hit) => "for _, sid in ipairs(redis.call('SMEMBERS','gptq:agent-sessions:'..agent)) do\n" +
    `    if redis.call('EXISTS','gptq:lease:'..sid) == 1 then return ${hit} end\n  end`),
  // Expired claims retain their payloads for normal lazy recovery. They
  // no longer represent an owner, so must not prevent native reconnect.
  // Read Redis's clock inside the atomic guard, including for operator apply.
  signal("claims_index", (hit) => `${REDIS_NOW_MS_LUA}\n  if redis.call('ZCOUNT','gptq:claims-index:'..agent,'('..now,'+inf') > 0 then return ${hit} end`),
]);

/** Generate the shared Lua `occupied(agent, wrapper)` guard: the first blocking signal's name, or false. */
export const occupancyGuardLua = (fnName: string): string =>
  `local function ${fnName}(agent, wrapper)\n  ${OCCUPANCY_SIGNALS.map((s) => s.lua).join("\n  ")}\n  return false\nend`;

/**
 * Generate Lua recheck-hint helpers (Redis ms, or false when unknown). Each is
 * a HINT, never a promise: other claims may remain, renewals move expiry and
 * further sends extend outbound activity.
 * - `${prefix}_claim(index)`: the earliest UNEXPIRED claim expiry in a claims index;
 * - `${prefix}_ttl(key)`: Redis now plus the key's remaining TTL.
 */
export const recheckHintLua = (prefix: string): string => `local function ${prefix}_claim(index)
  ${REDIS_NOW_MS_LUA}
  local first = redis.call('ZRANGEBYSCORE', index, '('..now, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  if first[2] then return tonumber(first[2]) end
  return false
end
local function ${prefix}_ttl(key)
  ${REDIS_NOW_MS_LUA}
  local ttl = redis.call('PTTL', key)
  if ttl > 0 then return now + ttl end
  return false
end`;
