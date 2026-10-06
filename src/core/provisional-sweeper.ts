/**
 * DL-2 / AC-1: retire crashed sidecars' provisional identities.
 *
 * A registered sidecar first holds a per-startup provisional name, which
 * peers can discover and send to, before it adopts or binds its mailbox. If
 * it dies before adoption or close(), that name would keep accepting work
 * nobody will ever claim. The sweep retires such a name in ONE atomic Lua
 * step once every session is expired and no claim remains:
 *   - the registry entry and provisional mark are removed, so send_message
 *     answers unknown_recipient from then on;
 *   - each queued envelope moves, byte for byte, to the name's DLQ with
 *     reason recipient_retired;
 *   - each moved task gets one typed error envelope (RECIPIENT_RETIRED,
 *     in_reply_to = the task id) from SYSTEM_SENDER to its sender. The
 *     sender's outstanding-request owner is re-pointed from the dead name to
 *     SYSTEM_SENDER first, so the existing reply checks wake the sender;
 *   - results, errors and pings only move to the DLQ (no reply loops).
 * Atomicity makes concurrent sweeps exactly-once: the loser finds the name
 * already retired. The error id is derived from the task id, so it is stable.
 * No daemon: callers run it opportunistically (provisional registration, the
 * doctor).
 */
import type { Redis } from "ioredis";
import { SESSION_KEYS, SYSTEM_SENDER } from "./keys.js";
import { TaskClaimStore } from "./task-claim-store.js";

export const RECIPIENT_RETIRED = "RECIPIENT_RETIRED";
export const RECIPIENT_RETIRED_TEXT =
  "Your task was not delivered: its recipient was a temporary startup identity of an agent sidecar " +
  "that exited before adopting its bound mailbox. Resend it to the agent's bound mailbox.";

export type SweepOutcome = "retired" | "live" | "claimed" | "unmarked";
export type SweepResult = Readonly<{ name: string; outcome: SweepOutcome; moved: number; errors_sent: number }>;

// Lua cannot call TS: the literal key prefixes below must stay in sync with
// SESSION_KEYS / DLQ_KEYS (src/core/keys.ts), as in core/occupancy-guard.ts.
const RETIRE_SCRIPT = `
local name, now, system, text, code = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local provisional, registry, audit = KEYS[1], KEYS[2], KEYS[3]
if redis.call('SISMEMBER', provisional, name) == 0 then return {'unmarked', 0, 0} end
for _, sid in ipairs(redis.call('SMEMBERS', 'gptq:agent-sessions:' .. name)) do
  if redis.call('EXISTS', 'gptq:lease:' .. sid) == 1 then return {'live', 0, 0} end
end
if redis.call('ZCARD', 'gptq:claims-index:' .. name) > 0 then return {'claimed', 0, 0} end
local queue, dlq, reasons = 'gptq:q:' .. name, 'gptq:dlq:' .. name, 'gptq:dlq-reason:' .. name
local pending = redis.call('LRANGE', queue, 0, -1)
local errors = 0
-- SEC-F4: during a rollout an older server can register the reserved system
-- name. Then a reply "from" it is forgeable, so the sweep neither hands it
-- outstanding-reply ownership nor speaks as it; the work still moves to the
-- DLQ, and a typed audit event records why no error was sent.
local reserved = redis.call('HEXISTS', registry, system) == 1 or redis.call('SCARD', 'gptq:agent-sessions:' .. system) > 0
if reserved and #pending > 0 then
  redis.call('XADD', audit, 'MAXLEN', '~', 1000, '*', 'event', 'reserved_sender_registered', 'name', name, 'reserved', system)
end
for _, raw in ipairs(pending) do
  -- ACCEPTED (final review 2026-10-06): this DLQ is deliberately not trimmed
  -- to the DLQ size bound. Accepted work is never dropped, and the dropped
  -- counter stays truthful because nothing is dropped here.
  redis.call('LPUSH', dlq, raw)
  local ok, msg = pcall(cjson.decode, raw)
  if ok and type(msg) == 'table' and type(msg.id) == 'string' then
    redis.call('HSET', reasons, msg.id, 'recipient_retired')
    if not reserved and msg.type == 'task' and type(msg.from) == 'string' and #msg.from > 0 and msg.from ~= system then
      local owner = 'gptq:outstanding:' .. msg.from .. ':' .. msg.id
      if redis.call('GET', owner) == name then redis.call('SET', owner, system, 'KEEPTTL') end
      local envelope = cjson.encode({
        id = 'retired-' .. msg.id, from = system, to = msg.from, timestamp = now, type = 'error',
        payload = { content = text, in_reply_to = msg.id, metadata = { code = code, recipient = name } },
      })
      local inbox = 'gptq:q:' .. msg.from
      local size = redis.call('RPUSH', inbox, envelope)
      redis.call('HSET', 'gptq:meta:' .. msg.from, 'current_size', size)
      if redis.call('GET', owner) == system then
        redis.call('XADD', 'gptq:inbox-events:' .. msg.from, 'MAXLEN', '~', 1024, '*', 'message_id', 'retired-' .. msg.id,
          'type', 'error', 'timestamp', now, 'from', system, 'to', msg.from, 'in_reply_to', msg.id)
      end
      errors = errors + 1
    end
  end
end
redis.call('DEL', queue)
redis.call('HDEL', registry, name)
redis.call('SREM', provisional, name)
-- COR-F2: a send that resolved this name before now is refused, not stranded.
redis.call('SET', 'gptq:forward:' .. name, 'retired', 'EX', 86400)
redis.call('XADD', audit, 'MAXLEN', '~', 1000, '*', 'event', 'recipient_retired', 'name', name,
  'moved', tostring(#pending), 'errors_sent', tostring(errors))
return {'retired', #pending, errors}
`;

/** Retire one provisional name if it is eligible; safe to call concurrently and repeatedly. */
export const retireProvisional = async (redis: Redis, name: string, now = new Date()): Promise<SweepResult> => {
  // Expired claims go back to the inbox (or the DLQ) first, so they are swept too.
  await new TaskClaimStore(redis).recoverExpired({ actor_id: name, now: now.toISOString() });
  const [outcome, moved, errors] = (await redis.eval(
    RETIRE_SCRIPT, 3, SESSION_KEYS.provisional, SESSION_KEYS.registry, SESSION_KEYS.identityAudit,
    name, now.toISOString(), SYSTEM_SENDER, RECIPIENT_RETIRED_TEXT, RECIPIENT_RETIRED,
  )) as [SweepOutcome, number, number];
  return Object.freeze({ name, outcome, moved: Number(moved), errors_sent: Number(errors) });
};

/**
 * SEC-F4 doctor check: names of every registry entry or session set that
 * claims the reserved SYSTEM_SENDER. Current code refuses that name, so any
 * hit was written by an older server and should be removed by an operator.
 */
export const reservedSenderRegistrations = async (redis: Redis): Promise<Readonly<{ registry: boolean; sessions: number }>> => {
  const [registry, sessions] = await Promise.all([
    redis.hexists(SESSION_KEYS.registry, SYSTEM_SENDER),
    redis.scard(SESSION_KEYS.agentSessions(SYSTEM_SENDER)),
  ]);
  return Object.freeze({ registry: registry === 1, sessions });
};

/** Sweep up to `limit` provisional names (a cheap, bounded, opportunistic pass). */
export const sweepProvisional = async (redis: Redis, limit = 32): Promise<readonly SweepResult[]> => {
  const names = (await redis.srandmember(SESSION_KEYS.provisional, limit)) as string[];
  const results: SweepResult[] = [];
  for (const name of names) results.push(await retireProvisional(redis, name));
  return Object.freeze(results);
};

