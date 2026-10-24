/**
 * Operator-run reaper for dead per-startup sidecar registrations.
 *
 * Older sidecars never removed their per-startup name
 * (`gptqueue-shell-<client>-<dir>-<uuid>`, registered-shell/server.ts), so the
 * registry accumulates offline names. This reaps ONLY names that are offline,
 * empty and provably old:
 *   - the name starts with `namePrefix` AND matches the sidecar shape
 *     (`nameShape`, default `<prefix><client>-<dir>-<uuid>`), is not
 *     SYSTEM_SENDER, not `claude-*` and not a durable actor;
 *   - its registry entry carries a parseable `registered_at`, and every session
 *     record of it carries a parseable `last_seen`/`created_at`; all are older
 *     than `minAgeMs` (missing or unparseable => skipped as `age_unknown`);
 *   - every BLOCKING key below is absent, and no session has a lease.
 *
 * Atomicity. All decisions that can race are taken in ONE Lua step covering
 * the whole batch. That step:
 *   1. SCANs the whole `gptq:*` keyspace with a CONSTANT pattern (no name ever
 *      reaches a MATCH glob) and attributes keys by exact prefix:
 *      - `gptq:session:<sid>` whose `agent_name` is a candidate but whose sid is
 *        not in the snapshot passed in => `session_outside_snapshot`;
 *      - `gptq:runtime-mailbox:<sha>` whose JSON `agent` is a candidate (or,
 *        when undecodable, whose raw value contains the name) =>
 *        `runtime_mapping_reference`;
 *      - `gptq:outstanding:`, `gptq:rc:`, `gptq:idempotency:` keys are
 *        `<prefix><name>:<rest>` where both parts may contain ':'; EVERY
 *        candidate equal to the text before ANY ':' is blocked, so a name that
 *        prefixes another name + ':' fails closed rather than being reaped.
 *   2. per candidate: registry value byte-identical to the one examined,
 *      session-set members all inside the snapshot, no lease on any snapshot
 *      session, every exact-name blocking key absent.
 * The writers of runtime-mailbox mappings (mailbox-continuity.ts applyContinuity)
 * and outstanding keys (lua/bounded-push.lua:19, bounded-push-idempotent.lua)
 * do NOT check a lease of the name atomically, so the reaper cannot rely on
 * its lease check for them; the in-step SCAN is the guard. Redis runs a
 * script without interleaving, so no key can appear between that SCAN and the
 * deletions. The step scans the whole gptq:* keyspace and gptq:claims on every
 * run (operator-run tool; accepted bound at ~2.3k keys, revisit if the keyspace
 * grows past ~100k). A scanned reference of unexpected type, or a session record
 * without agent_name, refuses the whole batch (unexpected_key_type). Cost: one keyspace SCAN per run, during which Redis is blocked.
 * CLI exit codes (bin/gptqueue-doctor reap-offline, dry run and apply): 0 batch
 * evaluated (per-name skips and remaining > 0 are normal); 4 batch refused with
 * nothing written (`refused` set); 5 batch evaluated, other names may have
 * been reaped, but `anomalies` > 0 names were skipped as unexpected_key_type
 * (unhealthy; inspect skipped[]); 3 client timeout, server outcome unknown;
 * precedence 3 > 4 > 5 > 0;
 * 1 usage error; 2 connection or other error.
 * Growth breaker: the step's FIRST action is DBSIZE; above `maxKeys` it neither
 * scans nor writes and reports every candidate as keyspace_too_large. This is
 * the only bound on server-side time: killing the client (doctor
 * --timeout-seconds) cannot interrupt a Lua script that is already running.
 * No rollback: the step decides every candidate (with TYPE checks) before its
 * first write; a wrong-typed batch-wide key (registry, pending, restore, actors,
 * provisional, audit) refuses the whole batch as unexpected_key_type with
 * nothing written, and a wrong-typed per-name forward/session-set key skips
 * only that name.
 *
 * Name-owned key classification (keys.ts):
 *   registry field (gptq:registry)          deleted
 *   gptq:agent-sessions:<n> + session/lease deleted (sessions in the snapshot)
 *   gptq:meta:<n>                           deleted (size counters only)
 *   provisional-set member                  deleted
 *   gptq:forward:<n>                        deleted if dangling, else kept
 *   gptq:agent:<n> (legacy, no writer)      BLOCKING
 *   gptq:registry:pending / :restore field  BLOCKING (registration in flight)
 *   actor-profiles field                    BLOCKING
 *   gptq:q:<n>                              BLOCKING
 *   gptq:heartbeat:<n>                      BLOCKING
 *   gptq:runtime-binding:<n>                BLOCKING
 *   gptq:idempotency:<n> and <n>:*          BLOCKING
 *   gptq:outstanding:<n>:*                  BLOCKING
 *   gptq:activation:<n>                     BLOCKING
 *   gptq:agent-profile:<n>                  BLOCKING
 *   gptq:outbound-activity:<n>              BLOCKING
 *   experimental-wrapper-claim:<sha(n)>     BLOCKING
 *   gptq:wake-lease:<n>                     BLOCKING
 *   gptq:claims-index:<n>                   BLOCKING
 *   gptq:dlq:<n>, dlq-dropped, dlq-reason   BLOCKING
 *   gptq:dlq-notice-pending:<n>             BLOCKING (unsent notice ids)
 *   gptq:dlq-notice-overflow:<n>            BLOCKING (unsent notice count)
 *   gptq:dlq-notice-window:<n>              BLOCKING (notice delivery window)
 *   gptq:dlq-notice-collisions:<n>          BLOCKING (reserved-sender diagnostic)
 *   gptq:rc:<n>:*                           BLOCKING
 *   gptq:claims record (actor_id/session_id) BLOCKING
 *   snapshot session whose record names another agent: refuses the name (never deleted)
 *   runtime-mailbox mapping naming <n>      BLOCKING
 *   gptq:inbox-events:<n>, inbox-trace:<n>  intentionally KEPT (diagnostic history)
 * Notice keys are checked by exact actor key inside the atomic step. Any
 * existing value (including zero counters or wrong types) blocks, as with
 * dlq-dropped/recover counters. Colon-suffixed actors own separate notice
 * keys; the reaper never deletes notice state to make an identity reapable.
 */
import type { Redis } from "ioredis";
import { ACTOR_KEYS, CLAIM_KEYS, SESSION_KEYS, SYSTEM_SENDER } from "./keys.js";

export const REAP_AUDIT_REASON = "reaped_offline_empty";
export const DEFAULT_MIN_AGE_MS = 24 * 60 * 60 * 1000;
export const DEFAULT_NAME_PREFIX = "gptqueue-shell-";
/** Max candidates sent to the atomic step per run (bounds the time Redis is blocked). */
export const DEFAULT_BATCH_LIMIT = 200;
/** Growth breaker: above this DBSIZE the step neither scans nor writes (set from the reaper benchmark). */
export const DEFAULT_MAX_KEYS = 25_000;

export type SkipReason =
  | "reserved_name" | "claude_name" | "durable_actor" | "live_lease" | "heartbeat" | "runtime_binding"
  | "wrapper_claim" | "queue_not_empty" | "claims_outstanding" | "dlq_not_empty" | "dlq_dropped" | "dlq_reason"
  | "dlq_notice_pending" | "dlq_notice_overflow" | "dlq_notice_window" | "dlq_notice_collisions"
  | "recover_count" | "activation" | "agent_profile" | "outbound_activity" | "wake_lease" | "idempotency"
  | "legacy_agent_hash" | "registration_in_flight" | "session_outside_snapshot" | "runtime_mapping_reference"
  | "outstanding_request" | "keyspace_too_large" | "unexpected_key_type" | "session_owner_mismatch" | "claim_record" | "too_young" | "age_unknown" | "limit_reached" | "changed_before_delete";

export type ReapOptions = Readonly<{
  dryRun: boolean; minAgeMs?: number; limit?: number; maxKeys?: number; namePrefix?: string; now?: Date;
  /** Full-name shape a candidate must also match; defaults to the sidecar shape under `namePrefix`. */
  nameShape?: RegExp;
}>;
export type ReapCandidate = Readonly<{ name: string; registered_at: string | null; sessions: number }>;
export type ReapSkip = Readonly<{ name: string; reason: SkipReason }>;
export type ReapReport = Readonly<{
  dry_run: boolean;
  name_prefix: string;
  min_age_ms: number;
  examined: number;
  /** Reaped names, or would-be-reaped names in a dry run. */
  candidates: readonly ReapCandidate[];
  skipped: readonly ReapSkip[];
  counts: Readonly<Record<string, number>>;
  retained: string;
  /** Eligible names beyond this run's batch (reported as limit_reached); re-run to continue. */
  remaining: number;
  /** Growth-breaker threshold; DBSIZE at the step (null when no step ran) and whether it tripped. */
  /** Batch-wide refusal with nothing written (keyspace_too_large | unexpected_key_type), else null. */
  refused: string | null;
  /** Names skipped as unexpected_key_type when the batch was NOT refused (others may have been reaped); unhealthy. */
  anomalies: number;
  max_keys: number;
  keyspace_keys: number | null;
  keyspace_too_large: boolean;
}>;

const RETAINED_NOTE = "inbox-trace and inbox-events streams are kept as diagnostic history";
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
/** registered-shell/server.ts shellIdentity: `<prefix><client>-<[A-Za-z0-9_-]{1,32}>-<uuid>`. */
const sidecarShape = (prefix: string): RegExp => new RegExp(`^${escapeRegExp(prefix)}[A-Za-z0-9_-]+-${UUID}$`, "u");

// Lua cannot call TS: literal prefixes must stay in sync with src/core/keys.ts.
// KEYS: 1 registry, 2 identity audit, 3 provisional set, 4 actor profiles, 5 registry pending, 6 registry restore, 7 claims.
// ARGV: 1 dry-run '1'|'0', 2 audit reason, 3 now, 4 limit (-1 = none),
//   5 JSON [{name, expected, sids, wrapper}] (wrapper key is sha256-named, so built in TS).
// Returns JSON [[name, outcome, sessions]] in input order.
const REAP_SCRIPT = `
local registry, audit, provisional, actors, pending, restore, claims = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7]
local dry, reason, now, limit = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
local cands = cjson.decode(ARGV[5])
-- Growth breaker, FIRST action: over maxKeys, neither scan nor write.
local dbsize = redis.call('DBSIZE')
if dbsize > tonumber(ARGV[6]) then
  local refused = {}
  for _, c in ipairs(cands) do refused[#refused + 1] = {c.name, 'keyspace_too_large', 0} end
  return cjson.encode({dbsize = dbsize, tripped = true, refused = 'keyspace_too_large', results = refused})
end
local byName, blocked, bySid = {}, {}, {}
for _, c in ipairs(cands) do
  c.snap = {}
  for _, sid in ipairs(c.sids) do c.snap[sid] = true; bySid[sid] = c.name end
  byName[c.name] = c
end
local function block(name, why) if byName[name] and not blocked[name] then blocked[name] = why end end
-- '<name>:<rest>' with ':' allowed in both: every prefix ending before a ':' is a possible owner.
local function blockOwners(rest, why)
  local i = 0
  while true do
    i = string.find(rest, ':', i + 1, true)
    if not i then return end
    block(string.sub(rest, 1, i - 1), why)
  end
end
local function typeOk(key, want) local t = redis.call('TYPE', key).ok; return t == 'none' or t == want end
-- Durable claim records (claims-batch-claim.lua) carry actor_id and session_id; any naming a
-- candidate or one of its sessions blocks it. Undecodable records match by plain text.
-- A reference we cannot read or attribute (wrong type, missing owner field) sets
-- 'unreadable', which refuses the WHOLE batch as unexpected_key_type, nothing written.
local unreadable = false
if not typeOk(claims, 'hash') then unreadable = true else
  local ccur = '0'
  repeat
    local page = redis.call('HSCAN', claims, ccur, 'COUNT', 1000)
    ccur = page[1]
    for i = 2, #page[2], 2 do
      local raw = page[2][i]
      local ok, rec = pcall(cjson.decode, raw)
      if ok and type(rec) == 'table' and type(rec.actor_id) == 'string' then
        do block(rec.actor_id, 'claim_record') end
        if type(rec.session_id) == 'string' and bySid[rec.session_id] then block(bySid[rec.session_id], 'claim_record') end
      else
        for name in pairs(byName) do if string.find(raw, name, 1, true) then block(name, 'claim_record') end end
        for sid, name in pairs(bySid) do if string.find(raw, sid, 1, true) then block(name, 'claim_record') end end
      end
    end
  until ccur == '0'
end
local owned = { {'gptq:outstanding:', 'outstanding_request'}, {'gptq:rc:', 'recover_count'}, {'gptq:idempotency:', 'idempotency'} }
local cursor = '0'
repeat
  local page = redis.call('SCAN', cursor, 'MATCH', 'gptq:*', 'COUNT', 1000)
  cursor = page[1]
  for _, key in ipairs(page[2]) do
    if string.sub(key, 1, 13) == 'gptq:session:' then
      local t = redis.call('TYPE', key).ok
      local owner = t == 'hash' and redis.call('HGET', key, 'agent_name') or nil
      -- 'none' = logically expired since SCAN returned it; anything else unattributable fails closed.
      if t ~= 'none' and type(owner) ~= 'string' then unreadable = true end
      if type(owner) == 'string' and byName[owner] and not byName[owner].snap[string.sub(key, 14)] then
        block(owner, 'session_outside_snapshot')
      end
    elseif string.sub(key, 1, 21) == 'gptq:runtime-mailbox:' then
      local t = redis.call('TYPE', key).ok
      if t ~= 'none' and t ~= 'string' then unreadable = true end
      local raw = t == 'string' and redis.call('GET', key) or nil
      if type(raw) == 'string' then
        local ok, mapping = pcall(cjson.decode, raw)
        if ok and type(mapping) == 'table' and type(mapping.agent) == 'string' then
          block(mapping.agent, 'runtime_mapping_reference')
        else
          for name in pairs(byName) do
            if string.find(raw, name, 1, true) then block(name, 'runtime_mapping_reference') end
          end
        end
      end
    else
      for _, p in ipairs(owned) do
        if string.sub(key, 1, #p[1]) == p[1] then blockOwners(string.sub(key, #p[1] + 1), p[2]) end
      end
    end
  end
until cursor == '0'
local exact = {
  {'gptq:agent:', 'legacy_agent_hash'}, {'gptq:q:', 'queue_not_empty'}, {'gptq:heartbeat:', 'heartbeat'},
  {'gptq:runtime-binding:', 'runtime_binding'}, {'gptq:idempotency:', 'idempotency'}, {'gptq:activation:', 'activation'},
  {'gptq:agent-profile:', 'agent_profile'}, {'gptq:outbound-activity:', 'outbound_activity'},
  {'gptq:wake-lease:', 'wake_lease'}, {'gptq:claims-index:', 'claims_outstanding'}, {'gptq:dlq:', 'dlq_not_empty'},
  {'gptq:dlq-dropped:', 'dlq_dropped'}, {'gptq:dlq-reason:', 'dlq_reason'},
  {'gptq:dlq-notice-pending:', 'dlq_notice_pending'}, {'gptq:dlq-notice-overflow:', 'dlq_notice_overflow'},
  {'gptq:dlq-notice-window:', 'dlq_notice_window'}, {'gptq:dlq-notice-collisions:', 'dlq_notice_collisions'},
}
-- Redis does not roll scripts back, so nothing below may error once mutation starts:
-- phase A validates key TYPEs and decides every candidate with no writes; phase B
-- only mutates keys phase A proved are of the type its commands accept.
local function refusal(c)
  local name = c.name
  if redis.call('HGET', registry, name) ~= c.expected then return 'changed_before_delete' end
  if blocked[name] then return blocked[name] end
  if redis.call('HEXISTS', actors, name) == 1 then return 'durable_actor' end
  if redis.call('HEXISTS', pending, name) == 1 or redis.call('HEXISTS', restore, name) == 1 then return 'registration_in_flight' end
  if not typeOk('gptq:agent-sessions:' .. name, 'set') or not typeOk('gptq:forward:' .. name, 'string') then return 'unexpected_key_type' end
  for _, sid in ipairs(redis.call('SMEMBERS', 'gptq:agent-sessions:' .. name)) do
    if not c.snap[sid] then return 'session_outside_snapshot' end
  end
  for _, sid in ipairs(c.sids) do
    local record = 'gptq:session:' .. sid
    if not typeOk(record, 'hash') then return 'unexpected_key_type' end
    local owner = redis.call('HGET', record, 'agent_name')
    if redis.call('EXISTS', record) == 1 and owner ~= name then return 'session_owner_mismatch' end
    if redis.call('EXISTS', 'gptq:lease:' .. sid) == 1 then return 'live_lease' end
  end
  if redis.call('EXISTS', c.wrapper) == 1 then return 'wrapper_claim' end
  for _, e in ipairs(exact) do
    if redis.call('EXISTS', e[1] .. name) == 1 then return e[2] end
  end
  return nil
end
local results, reaped = {}, 0
-- Batch-wide keys every mutation touches: a wrong type refuses the WHOLE batch, nothing written.
local globalOk = typeOk(registry, 'hash') and typeOk(pending, 'hash') and typeOk(restore, 'hash')
  and typeOk(actors, 'hash') and typeOk(provisional, 'set') and typeOk(audit, 'stream') and typeOk(claims, 'hash') and not unreadable
for _, c in ipairs(cands) do
  local why = (not globalOk) and 'unexpected_key_type' or refusal(c)
  if not why and limit >= 0 and reaped >= limit then why = 'limit_reached' end
  if why then
    results[#results + 1] = {c.name, why, 0}
  else
    reaped = reaped + 1
    results[#results + 1] = {c.name, 'reapable', #c.sids}
  end
end
if dry ~= '1' then
  for i, c in ipairs(cands) do
    if results[i][2] == 'reapable' then
      local name = c.name
      -- DEL never errors on type; HDEL/SREM/GET/HEXISTS/XADD keys were type-checked in phase A.
      for _, sid in ipairs(c.sids) do redis.call('DEL', 'gptq:session:' .. sid, 'gptq:lease:' .. sid) end
      redis.call('DEL', 'gptq:agent-sessions:' .. name, 'gptq:meta:' .. name)
      redis.call('HDEL', registry, name)
      redis.call('SREM', provisional, name)
      local fwd = redis.call('GET', 'gptq:forward:' .. name)
      if fwd then
        local target = string.match(fwd, '^to:(.+)$')
        if (not target) or redis.call('HEXISTS', registry, target) == 0 then redis.call('DEL', 'gptq:forward:' .. name) end
      end
      redis.call('XADD', audit, 'MAXLEN', '~', 1000, '*', 'event', 'registry_reaped', 'name', name,
        'reason', reason, 'sessions_removed', tostring(#c.sids), 'timestamp', now)
    end
  end
end
return cjson.encode({dbsize = dbsize, tripped = false, refused = (not globalOk) and 'unexpected_key_type' or false, results = results})
`;

/** session id -> agent_name for every session record (constant SCAN pattern; no name in the glob). */
const sessionOwners = async (redis: Redis): Promise<ReadonlyMap<string, string[]>> => {
  const owners = new Map<string, string[]>();
  const prefix = SESSION_KEYS.session("");
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 1000);
    cursor = next;
    const names = keys.length
      ? await Promise.all(keys.map(key => redis.hget(key, "agent_name").catch(() => null)))
      : [];
    keys.forEach((key, i) => {
      const owner = names[i];
      if (typeof owner === "string") owners.set(owner, [...(owners.get(owner) ?? []), key.slice(prefix.length)]);
    });
  } while (cursor !== "0");
  return owners;
};

const parseMs = (value: unknown): number | null => {
  const ms = typeof value === "string" ? Date.parse(value) : NaN;
  return Number.isFinite(ms) ? ms : null;
};

const registeredAt = (raw: string): string | null => {
  try { const value = (JSON.parse(raw) as { registered_at?: unknown }).registered_at; return typeof value === "string" ? value : null; }
  catch { return null; }
};

/**
 * Newest evidence of activity in ms, or null when age cannot be established:
 * registered_at must parse, and every existing session record must carry a
 * parseable last_seen or created_at.
 */
const youngestMs = async (redis: Redis, at: string | null, sessions: readonly string[]): Promise<number | null> => {
  const registered = parseMs(at);
  if (registered === null) return null;
  const stamps = await Promise.all(sessions.map(async sid => {
    const [exists, [created, seen]] = await Promise.all([
      redis.exists(SESSION_KEYS.session(sid)), redis.hmget(SESSION_KEYS.session(sid), "created_at", "last_seen"),
    ]);
    // A set member with no record has no age evidence of its own; the in-step lease check still covers it.
    if (exists === 0) return registered;
    const values = [parseMs(created), parseMs(seen)].filter((ms): ms is number => ms !== null);
    return values.length ? Math.max(...values) : null;
  }));
  return stamps.some(ms => ms === null) ? null : Math.max(registered, ...(stamps as number[]));
};

/** Static exclusions that need no Redis state beyond the name itself. */
const staticSkip = (name: string): SkipReason | null =>
  name === SYSTEM_SENDER ? "reserved_name" : name.startsWith("claude-") ? "claude_name" : null;

/** Reap (or, with dryRun, list) empty offline per-startup sidecar names. */
export const reapOfflineIdentities = async (redis: Redis, options: ReapOptions): Promise<ReapReport> => {
  const minAgeMs = options.minAgeMs ?? DEFAULT_MIN_AGE_MS;
  const prefix = options.namePrefix ?? DEFAULT_NAME_PREFIX;
  const shape = options.nameShape ?? sidecarShape(prefix);
  const limit = options.limit ?? DEFAULT_BATCH_LIMIT;
  const maxKeys = options.maxKeys ?? DEFAULT_MAX_KEYS;
  const now = options.now ?? new Date();
  const cutoff = now.getTime() - minAgeMs;
  const registry = await redis.hgetall(SESSION_KEYS.registry);
  const names = Object.keys(registry).filter(name => name.startsWith(prefix) && shape.test(name)).sort();
  const owners = await sessionOwners(redis);
  const skipped: ReapSkip[] = [];
  const skip = (name: string, reason: SkipReason) => { skipped.push(Object.freeze({ name, reason })); };
  const pending: { name: string; expected: string; sids: string[]; wrapper: string; at: string | null }[] = [];
  for (const name of names) {
    const raw = registry[name]!;
    const fixed = staticSkip(name);
    if (fixed) { skip(name, fixed); continue; }
    // Session completeness: set members UNION every record naming this agent.
    const sids = [...new Set([...(await redis.smembers(SESSION_KEYS.agentSessions(name))), ...(owners.get(name) ?? [])])].sort();
    const at = registeredAt(raw);
    const youngest = await youngestMs(redis, at, sids);
    if (youngest === null) { skip(name, "age_unknown"); continue; }
    if (!(youngest < cutoff)) { skip(name, "too_young"); continue; }
    pending.push({ name, expected: raw, sids, wrapper: SESSION_KEYS.wrapperClaim(name), at });
  }
  // --limit bounds the INPUT batch, so the atomic step's blocking time is bounded too.
  const batch = pending.slice(0, limit);
  for (const { name } of pending.slice(limit)) skip(name, "limit_reached");
  const candidates: ReapCandidate[] = [];
  let refused: string | null = null;
  let keyspace: { keys: number | null; too_large: boolean } = { keys: null, too_large: false };
  if (batch.length) {
    const raw = await redis.eval(
      REAP_SCRIPT, 7, SESSION_KEYS.registry, SESSION_KEYS.identityAudit, SESSION_KEYS.provisional, ACTOR_KEYS.profiles,
      SESSION_KEYS.registryPending, SESSION_KEYS.registryRestore, CLAIM_KEYS.claims,
      options.dryRun ? "1" : "0", REAP_AUDIT_REASON, now.toISOString(), "-1",
      JSON.stringify(batch.map(({ name, expected, sids, wrapper }) => ({ name, expected, sids, wrapper }))), String(maxKeys),
    ) as string;
    const at = new Map(pending.map(p => [p.name, p.at]));
    const step = JSON.parse(raw) as { dbsize: number; tripped: boolean; refused: string | false; results: [string, string, number][] };
    keyspace = { keys: step.dbsize, too_large: step.tripped };
    refused = step.refused || null;
    for (const [name, outcome, removed] of step.results) {
      if (outcome === "reapable") candidates.push(Object.freeze({ name, registered_at: at.get(name) ?? null, sessions: Number(removed) }));
      else skip(name, outcome as SkipReason);
    }
  }
  const counts: Record<string, number> = { [options.dryRun ? "would_reap" : "reaped"]: candidates.length };
  for (const { reason } of skipped) counts[reason] = (counts[reason] ?? 0) + 1;
  return Object.freeze({
    dry_run: options.dryRun, name_prefix: prefix, min_age_ms: minAgeMs, examined: names.length,
    candidates: Object.freeze(candidates), skipped: Object.freeze(skipped), counts: Object.freeze(counts), retained: RETAINED_NOTE,
    remaining: pending.length - batch.length, max_keys: maxKeys,
    refused, keyspace_keys: keyspace.keys,
    anomalies: refused ? 0 : skipped.filter(s => s.reason === "unexpected_key_type").length, keyspace_too_large: keyspace.too_large,
  });
};
