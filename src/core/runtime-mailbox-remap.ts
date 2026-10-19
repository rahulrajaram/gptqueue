import type { Redis } from "ioredis";
import { z } from "zod";
import { CLAIM_KEYS, SESSION_KEYS } from "./keys.js";
import { parseOutstanding } from "./watchdog.js";
import { admitTaskLedgerEntry, taskLedgerKey } from "./task-ledger.js";

/**
 * Operator-only recovery for a refused runtime mailbox adoption: repoint ONE
 * runtime's mailbox mapping from FROM (the mapped, now dormant mailbox) to TO
 * (the live sidecar's current, usually provisional, name), so that sidecar's
 * next bind finds the mapping naming itself and starts its dispatcher.
 *
 * It moves NO work: FROM's queued messages, claims and outstanding reply
 * correlations stay at FROM. The plan discloses both mailboxes' retained work
 * so the operator sees what remains; correlations come from a bounded SCAN,
 * and an incomplete enumeration refuses apply unless the operator accepts it.
 * Apply is one atomic Lua step: mapping CAS, guard re-check, SET, clearing
 * TO's provisional mark, and an `operator_remap` continuity-audit event.
 */
export type RemapRequest = Readonly<{ client: "codex" | "pi"; runtimeId: string; from: string; to: string }>;

export type MailboxPreview = Readonly<{
  name: string;
  registered: boolean;
  pid: number | null;
  /** Checked by THIS process (signal 0), not inside Redis; null without a pid. */
  pid_alive: boolean | null;
  working_directory: string | null;
  provisional: boolean;
  queue_length: number;
  unexpired_claims: ReadonlyArray<Readonly<{ claim_id: string; expires_at: string }>>;
  expired_claims: number;
  runtime_binding: boolean;
  heartbeat: boolean;
  live_session_lease: boolean;
  outbound_activity: boolean;
}>;

/** One outstanding request (`gptq:outstanding:<sender>:<task>`, value: the recipient that owes the reply). */
export type CorrelationSample = Readonly<{
  sender: string; task_id: string; recipient: string; sent_at: string;
  /** From the task ledger, when the send recorded one. */
  subject: string | null;
  /** The recipient's reply was marked answered. */
  answered: boolean;
}>;
export type MailboxCorrelations = Readonly<{
  /** Requests this mailbox sent and still awaits replies to. */
  awaiting_reply: number;
  /** Requests sent to this mailbox that it still owes replies to. */
  owes_reply: number;
  sample: ReadonlyArray<CorrelationSample>;
}>;
export type CorrelationDisclosure = Readonly<{
  /** False when the SCAN stopped at its cap: counts are then lower bounds. */
  complete: boolean;
  /** Cap on keyspace entries the SCAN may traverse (approximate: SCAN COUNT is a hint). */
  scan_limit: number;
  /** Keyspace entries traversed, approximately. */
  scanned: number;
  outstanding_keys: number;
  /** Outstanding keys that could not be read as a correlation (no TTL, wrong type or malformed). */
  unparsed: number;
  from: MailboxCorrelations;
  to: MailboxCorrelations;
}>;
export const DEFAULT_CORRELATION_SCAN_LIMIT = 200_000;
const CORRELATION_SAMPLE = 5;

export type RemapGuardName =
  | "distinct_mailboxes" | "mapping_names_from" | "to_registered" | "to_pid_alive"
  | "to_live_registration" | "from_dormant" | "correlation_disclosure_complete";
export type RemapGuard = Readonly<{ guard: RemapGuardName; ok: boolean; detail: string }>;

export type RemapPlan = Readonly<{
  request: RemapRequest;
  mapping: Readonly<{ key: string; agent: string | null; working_directory: string | null }>;
  mailboxes: Readonly<{ from: MailboxPreview; to: MailboxPreview }>;
  correlations: CorrelationDisclosure;
  guards: ReadonlyArray<RemapGuard>;
  ok: boolean;
  notes: ReadonlyArray<string>;
  /** CAS inputs for apply; internal, not part of the printed preview. */
  cas: Readonly<{ expectedMapping: string | null; desiredMapping: string | null; toRegistration: string | null }>;
}>;

export type RemapRefusalReason =
  | RemapGuardName | "mapping_changed" | "to_registration_changed" | "to_not_live" | "from_not_dormant" | "wrong_key_type";
export type RemapOutcome =
  | Readonly<{ status: "applied"; audit_id: string }>
  | Readonly<{ status: "refused"; refusals: ReadonlyArray<Readonly<{ reason: RemapRefusalReason; detail: string }>> }>;

const mappingSchema = z.object({ agent: z.string().min(1), working_directory: z.string() }).strict();

/** Signal-0 liveness; EPERM means the process exists under another user. */
export const pidAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
};

const parseJson = (raw: string | null): unknown => {
  if (raw === null) return null;
  try { return JSON.parse(raw) as unknown; } catch { return null; }
};
const record = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
/** Only a positive integer is a pid: signal 0 to 0 or a negative value targets a process group. */
const positivePid = (value: unknown): number | null =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : null;

const preview = async (redis: Redis, name: string, isAlive: (pid: number) => boolean): Promise<Readonly<{ view: MailboxPreview; raw: string | null }>> => {
  const time = await redis.time();
  const now = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
  const [raw, provisional, queueLength, claims, totalClaims, binding, heartbeat, outbound, sessions] = await Promise.all([
    redis.hget(SESSION_KEYS.registry, name), redis.sismember(SESSION_KEYS.provisional, name),
    redis.llen(SESSION_KEYS.queue(name)), redis.zrangebyscore(CLAIM_KEYS.index(name), `(${now}`, "+inf", "WITHSCORES"),
    redis.zcard(CLAIM_KEYS.index(name)), redis.exists(SESSION_KEYS.runtimeBinding(name)), redis.exists(SESSION_KEYS.heartbeat(name)),
    redis.exists(SESSION_KEYS.outboundActivity(name)), redis.smembers(SESSION_KEYS.agentSessions(name)),
  ]);
  const leases = sessions.length ? await redis.exists(...sessions.map((sid) => SESSION_KEYS.lease(sid))) : 0;
  const reg = record(parseJson(raw));
  const pid = positivePid(reg?.pid);
  const metadata = record(reg?.metadata);
  const unexpired = Array.from({ length: claims.length / 2 }, (_, i) =>
    Object.freeze({ claim_id: claims[2 * i]!, expires_at: new Date(Number(claims[2 * i + 1])).toISOString() }));
  return Object.freeze({ raw, view: Object.freeze({
    name, registered: raw !== null, pid, pid_alive: pid === null ? null : isAlive(pid),
    working_directory: typeof metadata?.working_directory === "string" ? metadata.working_directory : null,
    provisional: provisional === 1, queue_length: queueLength, unexpired_claims: Object.freeze(unexpired),
    expired_claims: totalClaims - unexpired.length, runtime_binding: binding === 1, heartbeat: heartbeat === 1,
    live_session_lease: leases > 0, outbound_activity: outbound === 1,
  }) });
};

const OUTSTANDING_PREFIX = SESSION_KEYS.outstanding("", "").slice(0, -1);

/**
 * Read-only, bounded disclosure of the outstanding reply correlations that
 * touch either mailbox. No index exists by mailbox (the owner is the value of
 * `gptq:outstanding:<sender>:<task>`), so this SCANs that prefix, as the
 * watchdog does, but stops after `limit` traversed entries and reports
 * whether the enumeration completed.
 */
const discloseCorrelations = async (redis: Redis, from: string, to: string, limit: number): Promise<CorrelationDisclosure> => {
  const count = Math.max(1, Math.min(1_000, limit));
  const keys = new Set<string>();
  let cursor = "0";
  let scanned = 0;
  do {
    const [next, batch] = await redis.scan(cursor, "MATCH", `${OUTSTANDING_PREFIX}*`, "COUNT", count);
    for (const key of batch) keys.add(key);
    cursor = next;
    scanned += count;
  } while (cursor !== "0" && scanned < limit);
  const time = await redis.time();
  const nowMs = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
  const all = [...keys];
  const read = async (batch: readonly string[]) => {
    const pipeline = redis.pipeline();
    for (const key of batch) pipeline.get(key).pttl(key);
    const replies = (await pipeline.exec()) ?? [];
    return batch.map((key, i) => ({ key, failed: !!replies[2 * i]?.[0], value: replies[2 * i]?.[1], pttl: replies[2 * i + 1]?.[1] }));
  };
  const rows = (await Promise.all(Array.from({ length: Math.ceil(all.length / 1_000) }, (_, i) => read(all.slice(i * 1_000, (i + 1) * 1_000))))).flat();
  // A key that vanished since the SCAN (no value) is not a correlation; any other unreadable one is reported.
  const present = rows.filter((row) => row.failed || (row.value !== null && row.value !== undefined));
  const requests = present.flatMap((row) => {
    const request = typeof row.value === "string" && typeof row.pttl === "number" ? parseOutstanding(row.key, row.value, row.pttl, nowMs) : null;
    return request ? [request] : [];
  });
  const touching = requests.filter((r) => r.sender === from || r.sender === to || r.recipient === from || r.recipient === to)
    .sort((a, b) => a.sent_at.localeCompare(b.sent_at));
  const sampled = (name: string) => touching.filter((r) => r.sender === name || r.recipient === name).slice(0, CORRELATION_SAMPLE);
  const picked = [...new Set([...sampled(from), ...sampled(to)])];
  const [ledgers, answered] = picked.length
    ? await Promise.all([redis.mget(picked.map((r) => taskLedgerKey(r.task_id))), redis.mget(picked.map((r) => SESSION_KEYS.taskAnswered(r.task_id)))])
    : [[], []];
  const detail = new Map(picked.map((r, i) => [r, Object.freeze({ ...r,
    subject: admitTaskLedgerEntry(ledgers[i] ?? null)?.subject ?? null, answered: (answered[i] ?? null) !== null })]));
  const summary = (name: string): MailboxCorrelations => Object.freeze({
    awaiting_reply: touching.filter((r) => r.sender === name).length,
    owes_reply: touching.filter((r) => r.recipient === name).length,
    sample: Object.freeze(sampled(name).map((r) => detail.get(r)!)),
  });
  return Object.freeze({ complete: cursor === "0", scan_limit: limit, scanned, outstanding_keys: all.length,
    unparsed: present.length - requests.length, from: summary(from), to: summary(to) });
};

const guard = (name: RemapGuardName, ok: boolean, detail: string): RemapGuard => Object.freeze({ guard: name, ok, detail });

/** Pure: evaluate the guards over a read snapshot. */
export const remapGuards = (
  request: RemapRequest, mappingAgent: string | null, from: MailboxPreview, to: MailboxPreview,
  disclosure: Readonly<{ complete: boolean; scanned: number; accepted: boolean }>,
): ReadonlyArray<RemapGuard> => Object.freeze([
  guard("distinct_mailboxes", request.from !== request.to, request.from !== request.to ? "from and to differ" : "from and to are the same mailbox"),
  guard("mapping_names_from", mappingAgent === request.from,
    mappingAgent === request.from ? `mapping names ${request.from}` : `mapping names ${mappingAgent ?? "nothing (missing or malformed)"}, not ${request.from}`),
  guard("to_registered", to.registered, to.registered ? `${to.name} is registered` : `${to.name} is not registered`),
  guard("to_pid_alive", to.pid_alive === true,
    to.pid === null ? `${to.name} has no registration pid` : `${to.name} pid ${to.pid} is ${to.pid_alive ? "alive" : "not running"} (checked by this process)`),
  guard("to_live_registration", to.heartbeat || to.live_session_lease,
    to.heartbeat || to.live_session_lease ? `${to.name} has a ${to.heartbeat ? "heartbeat" : "live session lease"}` : `${to.name} has no heartbeat and no live session lease`),
  guard("from_dormant", !from.runtime_binding && !from.heartbeat,
    !from.runtime_binding && !from.heartbeat ? `${from.name} has no runtime binding and no heartbeat`
      : `${from.name} is live (${[from.runtime_binding && "runtime binding", from.heartbeat && "heartbeat"].filter(Boolean).join(", ")}); do not remap`),
  guard("correlation_disclosure_complete", disclosure.complete || disclosure.accepted,
    disclosure.complete ? "outstanding reply correlations fully enumerated"
      : disclosure.accepted ? `correlation SCAN stopped at its cap (~${disclosure.scanned} keyspace entries); incomplete disclosure accepted by the operator`
        : `correlation SCAN stopped at its cap (~${disclosure.scanned} keyspace entries) before finishing; raise --max-correlation-scan or pass --accept-incomplete-disclosure yes`),
]);

/** Read-only plan: both mailboxes' retained work, and the guard results. */
export const planRuntimeMailboxRemap = async (
  redis: Redis, request: RemapRequest, options: Readonly<{
    isPidAlive?: (pid: number) => boolean;
    /** Keyspace entries the correlation SCAN may traverse (approximate). */
    correlationScanLimit?: number;
    /** Allow apply although the correlation SCAN stopped at its cap. */
    acceptIncompleteDisclosure?: boolean;
  }> = {},
): Promise<RemapPlan> => {
  const isAlive = options.isPidAlive ?? pidAlive;
  const limit = options.correlationScanLimit ?? DEFAULT_CORRELATION_SCAN_LIMIT;
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("correlationScanLimit must be a positive integer");
  const key = SESSION_KEYS.runtimeMailbox(request.client, request.runtimeId);
  const [expected, from, to, correlations] = await Promise.all([redis.get(key), preview(redis, request.from, isAlive),
    preview(redis, request.to, isAlive), discloseCorrelations(redis, request.from, request.to, limit)]);
  const parsed = mappingSchema.safeParse(parseJson(expected));
  const mapping = parsed.success ? parsed.data : null;
  const guards = remapGuards(request, mapping?.agent ?? null, from.view, to.view,
    { complete: correlations.complete, scanned: correlations.scanned, accepted: options.acceptIncompleteDisclosure === true });
  const bound = correlations.complete ? "" : " at least";
  return Object.freeze({
    request: Object.freeze({ ...request }),
    mapping: Object.freeze({ key, agent: mapping?.agent ?? null, working_directory: mapping?.working_directory ?? null }),
    mailboxes: Object.freeze({ from: from.view, to: to.view }),
    correlations,
    guards, ok: guards.every((g) => g.ok),
    notes: Object.freeze([
      `Remapping moves no work: ${from.view.queue_length} queued message(s), ${from.view.unexpired_claims.length} unexpired claim(s), and${bound} ` +
        `${correlations.from.awaiting_reply} awaited and${bound} ${correlations.from.owes_reply} owed reply correlation(s) stay at ${request.from}.`,
      `${request.to}'s pid liveness is checked by this process before the atomic apply; inside Redis the apply accepts a heartbeat or a live session lease as proof of a live registration.`,
    ]),
    cas: Object.freeze({ expectedMapping: expected,
      desiredMapping: mapping ? JSON.stringify({ agent: request.to, working_directory: mapping.working_directory }) : null,
      toRegistration: to.raw }),
  });
};

/** The printable preview: the plan without its internal CAS inputs. */
export const remapPreview = (plan: RemapPlan): Omit<RemapPlan, "cas"> => {
  const { cas: _cas, ...printable } = plan;
  return printable;
};

/**
 * KEYS: 1 mapping, 2 registry, 3 runtime binding(from), 4 heartbeat(from),
 *       5 heartbeat(to), 6 agent sessions(to), 7 provisional set, 8 continuity audit
 * ARGV: 1 planned mapping, 2 desired mapping, 3 from, 4 to, 5 runtime id,
 *       6 planned TO registration, 7 client
 */
const APPLY_REMAP_SCRIPT = `
-- Validate the type of EVERY key this script reads or writes before the first
-- write: a Redis command error after a write would leave a partial remap.
local function wrong_type(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual == 'none' or actual == expected then return false end
  return actual
end
local checks = { {1, 'string', 'mapping'}, {2, 'hash', 'registry'}, {3, 'string', 'from_runtime_binding'},
  {4, 'string', 'from_heartbeat'}, {5, 'string', 'to_heartbeat'}, {6, 'set', 'to_sessions'},
  {7, 'set', 'provisional'}, {8, 'stream', 'continuity_audit'} }
for _, check in ipairs(checks) do
  local actual = wrong_type(KEYS[check[1]], check[2])
  if actual then return {-5, check[3], actual} end
end
for _, sid in ipairs(redis.call('SMEMBERS', KEYS[6])) do
  local actual = wrong_type('${SESSION_KEYS.lease("")}' .. sid, 'string')
  if actual then return {-5, 'to_session_lease', actual} end
end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1, 'mapping_changed'} end
if redis.call('HGET', KEYS[2], ARGV[4]) ~= ARGV[6] then return {-2, 'to_registration_changed'} end
local live = redis.call('EXISTS', KEYS[5]) == 1
if not live then
  for _, sid in ipairs(redis.call('SMEMBERS', KEYS[6])) do
    if redis.call('EXISTS', '${SESSION_KEYS.lease("")}' .. sid) == 1 then live = true break end
  end
end
if not live then return {-3, 'to_not_live'} end
if redis.call('EXISTS', KEYS[3]) == 1 then return {-4, 'runtime_binding'} end
if redis.call('EXISTS', KEYS[4]) == 1 then return {-4, 'heartbeat'} end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('SREM', KEYS[7], ARGV[4])
return redis.call('XADD', KEYS[8], 'MAXLEN', '~', 1000, '*', 'event', 'operator_remap', 'target', ARGV[4], 'source', ARGV[3], 'runtime_id', ARGV[5], 'client', ARGV[7])
`;

type RemapRefusal = Readonly<{ reason: RemapRefusalReason; detail: string }>;

/** The type the apply script expects of each key it validates (F4). */
const EXPECTED_TYPES: Readonly<Record<string, string>> = Object.freeze({
  mapping: "string", registry: "hash", from_runtime_binding: "string", from_heartbeat: "string", to_heartbeat: "string",
  to_sessions: "set", provisional: "set", continuity_audit: "stream", to_session_lease: "string",
});

/** The apply script's refusal replies; undefined for an unknown one. */
const applyRefusal = (code: number, signal: string, actual: unknown, plan: RemapPlan): RemapRefusal | undefined => {
  switch (code) {
    case -1: return { reason: "mapping_changed", detail: `mapping ${plan.mapping.key} changed since the plan was read` };
    case -2: return { reason: "to_registration_changed", detail: `${plan.request.to}'s registration changed since its pid was checked` };
    case -3: return { reason: "to_not_live", detail: `${plan.request.to} has no heartbeat and no live session lease` };
    case -4: return { reason: "from_not_dormant", detail: `${plan.request.from} became live (${signal.replaceAll("_", " ")}); do not remap` };
    case -5: return signal in EXPECTED_TYPES && typeof actual === "string"
      ? { reason: "wrong_key_type", detail: `the ${signal.replaceAll("_", " ")} key has type ${actual}, expected ${EXPECTED_TYPES[signal]}; nothing was written` }
      : undefined;
    default: return undefined;
  }
};

/**
 * Apply a plan whose guards all held, in ONE atomic step. A refusal writes
 * nothing. A Redis error or an unexpected reply throws, never a refusal, and
 * says the outcome is unknown: re-read the state before retrying.
 */
export const applyRuntimeMailboxRemap = async (redis: Redis, plan: RemapPlan): Promise<RemapOutcome> => {
  const failed = plan.guards.filter((g) => !g.ok);
  const { expectedMapping, desiredMapping, toRegistration } = plan.cas;
  if (failed.length || expectedMapping === null || desiredMapping === null || toRegistration === null) {
    return Object.freeze({ status: "refused", refusals: Object.freeze(failed.map((g) => Object.freeze({ reason: g.guard, detail: g.detail }))) });
  }
  const { from, to, runtimeId, client } = plan.request;
  const unknown = (what: string, cause?: unknown) =>
    new Error(`Remap outcome unknown (${what}); run the dry run again to inspect the mapping before retrying`, { cause });
  let reply: unknown;
  try {
    reply = await redis.eval(APPLY_REMAP_SCRIPT, 8, plan.mapping.key, SESSION_KEYS.registry,
      SESSION_KEYS.runtimeBinding(from), SESSION_KEYS.heartbeat(from), SESSION_KEYS.heartbeat(to), SESSION_KEYS.agentSessions(to),
      SESSION_KEYS.provisional, SESSION_KEYS.continuityAudit,
      expectedMapping, desiredMapping, from, to, runtimeId, toRegistration, client);
  } catch (error) {
    throw unknown(error instanceof Error ? error.message : String(error), error);
  }
  if (typeof reply === "string") return Object.freeze({ status: "applied", audit_id: reply });
  const [code, signal, actual] = Array.isArray(reply) ? reply : [];
  const refusal = typeof code === "number" && typeof signal === "string" ? applyRefusal(code, signal, actual, plan) : undefined;
  if (!refusal) throw unknown(`unexpected reply ${JSON.stringify(reply)}`);
  return Object.freeze({ status: "refused", refusals: Object.freeze([Object.freeze(refusal)]) });
};
