import type { Redis } from "ioredis";
import { z } from "zod";
import { CLAIM_KEYS, SESSION_KEYS } from "./keys.js";

/**
 * Operator-only recovery for a refused runtime mailbox adoption: repoint ONE
 * runtime's mailbox mapping from FROM (the mapped, now dormant mailbox) to TO
 * (the live sidecar's current, usually provisional, name), so that sidecar's
 * next bind finds the mapping naming itself and starts its dispatcher.
 *
 * It moves NO work: FROM's queued messages and claims stay at FROM. The plan
 * discloses both mailboxes' retained work so the operator sees what remains.
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

export type RemapGuardName =
  | "distinct_mailboxes" | "mapping_names_from" | "to_registered" | "to_pid_alive"
  | "to_live_registration" | "from_dormant";
export type RemapGuard = Readonly<{ guard: RemapGuardName; ok: boolean; detail: string }>;

export type RemapPlan = Readonly<{
  request: RemapRequest;
  mapping: Readonly<{ key: string; agent: string | null; working_directory: string | null }>;
  mailboxes: Readonly<{ from: MailboxPreview; to: MailboxPreview }>;
  guards: ReadonlyArray<RemapGuard>;
  ok: boolean;
  notes: ReadonlyArray<string>;
  /** CAS inputs for apply; internal, not part of the printed preview. */
  cas: Readonly<{ expectedMapping: string | null; desiredMapping: string | null; toRegistration: string | null }>;
}>;

export type RemapRefusalReason =
  | RemapGuardName | "mapping_changed" | "to_registration_changed" | "to_not_live" | "from_not_dormant";
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

const guard = (name: RemapGuardName, ok: boolean, detail: string): RemapGuard => Object.freeze({ guard: name, ok, detail });

/** Pure: evaluate the guards over a read snapshot. */
export const remapGuards = (
  request: RemapRequest, mappingAgent: string | null, from: MailboxPreview, to: MailboxPreview,
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
]);

/** Read-only plan: both mailboxes' retained work, and the guard results. */
export const planRuntimeMailboxRemap = async (
  redis: Redis, request: RemapRequest, options: Readonly<{ isPidAlive?: (pid: number) => boolean }> = {},
): Promise<RemapPlan> => {
  const isAlive = options.isPidAlive ?? pidAlive;
  const key = SESSION_KEYS.runtimeMailbox(request.client, request.runtimeId);
  const [expected, from, to] = await Promise.all([redis.get(key), preview(redis, request.from, isAlive), preview(redis, request.to, isAlive)]);
  const parsed = mappingSchema.safeParse(parseJson(expected));
  const mapping = parsed.success ? parsed.data : null;
  const guards = remapGuards(request, mapping?.agent ?? null, from.view, to.view);
  return Object.freeze({
    request: Object.freeze({ ...request }),
    mapping: Object.freeze({ key, agent: mapping?.agent ?? null, working_directory: mapping?.working_directory ?? null }),
    mailboxes: Object.freeze({ from: from.view, to: to.view }),
    guards, ok: guards.every((g) => g.ok),
    notes: Object.freeze([
      `Remapping moves no work: ${from.view.queue_length} queued message(s) and ${from.view.unexpired_claims.length} unexpired claim(s) stay at ${request.from}.`,
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

/** The apply script's refusal codes; undefined for an unknown code. */
const applyRefusal = (code: number, signal: string, plan: RemapPlan): RemapRefusal | undefined => {
  switch (code) {
    case -1: return { reason: "mapping_changed", detail: `mapping ${plan.mapping.key} changed since the plan was read` };
    case -2: return { reason: "to_registration_changed", detail: `${plan.request.to}'s registration changed since its pid was checked` };
    case -3: return { reason: "to_not_live", detail: `${plan.request.to} has no heartbeat and no live session lease` };
    case -4: return { reason: "from_not_dormant", detail: `${plan.request.from} became live (${signal.replaceAll("_", " ")}); do not remap` };
    default: return undefined;
  }
};

/**
 * Apply a plan whose guards all held, in ONE atomic step. A refusal writes
 * nothing; an unexpected reply throws (infrastructure, never a refusal).
 */
export const applyRuntimeMailboxRemap = async (redis: Redis, plan: RemapPlan): Promise<RemapOutcome> => {
  const failed = plan.guards.filter((g) => !g.ok);
  const { expectedMapping, desiredMapping, toRegistration } = plan.cas;
  if (failed.length || expectedMapping === null || desiredMapping === null || toRegistration === null) {
    return Object.freeze({ status: "refused", refusals: Object.freeze(failed.map((g) => Object.freeze({ reason: g.guard, detail: g.detail }))) });
  }
  const { from, to, runtimeId, client } = plan.request;
  const reply: unknown = await redis.eval(APPLY_REMAP_SCRIPT, 8, plan.mapping.key, SESSION_KEYS.registry,
    SESSION_KEYS.runtimeBinding(from), SESSION_KEYS.heartbeat(from), SESSION_KEYS.heartbeat(to), SESSION_KEYS.agentSessions(to),
    SESSION_KEYS.provisional, SESSION_KEYS.continuityAudit,
    expectedMapping, desiredMapping, from, to, runtimeId, toRegistration, client);
  if (typeof reply === "string") return Object.freeze({ status: "applied", audit_id: reply });
  const [code, signal] = Array.isArray(reply) ? reply : [];
  const refusal = typeof code === "number" && typeof signal === "string" ? applyRefusal(code, signal, plan) : undefined;
  if (!refusal) throw new Error(`Unexpected remap reply: ${JSON.stringify(reply)}`);
  return Object.freeze({ status: "refused", refusals: Object.freeze([Object.freeze(refusal)]) });
};
