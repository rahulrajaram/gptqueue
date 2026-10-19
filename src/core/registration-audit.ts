/**
 * Append-only registration audit (stream SESSION_KEYS.registrationAudit) and
 * its read-only query (`gptqueue-doctor registrations`).
 *
 * Every committed registration of a new session appends one event=registered
 * entry in the same Lua script as the registry publication (session-store.ts
 * PUBLISH_REGISTRATION_AUDITED_SCRIPT; the runtime adoption script likewise).
 * A failed attempt never reaches that script and emits nothing; a later
 * unregister, reap or rollback never removes the event. So the stream proves,
 * for a time window it fully covers, whether a name, directory or Claude
 * session ever registered.
 *
 * The audit is best-effort: it never fails or rolls back a registration. When
 * the audit key is unusable (not a stream), the script still publishes and
 * records an audit gap instead (SESSION_KEYS.registrationAuditGap, epoch ms),
 * which makes the query report coverage "incomplete" from that instant.
 *
 * Events carry the session's sessionTag(), never the raw session id (a bearer
 * credential).
 */
import type { Redis } from "ioredis";
import { SESSION_KEYS } from "./keys.js";

export const REGISTRATION_AUDIT_MAXLEN = 10_000;
export const REGISTRATIONS_MAX_EVENTS = 500;
/** Entries examined per query at most (the stream is trimmed near 10000). */
const SCAN_PAGE = 1_000;
const SCAN_CAP = 4 * REGISTRATION_AUDIT_MAXLEN;

export type AuditRuntime = "claude" | "codex" | "pi" | "generic" | "unknown";

export type RegistrationAuditEvent = Readonly<{
  name: string;
  session_tag: string;
  runtime: AuditRuntime;
  pid: number;
  cwd: string;
  /** The Claude session UUID when the registering Claude MCP path knows it, else "". */
  claude_session: string;
  /** register: a new session's registration; adoption: a live session adopted this name (runtime continuity). */
  via: "register" | "adoption";
}>;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Values the registering process already has: the client the caller declared
 * (registered shells), else the Claude MCP marker (GPTQ_CLAUDE_SESSION_RECORD=1),
 * else a generic registration. A declared client outside the enum is "unknown".
 */
export const auditRuntime = (client: string | null | undefined, env: NodeJS.ProcessEnv): AuditRuntime => {
  switch (client) {
    case "codex": return "codex";
    case "pi": return "pi";
    case "claude": return "claude";
    case null:
    case undefined:
    case "": return env.GPTQ_CLAUDE_SESSION_RECORD === "1" ? "claude" : "generic";
    default: return "unknown";
  }
};

export const auditClaudeSession = (runtime: AuditRuntime, env: NodeJS.ProcessEnv): string => {
  if (runtime !== "claude") return "";
  const candidate = env.CLAUDE_SESSION_ID || env.CLAUDE_CODE_SESSION_ID || "";
  return UUID.test(candidate) ? candidate.toLowerCase() : "";
};

/**
 * Lua: defines audit_or_gap(stream, gap, maxlen, fields). It XADDs the event
 * when `stream` is a stream or missing; otherwise (or if that XADD errors) it
 * records an audit gap: SET `gap` to the server's epoch ms, never lowering it,
 * and only when `gap` is a string or missing (anything else is left alone; the
 * query then reports coverage "unknown"). It never raises, so the enclosing
 * registration script always completes. Returns 1 audited, 0 not audited.
 */
export const auditOrGapLua = `
local function audit_or_gap(stream, gap, maxlen, fields)
  local st = redis.call('TYPE', stream).ok
  if st == 'stream' or st == 'none' then
    local r = redis.pcall('XADD', stream, 'MAXLEN', '~', maxlen, '*', unpack(fields))
    if type(r) ~= 'table' or not r.err then return 1 end
  end
  local gt = redis.call('TYPE', gap).ok
  if gt == 'string' or gt == 'none' then
    local t = redis.call('TIME')
    local now = t[1] .. string.format('%03d', math.floor(tonumber(t[2]) / 1000))
    local prev = tonumber(redis.call('GET', gap) or '')
    if prev == nil or prev < tonumber(now) then redis.pcall('SET', gap, now) end
  end
  return 0
end
`;

/** Flat XADD field list for one event (event=registered first). */
export const auditFieldList = (event: RegistrationAuditEvent): string[] => [
  "event", "registered", "name", event.name, "session_tag", event.session_tag, "runtime", event.runtime,
  "pid", String(event.pid), "cwd", event.cwd, "claude_session", event.claude_session, "via", event.via,
];

// ---------------------------------------------------------------- query

export type Coverage = "complete" | "incomplete" | "unknown";

export type RegistrationsQuery = Readonly<{
  sinceMs: number;
  untilMs: number;
  cwd?: string;
  claudeSession?: string;
  name?: string;
}>;

export type RegistrationsReport = Readonly<{
  schema_version: 1;
  since: string;
  until: string;
  coverage: Coverage;
  /**
   * The earliest instant the stream provably holds every event from (ISO), or
   * null when no such instant is known. coverage is "complete" iff this is at
   * or before since. A null with "incomplete" means a recorded audit gap falls
   * in the window but the stream itself gives no bound.
   */
  complete_from: string | null;
  events: ReadonlyArray<Readonly<{ at: string; name: string; session_tag: string; runtime: string; pid: number | null; cwd: string; claude_session: string; via: string }>>;
  truncated: boolean;
}>;

/** The subset of XINFO STREAM the coverage decision reads. */
export type StreamInfo = Readonly<{
  exists: boolean;
  /** Absent on Redis older than 7.0. */
  maxDeletedEntryId?: string;
  entriesAdded?: number;
  length: number;
  firstEntryId: string | null;
}>;

const idMs = (id: string): number => Number(id.split("-")[0]);

const assertNever = (value: never): never => {
  throw new Error(`Unhandled audit gap marker: ${JSON.stringify(value)}`);
};

type CoverageResult = Readonly<{ coverage: Coverage; complete_from: string | null }>;
const UNKNOWN: CoverageResult = { coverage: "unknown", complete_from: null };

/**
 * Coverage from the stream's own history (Redis >= 7 XINFO STREAM fields).
 * - Nothing removed (entries-added == length, max-deleted 0-0): every event
 *   since the first entry is present, so coverage starts at the first entry,
 *   i.e. when the audit first wrote to this Redis (its deploy).
 * - Entries removed: MAXLEN trimming removes the oldest entries and does NOT
 *   move max-deleted-entry-id (only XDEL does), so a trim shows as
 *   entries-added > length. The retained first entry then bounds what was
 *   trimmed (events in its own millisecond may be gone: start 1 ms later), and
 *   any XDEL bounds holes at max-deleted-entry-id + 1 ms.
 * - No stream, missing fields (older Redis) or no evidence: unknown.
 */
export const coverageOf = (info: StreamInfo, sinceMs: number): CoverageResult => {
  if (!info.exists || info.maxDeletedEntryId === undefined || info.entriesAdded === undefined) return UNKNOWN;
  const trimmed = info.entriesAdded > info.length;
  const deletedMs = info.maxDeletedEntryId === "0-0" ? null : idMs(info.maxDeletedEntryId) + 1;
  const firstMs = info.firstEntryId === null ? null : idMs(info.firstEntryId) + (trimmed ? 1 : 0);
  if (firstMs === null) return UNKNOWN;
  const fromMs = deletedMs === null ? firstMs : Math.max(firstMs, deletedMs);
  return { coverage: fromMs <= sinceMs ? "complete" : "incomplete", complete_from: new Date(fromMs).toISOString() };
};

/**
 * The audit gap marker (SESSION_KEYS.registrationAuditGap): the epoch ms of the
 * latest registration the audit could not record. absent: no gap recorded;
 * invalid: the key is not a string or does not hold epoch ms, so whether and
 * when registrations went unaudited is unknown.
 */
export type GapMarker =
  | Readonly<{ kind: "absent" }>
  | Readonly<{ kind: "at"; ms: number }>
  | Readonly<{ kind: "invalid" }>;

export const parseGap = (type: string, value: string | null): GapMarker => {
  if (type === "none" || (type === "string" && value === null)) return { kind: "absent" };
  if (type !== "string" || value === null || !/^\d{1,16}$/.test(value)) return { kind: "invalid" };
  return { kind: "at", ms: Number(value) };
};

/**
 * Stream coverage narrowed by the gap marker. An event at the gap instant is
 * missing, so the stream holds every event only from gap + 1 ms:
 * complete_from = max(stream bound, gap + 1), and a gap at or after since
 * makes coverage "incomplete" (with complete_from null when the stream gives
 * no bound). An unreadable marker makes coverage "unknown".
 */
export const withGap = (base: CoverageResult, gap: GapMarker, sinceMs: number): CoverageResult => {
  switch (gap.kind) {
    case "absent": return base;
    case "invalid": return UNKNOWN;
    case "at": {
      if (base.complete_from === null) return gap.ms >= sinceMs ? { coverage: "incomplete", complete_from: null } : base;
      const fromMs = Math.max(Date.parse(base.complete_from), gap.ms + 1);
      return { coverage: fromMs <= sinceMs ? "complete" : "incomplete", complete_from: new Date(fromMs).toISOString() };
    }
    default: return assertNever(gap);
  }
};

/** The gap marker that goes with an audit stream key (SESSION_KEYS.registrationAuditGap for the real one). */
export const gapKeyOf = (key: string): string => `${key}:gap`;

const readGap = async (redis: Redis, key: string): Promise<GapMarker> => {
  const type = await redis.type(key);
  return parseGap(type, type === "string" ? await redis.get(key) : null);
};

const readInfo = async (redis: Redis, key: string): Promise<StreamInfo> => {
  // A missing key or one of another type holds no audit events.
  if ((await redis.type(key)) !== "stream") return { exists: false, length: 0, firstEntryId: null };
  const raw = (await redis.call("XINFO", "STREAM", key)) as unknown[];
  const fields = new Map<string, unknown>();
  for (let i = 0; i + 1 < raw.length; i += 2) fields.set(String(raw[i]), raw[i + 1]);
  const first = fields.get("first-entry");
  const maxDeleted = fields.get("max-deleted-entry-id");
  const added = fields.get("entries-added");
  return {
    exists: true,
    length: Number(fields.get("length") ?? 0),
    firstEntryId: Array.isArray(first) && typeof first[0] === "string" ? first[0] : null,
    ...(typeof maxDeleted === "string" ? { maxDeletedEntryId: maxDeleted } : {}),
    ...(added === undefined ? {} : { entriesAdded: Number(added) }),
  };
};

const toObject = (fields: readonly string[]): Record<string, string> => {
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) out[fields[i]!] = fields[i + 1]!;
  return out;
};

/** Test-only seam: runs after each XRANGE page, before the next read. */
export type QueryHooks = Readonly<{ onPage?: (pageIndex: number) => Promise<void> }>;

/**
 * Read-only: TYPE, XRANGE, XINFO STREAM and GET only; it never creates a key.
 * An audit key of another type holds no events (coverage from the gap marker).
 *
 * Coverage comes from an XINFO taken AFTER the last XRANGE page (F4), never
 * from metadata read before the scan. That is sound without a snapshot:
 * trimming only ever removes a prefix of the stream, so any matching entry
 * trimmed before or during the scan has an id below the post-scan first
 * entry, and entries-added > length marks that a trim happened; complete_from
 * is then after it. An XDEL during the scan moves max-deleted-entry-id to at
 * least the deleted id, which also moves complete_from past it. Either way a
 * matching event the scan missed lies at or after since and before
 * complete_from, so coverage cannot be "complete". A stream deleted during
 * the scan reads as missing afterwards: coverage "unknown".
 */
export const queryRegistrations = async (redis: Redis, query: RegistrationsQuery, key: string = SESSION_KEYS.registrationAudit, hooks: QueryHooks = {}): Promise<RegistrationsReport> => {
  const events: Array<RegistrationsReport["events"][number]> = [];
  let truncated = false;
  if ((await redis.type(key)) === "stream") {
    let start = `${query.sinceMs}-0`;
    const end = `${query.untilMs}-18446744073709551615`;
    let scanned = 0;
    let pageIndex = 0;
    scan: while (scanned < SCAN_CAP) {
      const page = (await redis.xrange(key, start, end, "COUNT", SCAN_PAGE)) as Array<[string, string[]]>;
      for (const [id, fields] of page) {
        scanned += 1;
        const f = toObject(fields);
        if (f.event !== "registered") continue;
        if (query.name !== undefined && f.name !== query.name) continue;
        if (query.cwd !== undefined && f.cwd !== query.cwd) continue;
        if (query.claudeSession !== undefined && (f.claude_session ?? "").toLowerCase() !== query.claudeSession.toLowerCase()) continue;
        if (events.length === REGISTRATIONS_MAX_EVENTS) { truncated = true; break scan; }
        const pid = Number(f.pid);
        events.push({ at: new Date(idMs(id)).toISOString(), name: f.name ?? "", session_tag: f.session_tag ?? "", runtime: f.runtime ?? "unknown",
          pid: Number.isInteger(pid) && f.pid !== "" ? pid : null, cwd: f.cwd ?? "", claude_session: f.claude_session ?? "", via: f.via ?? "register" });
      }
      if (hooks.onPage) await hooks.onPage(pageIndex);
      pageIndex += 1;
      if (page.length < SCAN_PAGE) break;
      start = `(${page[page.length - 1]![0]}`;
    }
    if (scanned >= SCAN_CAP) truncated = true;
  }
  // Both read after the scan: a registration that could not be audited while
  // the scan ran has already raised the gap marker by the time it is read.
  const stream = coverageOf(await readInfo(redis, key), query.sinceMs);
  const { coverage, complete_from } = withGap(stream, await readGap(redis, gapKeyOf(key)), query.sinceMs);
  return { schema_version: 1, since: new Date(query.sinceMs).toISOString(), until: new Date(query.untilMs).toISOString(), coverage, complete_from, events, truncated };
};

// ---------------------------------------------------------------- CLI

export const REGISTRATIONS_HELP = [
  "usage: gptqueue-doctor registrations --since <iso> --until <iso> [--cwd <path>] [--claude-session <uuid>] [--name <n>] [--redis-url <url>] --json",
  "  Read-only (TYPE, XRANGE, XINFO STREAM, GET) query of the append-only registration audit stream",
  `  ${SESSION_KEYS.registrationAudit}: one event per committed registration, kept even after the session`,
  "  unregisters, is reaped or rolled back. At most 500 events (truncated:true beyond).",
  "  The audit is best-effort and never fails a registration: when the stream key is unusable the",
  `  registration still commits and sets the gap marker ${SESSION_KEYS.registrationAuditGap} (epoch ms).`,
  "  coverage: complete  = the stream provably holds every registration since --since (complete_from <= since);",
  "            incomplete = events before complete_from were trimmed or deleted, the audit started after",
  "                         --since, or an audit gap was recorded at or after --since;",
  "            unknown   = no audit stream on this Redis, a Redis without XINFO max-deleted-entry-id,",
  "                        or a gap marker that is not a string of epoch ms.",
  "  --redis-url defaults to $REDIS_URL, then redis://127.0.0.1:6379/0.",
  "  Mixed versions: writers on releases without the audit emit nothing. An empty complete report proves",
  "  non-registration only if every writer that could register in the window ran an audit-emitting release.",
  "  Exit codes: 0 success, 2 invalid arguments, 4 Redis unavailable.",
].join("\n");

export type CliOutcome = Readonly<{ stdout: string; stderr: string; exitCode: number }>;

type ParsedArgs = Readonly<{ query: RegistrationsQuery; redisUrl: string }>;
const VALUE_OPTIONS = new Set(["--since", "--until", "--cwd", "--claude-session", "--name", "--redis-url"]);

const parseIso = (label: string, value: string | undefined): number | string => {
  if (value === undefined) return `Required ${label}`;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && /^\d{4}-\d{2}-\d{2}/.test(value) ? ms : `${label} must be an ISO-8601 timestamp`;
};

export const parseRegistrationsArgs = (args: readonly string[], env: NodeJS.ProcessEnv): ParsedArgs | string => {
  const options = new Map<string, string>();
  let json = false;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--json") { if (json) return "Duplicate flag --json"; json = true; continue; }
    if (!VALUE_OPTIONS.has(arg)) return `Unknown option ${arg}`;
    const value = args[i + 1];
    if (value === undefined || options.has(arg)) return "Expected unique option/value pairs";
    options.set(arg, value);
    i += 1;
  }
  if (!json) return "Required --json (the only output format)";
  const since = parseIso("--since", options.get("--since"));
  if (typeof since === "string") return since;
  const until = parseIso("--until", options.get("--until"));
  if (typeof until === "string") return until;
  if (until < since) return "--until must not be earlier than --since";
  return {
    redisUrl: options.get("--redis-url") || env.REDIS_URL || "redis://127.0.0.1:6379/0",
    query: {
      sinceMs: since, untilMs: until,
      ...(options.has("--cwd") ? { cwd: options.get("--cwd")! } : {}),
      ...(options.has("--claude-session") ? { claudeSession: options.get("--claude-session")! } : {}),
      ...(options.has("--name") ? { name: options.get("--name")! } : {}),
    },
  };
};

export const runRegistrationsCli = async (
  args: readonly string[],
  env: NodeJS.ProcessEnv,
  connect: (url: string) => Redis,
): Promise<CliOutcome> => {
  if (args.includes("--help") || args.includes("-h")) return { stdout: REGISTRATIONS_HELP, stderr: "", exitCode: 0 };
  const parsed = parseRegistrationsArgs(args, env);
  if (typeof parsed === "string") return { stdout: "", stderr: `error: ${parsed}\n${REGISTRATIONS_HELP}`, exitCode: 2 };
  const redis = connect(parsed.redisUrl);
  try {
    const report = await queryRegistrations(redis, parsed.query);
    return { stdout: JSON.stringify(report), stderr: "", exitCode: 0 };
  } catch (error) {
    return { stdout: "", stderr: `error: Redis unavailable or query failed: ${(error as Error).message}`, exitCode: 4 };
  } finally {
    redis.disconnect();
  }
};
