/**
 * `gptqueue retire-thread`: retire ONE idle Codex daemon thread.
 *
 * Unloading a thread (thread/archive) stops its MCP servers, so its gptqueue
 * sidecar exits, its session closes and its mailbox goes offline. Reversible:
 * thread/unarchive + `codex resume <id> --remote unix://<socket> ...` reloads
 * it and a new sidecar re-adopts the same mailbox.
 *
 * The guard logic is pure and dependency-injected (Redis store, Codex RPC,
 * daemon discovery, process pinning, clock); `retireThreadMain` below is the
 * thin IO shell the bin/ dispatcher calls. Everything fails closed:
 *
 * - The daemon behind the selected control socket is resolved by socket-fd
 *   ownership + token-wise argv (registered-shell/codex-daemon.ts) and its
 *   pid+start-ticks identity is re-verified before and after the archive.
 * - A thread is retired only when idle, its parent (if any) is positively
 *   idle, the runtime binding AND the durable runtime-mailbox mapping match
 *   the exact expected mailbox (or, with --expect-no-mailbox, neither
 *   exists), no unexpired claims remain and the queue is empty unless
 *   --allow-pending-mail. Duplicate or unreadable binding/mapping evidence
 *   refuses instead of guessing.
 * - Exactly ONE thread/archive request is sent. If its response is lost or
 *   ambiguous the outcome is reported unknown and never retried: no outcome
 *   ever implies nothing happened once the archive RPC was sent.
 * - Verification is bounded: paginated thread/loaded/list (the first page is
 *   never assumed complete), heartbeat disappearance, and sidecar exit only
 *   from a positively pinned sidecar identity (a registry pid reused by a
 *   foreign process is never trusted as "the sidecar is alive"; an unreadable
 *   identity stays unknown, never exit).
 *
 * Cross-Redis/RPC checks are observational; there is no atomic idleness lock,
 * so a thread can in principle turn active between the last guard read and
 * the archive request. Guards are re-read immediately before the archive to
 * narrow that window, not to eliminate it.
 */
import { Redis } from "ioredis";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { z } from "zod";
import { runtimeBindingSchema } from "./runtime-binding.js";
import { parseRedisSocketUrl } from "./redis-socket-url.js";
import { SESSION_KEYS, CLAIM_KEYS } from "./keys.js";
import {
  CodexSocketClient,
  defaultCodexSocketPath,
} from "../registered-shell/codex-socket.js";
import {
  discoverCodexDaemon,
  assertDaemonIdentity,
  isGptqueueSessionSidecarArgv,
  readProcessPin,
  snapshotDaemon,
  type DaemonIdentity,
} from "../registered-shell/codex-daemon.js";

// ---------------------------------------------------------------- CLI surface

export const DEFAULT_VERIFY_TIMEOUT_MS = 30_000;
const MAX_TIMEOUT_MS = 300_000;
const PREFLIGHT_TIMEOUT_MS = 20_000;
const ARCHIVE_TIMEOUT_MS = 60_000;

const explicitRedisEndpoint = (value: string): boolean => {
  try {
    if (parseRedisSocketUrl(value)) return true;
    // Require an authority in the supplied spelling, never URL/default recovery.
    if (!/^rediss?:\/\/[^/\s?#]+(?:[/?#]|$)/u.test(value)) return false;
    const url = new URL(value);
    return ["redis:", "rediss:"].includes(url.protocol) && url.hostname.length > 0 &&
      !url.search && !url.hash && /^\/(?:[0-9]+)?$/u.test(url.pathname || "/");
  } catch { return false; }
};

export const RETIRE_THREAD_USAGE = [
  "usage: gptqueue retire-thread <thread-id> (<expected-mailbox> | --expect-no-mailbox)",
  "                   [--dry-run] [--allow-pending-mail] --redis-url <url> [--socket <path>] [--verify-timeout-ms <n>]",
].join("\n");

export const RETIRE_THREAD_HELP = [
  RETIRE_THREAD_USAGE,
  "",
  "Retire ONE idle Codex daemon thread: unload it (thread/archive), which stops its MCP",
  "servers (its gptqueue sidecar exits, its session closes, its mailbox goes offline).",
  "Reversible: thread/unarchive, then `codex resume <thread-id> --remote unix://<socket> ...`.",
  "",
  "  <thread-id>          the Codex thread to retire (must be idle)",
  "  <expected-mailbox>   the exact mailbox this thread's runtime binding and durable",
  "                       runtime-mailbox mapping must name; refusal is safer than a guess",
  "  --expect-no-mailbox  the thread never bound a mailbox (e.g. a Codex sub-agent thread);",
  "                       refused if a binding or mapping exists",
  "  --dry-run            check every precondition, change nothing, exit 0",
  "  --allow-pending-mail override the empty-queue guard (queued mail stays in the mailbox;",
  "                       nothing wakes a retired thread for it)",
  "  --redis-url          REQUIRED: the Redis endpoint holding gptqueue state (no default)",
  "  --socket             the app-server control socket; default $GPTQUEUE_CODEX_APP_SERVER_SOCKET,",
  `                       then ${defaultCodexSocketPathLabel()}`,
  "  --verify-timeout-ms  post-archive budget, 1-300000 ms (default 30000)",
  "",
  "Connects to the existing daemon behind the selected socket only; it never launches or",
  "restarts a Codex process, and never unarchives or cleans anything up.",
  "Requires a same-UID daemon and complete readable ownership/child evidence.",
  "Output: one JSON event per line on stdout (check / already_unloaded / refused / dry_run /",
  "archived / verify).",
  "Exit codes: 0 verified or already unloaded or dry-run ok; 2 usage; 3 refused, nothing",
  "changed; 4 environment error, nothing changed; 1 failed or unknown after the archive",
  "request was sent.",
].join("\n");

function defaultCodexSocketPathLabel(): string {
  return "$CODEX_HOME/app-server-control/app-server-control.sock";
}

export const GPTQUEUE_USAGE = [
  "usage: gptqueue <command> [options]",
  "",
  "commands:",
  "  retire-thread    retire one idle Codex daemon thread (run `gptqueue retire-thread --help`)",
].join("\n");

// ---------------------------------------------------------------- arg parsing

export type ParsedRetireThreadArgs = Readonly<{
  threadId: string;
  expectedMailbox: string | null;
  expectNoMailbox: boolean;
  dryRun: boolean;
  allowPendingMail: boolean;
  redisUrl: string;
  socketPath: string | null;
  verifyTimeoutMs: number;
}>;

const FLAGS = new Set(["--expect-no-mailbox", "--dry-run", "--allow-pending-mail"]);
const VALUE_OPTIONS = new Set(["--redis-url", "--socket", "--verify-timeout-ms"]);

/** Pure argv parser: returns the parsed options or a usage-error message. */
export const parseRetireThreadArgs = (args: readonly string[]): ParsedRetireThreadArgs | string => {
  const flags = new Set<string>();
  const values = new Map<string, string>();
  const positional: string[] = [];
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (FLAGS.has(arg)) {
      if (flags.has(arg)) return `Duplicate flag ${arg}`;
      flags.add(arg);
      continue;
    }
    if (VALUE_OPTIONS.has(arg)) {
      if (values.has(arg)) return `Duplicate option ${arg}`;
      const value = args[i + 1];
      if (value === undefined || value === "") return `Option ${arg} requires a value`;
      values.set(arg, value);
      i += 1;
      continue;
    }
    if (arg.startsWith("--")) return `Unknown option ${arg}`;
    positional.push(arg);
  }
  const [threadId, mailboxArg, ...extra] = positional;
  if (extra.length > 0) return `Unexpected argument ${extra[0]}`;
  if (!threadId) return "Required <thread-id>";
  const expectNoMailbox = flags.has("--expect-no-mailbox");
  if (!mailboxArg && !expectNoMailbox) return "Required <expected-mailbox> (or --expect-no-mailbox)";
  if (mailboxArg && expectNoMailbox) return "Pass either <expected-mailbox> or --expect-no-mailbox, not both";
  const redisUrl = values.get("--redis-url");
  if (!redisUrl) return "Required --redis-url (the Redis endpoint is explicit; there is no default)";
  if (!explicitRedisEndpoint(redisUrl)) return "--redis-url requires an explicit Redis host or absolute socket path";
  let verifyTimeoutMs = DEFAULT_VERIFY_TIMEOUT_MS;
  const timeoutRaw = values.get("--verify-timeout-ms");
  if (timeoutRaw !== undefined) {
    if (!/^\d+$/.test(timeoutRaw) || (!Number.isSafeInteger(Number(timeoutRaw)) || Number(timeoutRaw) <= 0 || Number(timeoutRaw) > MAX_TIMEOUT_MS)) return "--verify-timeout-ms must be an integer from 1 to 300000 milliseconds";
    verifyTimeoutMs = Number(timeoutRaw);
  }
  return {
    threadId,
    expectedMailbox: mailboxArg ?? null,
    expectNoMailbox,
    dryRun: flags.has("--dry-run"),
    allowPendingMail: flags.has("--allow-pending-mail"),
    redisUrl,
    socketPath: values.get("--socket") ?? null,
    verifyTimeoutMs,
  };
};

// ---------------------------------------------------------------- injected IO

/** The read-only Redis surface the guards use; the core drives the SCAN cursor loop. */
export type RetireStore = Readonly<{
  time(): Promise<readonly string[]>;
  scan(cursor: string, pattern: string, count: number): Promise<readonly [string, readonly string[]]>;
  get(key: string): Promise<string | null>;
  zcount(key: string, min: string, max: string): Promise<number>;
  llen(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  hget(key: string, field: string): Promise<string | null>;
}>;

export type RpcKind = "read" | "archive";

export type RetireRpc = Readonly<{
  request(method: string, params: Record<string, unknown>, kind: RpcKind, signal?: AbortSignal): Promise<Record<string, unknown>>;
}>;

/** Minimal structural daemon/process views (satisfied by registered-shell/codex-daemon). */
export type DaemonRef = Readonly<{ pid: number; startTicks: string; argv: readonly string[]; basis: string; socketPath: string }>;
export type ChildRef = Readonly<{ pid: number; startTicks: string; argv: readonly string[] | null }>;
export type DaemonSnapshotRef = Readonly<{ daemon: DaemonRef; children: readonly ChildRef[]; sidecars: readonly ChildRef[] }>;
export type ProcPinRef = Readonly<{ pid: number; startTicks: string; ppid: number | null; argv: readonly string[] | null; nsPid?: string | null }>;

export type RetireDeps = Readonly<{
  store: RetireStore;
  rpc: RetireRpc;
  /** Resolve the daemon behind the selected socket; throws with a `mode` on refusal. */
  discover: (signal?: AbortSignal) => Promise<DaemonRef>;
  /** Re-verify the pinned daemon identity and snapshot children/sidecars. */
  snapshot: (signal?: AbortSignal) => Promise<DaemonSnapshotRef>;
  /** Pin a process by pid, or null when gone; throws when present but unreadable. */
  pinProcess: (pid: number, signal?: AbortSignal) => Promise<ProcPinRef | null>;
  clock: () => number;
  monotonic?: () => number;
  /** Must close transports and settle outstanding operations on deadline expiry. */
  cancel?: () => Promise<void>;
  sleep: (ms: number) => Promise<void>;
  onEvent?: (event: RetireEvent) => void;
}>;

export type RetireEvent = Readonly<{ at: string; step: string } & Record<string, unknown>>;

export type RetireOutcome = Readonly<{
  exitCode: number;
  /** Machine-readable outcome: one of verified | already_unloaded | dry_run | refused | environment_error | verify_failed | outcome_unknown. */
  outcome: string;
  code: string | null;
  problem: string | null;
  events: readonly RetireEvent[];
}>;

// ---------------------------------------------------------------- internals

class Refused extends Error {
  constructor(readonly problems: readonly string[]) {
    super(problems.join("; "));
    this.name = "Refused";
  }
}

class EnvironmentError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "EnvironmentError";
  }
}

const BINDING_PATTERN = "gptq:runtime-binding:*";
const BINDING_PREFIX = "gptq:runtime-binding:";
/** Read-only growth breaker for the binding scan: never proportional to the whole DB. */
const BINDING_SCAN_BUDGET = 20_000;
const BINDING_SCAN_CALL_BUDGET = 100;
const SCAN_COUNT = 500;
const LOADED_PAGE_LIMIT = 500;
const LOADED_MAX_PAGES = 100;

type SidecarEvidence =
  | Readonly<{ kind: "none" }>
  | Readonly<{ kind: "pinned"; pid: number; startTicks: string; argvDigest: string; nsPid: string | null }>
  | Readonly<{ kind: "gone"; pid: number }>
  | Readonly<{ kind: "foreign"; pid: number; reason: string; argv0: string | null }>
  | Readonly<{ kind: "unreadable"; pid: number }>;

const quoteIfNeeded = (value: string): string => (/^[A-Za-z0-9_@%+=:,./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`);

const restoreArgv = (threadId: string, cwd: string | null, socketPath: string): readonly string[] =>
  ["codex", "resume", threadId, "--remote", `unix://${socketPath}`, ...(cwd ? ["-C", cwd] : []), "-c", "check_for_update_on_startup=false"];

const restoreHint = (threadId: string, cwd: string | null, socketPath: string): string =>
  `thread/unarchive, then: ${restoreArgv(threadId, cwd, socketPath).map(quoteIfNeeded).join(" ")}`;

type ParsedThread = Readonly<{ statusType: string | null; parentThreadId: string | null; cwd: string | null; threadSource: string | null }>;

/** Defensive shape read of a thread/read result ({thread: {...}} or the fields inline). */
export const parseThreadResult = (result: Record<string, unknown>): ParsedThread | string => {
  const source = result.thread !== null && typeof result.thread === "object" && !Array.isArray(result.thread) ? result.thread as Record<string, unknown> : result;
  const status = source.status;
  const statusType = status !== null && typeof status === "object" && typeof (status as Record<string, unknown>).type === "string"
    ? (status as Record<string, unknown>).type as string
    : null;
  if (!("cwd" in source) && !("status" in source)) return "response carries neither cwd nor status";
  if (source.parentThreadId !== undefined && source.parentThreadId !== null && (typeof source.parentThreadId !== "string" || source.parentThreadId.length === 0)) return "malformed parentThreadId";
  return {
    statusType,
    parentThreadId: typeof source.parentThreadId === "string" ? source.parentThreadId : null,
    cwd: typeof source.cwd === "string" ? source.cwd : null,
    threadSource: typeof source.threadSource === "string" ? source.threadSource : null,
  };
};

/** Defensive shape read of a thread/loaded/list result: ids plus whether the listing is provably complete. */
export const parseLoadedListResult = (
  result: Record<string, unknown>,
  limit: number,
): Readonly<{ ids: readonly string[]; complete: boolean; cursor: string | null }> | string => {
  const data = result.data ?? result.threads;
  if (!Array.isArray(data)) return "response carries no data array";
  const ids: string[] = [];
  for (const entry of data) {
    if (typeof entry === "string") ids.push(entry);
    else if (entry !== null && typeof entry === "object" && typeof (entry as Record<string, unknown>).id === "string") ids.push((entry as Record<string, unknown>).id as string);
    else return "data entry is neither a thread id nor {id}";
  }
  const rawCursor = result.nextCursor ?? result.next_cursor ?? result.cursor;
  if (rawCursor !== undefined && rawCursor !== null && typeof rawCursor !== "string") return "malformed pagination cursor";
  const cursor = typeof rawCursor === "string" && rawCursor !== "" ? rawCursor : null;
  // Complete only when the page was short, or an explicit cursor is exhausted:
  // a full page without cursor info never proves the listing ended.
  const complete = cursor === null && data.length < limit;
  return { ids, complete, cursor };
};

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error));
const modeOf = (error: unknown): string | null =>
  error !== null && typeof error === "object" && typeof (error as { mode?: unknown }).mode === "string"
    ? (error as { mode: string }).mode
    : null;

/** Environment code for a daemon discovery/identity error (no double "daemon_" prefix). */
const daemonErrorCode = (error: unknown): string => {
  const mode = modeOf(error);
  if (mode === null) return "daemon_error";
  return mode.startsWith("daemon_") ? mode : `daemon_${mode}`;
};

// ActivationStore.attach persists RuntimeIdentity plus its lease-owner token.
const storedBindingSchema = runtimeBindingSchema.extend({
  client: z.enum(["codex", "pi", "opencode"]), token: z.string().min(1),
}).strict();
// restoreRuntimeMailbox persists precisely these two fields (no invented version).
const mappingSchema = z.object({ agent: z.string().min(1), working_directory: z.string() }).strict();

// ---------------------------------------------------------------- core run

export type RetireOptions = Readonly<{
  threadId: string;
  expectedMailbox: string | null;
  expectNoMailbox: boolean;
  dryRun: boolean;
  allowPendingMail: boolean;
  verifyTimeoutMs: number;
  socketPath: string;
}>;

export const runRetireThread = async (options: RetireOptions, deps: RetireDeps): Promise<RetireOutcome> => {
  if (!Number.isSafeInteger(options.verifyTimeoutMs) || options.verifyTimeoutMs <= 0 || options.verifyTimeoutMs > MAX_TIMEOUT_MS) throw new Error("invalid verification budget");
  const mailbox = options.expectNoMailbox ? null : options.expectedMailbox;
  const events: RetireEvent[] = [];
  const emit = (event: RetireEvent): void => {
    events.push(event);
    deps.onEvent?.(event);
  };
  const stamp = (): string => new Date(deps.clock()).toISOString();
  // An exhausted observation budget keeps its own code: relabelling it as a
  // daemon, RPC or Redis fault would hide that the run simply ran out of time.
  const envError = (code: string, error: unknown): EnvironmentError =>
    error instanceof EnvironmentError && error.code === "deadline_exceeded"
      ? error
      : new EnvironmentError(code, `${code}: observation failed${error instanceof EnvironmentError ? ` (${error.code})` : ""}`);

  const outcome = (exitCode: number, outcomeName: string, code: string | null = null, problem: string | null = null): RetireOutcome =>
    ({ exitCode, outcome: outcomeName, code, problem, events });

  const monotonic = deps.monotonic ?? (() => performance.now());
  let deadline = monotonic() + PREFLIGHT_TIMEOUT_MS;
  const observe = async <T>(operation: (signal: AbortSignal) => Promise<T>): Promise<T> => {
    const remaining = deadline - monotonic();
    if (remaining <= 0) throw new EnvironmentError("deadline_exceeded", "observation deadline exceeded");
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let cancellation: Promise<void> | undefined;
    let timedOut = false;
    const pending = operation(controller.signal);
    try {
      return await Promise.race([pending, new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          // Save cleanup before aborting: a rejected command may win the race,
          // but finally still waits for transport shutdown and task settlement.
          cancellation = Promise.resolve().then(async () => {
            controller.abort();
            await deps.cancel?.();
            if (deps.cancel) await Promise.allSettled([pending]);
          });
          void cancellation.then(
            () => reject(new EnvironmentError("deadline_exceeded", "observation deadline exceeded")),
            () => reject(new EnvironmentError("deadline_exceeded", "observation deadline exceeded")),
          );
        }, Math.ceil(remaining));
      })]);
    } catch (error) {
      // The abort makes the in-flight operation reject too (an aborted /proc
      // read surfaces as proc_unreadable, an aborted RPC as a plain error), and
      // that rejection can win the race. Once the deadline fired, report it.
      if (timedOut) throw new EnvironmentError("deadline_exceeded", "observation deadline exceeded");
      throw error;
    } finally { clearTimeout(timer); await cancellation; }
  };
  const snapshot = async () => {
    try { return await observe((signal) => deps.snapshot(signal)); }
    catch (error) { throw envError(daemonErrorCode(error), error); }
  };
  const pinProcess = (pid: number) => observe((signal) => deps.pinProcess(pid, signal));
  const request = async (method: string, params: Record<string, unknown>, kind: RpcKind) => {
    await snapshot();
    const result = await observe((signal) => deps.rpc.request(method, params, kind, signal));
    await snapshot();
    return result;
  };
  const store: RetireStore = {
    time: () => observe(() => deps.store.time()),
    scan: (...args) => observe(() => deps.store.scan(...args)),
    get: (...args) => observe(() => deps.store.get(...args)),
    zcount: (...args) => observe(() => deps.store.zcount(...args)),
    llen: (...args) => observe(() => deps.store.llen(...args)),
    exists: (...args) => observe(() => deps.store.exists(...args)),
    hget: (...args) => observe(() => deps.store.hget(...args)),
  };
  const claimCount = async (name: string): Promise<number> => {
    const time = await store.time();
    if (time.length !== 2 || !/^\d+$/.test(time[0]!) || !/^\d+$/.test(time[1]!) || Number(time[1]) >= 1_000_000) throw new Error("invalid Redis TIME");
    const now = Number(time[0]) * 1_000 + Math.floor(Number(time[1]) / 1_000);
    if (!Number.isSafeInteger(now) || now < 0) throw new Error("invalid Redis TIME");
    const count = await store.zcount(CLAIM_KEYS.index(name), String(now), "+inf");
    if (!Number.isSafeInteger(count) || count < 0) throw new Error("invalid claim count");
    return count;
  };

  // ---- phase A: discovery, evidence, guards (nothing mutates here) ----
  let daemon: DaemonRef;
  let thread: ParsedThread;
  let parentStatus: string | null;
  let boundMailbox: string | null;
  let mappedMailbox: string | null;
  let unexpiredClaims = 0;
  let queued = 0;
  let sidecarPid: number | null = null;
  let sidecar: SidecarEvidence = { kind: "none" };
  let before: DaemonSnapshotRef;

  try {
    try {
      daemon = await observe((signal) => deps.discover(signal));
    } catch (error) {
      throw envError(daemonErrorCode(error), error);
    }
    const readThread = async (threadId: string): Promise<ParsedThread> => {
      let result: Record<string, unknown>;
      try {
        result = await request("thread/read", { threadId, includeTurns: false }, "read");
      } catch (error) {
        if (error instanceof EnvironmentError) throw error;
        throw envError("rpc_error", error);
      }
      const parsed = parseThreadResult(result);
      if (typeof parsed === "string") throw envError("unexpected_response", new Error(`thread/read for ${threadId}: ${parsed}`));
      const source = result.thread !== null && typeof result.thread === "object" ? result.thread as Record<string, unknown> : result;
      if (source.id !== threadId) throw envError("unexpected_response", new Error("thread/read identity disagrees"));
      return parsed;
    };

    thread = await readThread(options.threadId);
    parentStatus = null;
    if (thread.parentThreadId !== null) {
      const parent = await readThread(thread.parentThreadId);
      parentStatus = parent.statusType ?? "unknown";
    }

    const ownership = async () => {
      const matches = new Map<string, z.infer<typeof storedBindingSchema>>();
      let cursor = "0", examined = 0, calls = 0;
      do {
        if (++calls > BINDING_SCAN_CALL_BUDGET) throw envError("binding_scan_exceeded", new Error("runtime-binding SCAN call budget exceeded"));
        let page: readonly [string, readonly string[]];
        try { page = await store.scan(cursor, BINDING_PATTERN, SCAN_COUNT); }
        catch (error) { throw envError("redis_error", error); }
        if (!Array.isArray(page) || typeof page[0] !== "string" || !/^\d+$/.test(page[0]) || !Array.isArray(page[1])) throw envError("binding_unreadable", new Error("invalid SCAN page"));
        cursor = page[0];
        examined += page[1].length;
        if (examined > BINDING_SCAN_BUDGET) throw envError("binding_scan_exceeded", new Error("runtime-binding key budget exceeded"));
        for (const key of page[1]) {
          if (typeof key !== "string" || !key.startsWith(BINDING_PREFIX) || key.length === BINDING_PREFIX.length) throw envError("binding_unreadable", new Error("invalid binding key"));
          let raw: string | null;
          try { raw = await store.get(key); } catch (error) { throw envError("redis_error", error); }
          // Disappearance during enumeration is incomplete evidence, never emptiness.
          let parsed: z.infer<typeof storedBindingSchema>;
          try { parsed = storedBindingSchema.parse(JSON.parse(raw ?? "null")); }
          catch { throw envError("binding_unreadable", new Error("runtime binding has invalid stored shape")); }
          if (parsed.runtime_id === options.threadId) {
            if (parsed.client !== "codex") throw new Refused(["matching runtime binding belongs to another client"]);
            const name = key.slice(BINDING_PREFIX.length);
            const prior = matches.get(name);
            if (prior && JSON.stringify(prior) !== JSON.stringify(parsed)) throw new Refused(["binding owner changed during enumeration"]);
            matches.set(name, parsed);
          }
        }
      } while (cursor !== "0");
      if (matches.size > 1) throw new Refused([`more than one mailbox is bound to this thread (${[...matches.keys()].join(", ")}); duplicate evidence refuses`]);
      const binding = [...matches.entries()][0] ?? null;
      let raw: string | null;
      try { raw = await store.get(SESSION_KEYS.runtimeMailbox("codex", options.threadId)); }
      catch (error) { throw envError("redis_error", error); }
      let mapping: z.infer<typeof mappingSchema> | null = null;
      if (raw !== null) {
        try { mapping = mappingSchema.parse(JSON.parse(raw)); }
        catch { throw envError("mapping_unreadable", new Error("runtime mailbox mapping has invalid stored shape")); }
      }
      if (binding && mapping && resolve(binding[1].working_directory) !== resolve(mapping.working_directory)) throw new Refused(["binding and mapping working directories disagree"]);
      let pin: SidecarEvidence = { kind: "none" };
      let registryOwner: Record<string, unknown> | null = null;
      if (mailbox !== null) {
        let registryRaw: string | null;
        try { registryRaw = await store.hget(SESSION_KEYS.registry, mailbox); }
        catch (error) { throw envError("redis_error", error); }
        let registry: Record<string, unknown> | null;
        try {
          const parsed: unknown = JSON.parse(registryRaw ?? "null");
          if (parsed !== null && (typeof parsed !== "object" || Array.isArray(parsed))) throw new Error("invalid registry");
          registry = parsed as Record<string, unknown> | null;
        } catch { throw envError("registry_unreadable", new Error("registry entry has invalid JSON")); }
        const pid = registry?.pid;
        if (pid === undefined) return { binding, mapping, pin, registryOwner };
        if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) throw envError("registry_unreadable", new Error("registry entry has no usable pid"));
        const metadata = registry?.metadata;
        if (registry?.name !== undefined && registry.name !== mailbox) throw new Refused(["registry mailbox identity disagrees"]);
        if (metadata !== undefined && (metadata === null || typeof metadata !== "object" || Array.isArray(metadata))) throw envError("registry_unreadable", new Error("invalid registry metadata"));
        const meta = (metadata ?? {}) as Record<string, unknown>;
        for (const field of ["uuid", "client", "working_directory"]) {
          if (meta[field] !== undefined && typeof meta[field] !== "string") throw envError("registry_unreadable", new Error("invalid registry owner metadata"));
        }
        if (meta.client !== undefined && meta.client !== "codex") throw new Refused(["registry owner is not a Codex client"]);
        registryOwner = { name: registry?.name ?? null, pid, uuid: meta.uuid ?? null, client: meta.client ?? null, working_directory: meta.working_directory ?? null };
        let process: ProcPinRef | null;
        try { process = await pinProcess(pid); } catch (error) { throw envError("sidecar_proc_unreadable", error); }
        if (process === null) pin = { kind: "gone", pid };
        else if (process.argv && isGptqueueSessionSidecarArgv(process.argv) && process.ppid === daemon.pid) pin = { kind: "pinned", pid, startTicks: process.startTicks, argvDigest: createHash("sha256").update(JSON.stringify(process.argv)).digest("hex"), nsPid: process.nsPid ?? null };
        else pin = { kind: "foreign", pid, reason: "not the selected daemon's Codex sidecar", argv0: null };
      }
      return { binding, mapping, pin, registryOwner };
    };
    const evidence = await ownership();
    boundMailbox = evidence.binding?.[0] ?? null;
    mappedMailbox = evidence.mapping?.agent ?? null;
    sidecar = evidence.pin;
    sidecarPid = sidecar.kind === "none" ? null : sidecar.pid;
    if (mailbox !== null) {
      try {
        unexpiredClaims = await claimCount(mailbox);
        queued = await store.llen(SESSION_KEYS.queue(mailbox));
        if (!Number.isSafeInteger(queued) || queued < 0) throw new Error("invalid queue length");
      } catch (error) { throw envError("redis_error", error); }
    }

    try {
      before = await snapshot();
    } catch (error) {
      throw envError(daemonErrorCode(error), error);
    }

    emit({
      at: stamp(), step: "check", threadId: options.threadId, cwd: thread.cwd, status: thread.statusType,
      thread_source: thread.threadSource, parent: thread.parentThreadId, parent_status: parentStatus,
      bound_mailbox: boundMailbox, mapped_mailbox: mappedMailbox, expected_mailbox: mailbox,
      unexpired_claims: mailbox !== null ? unexpiredClaims : null, queued: mailbox !== null ? queued : null,
      sidecar_pid: sidecarPid, sidecar_evidence: sidecar.kind,
      codex_sidecars_total: before.sidecars.length, daemon_children: before.children.length,
      daemon: { pid: daemon.pid, basis: daemon.basis, socket: options.socketPath },
    });

    if (thread.statusType === "notLoaded") {
      emit({
        at: stamp(), step: "already_unloaded",
        note: "the thread is not loaded; nothing to do (no archive request is sent)",
        codex_sidecars_total: before.sidecars.length, daemon_children: before.children.length,
        restore: restoreHint(options.threadId, thread.cwd, options.socketPath),
        restore_argv: restoreArgv(options.threadId, thread.cwd, options.socketPath),
      });
      return outcome(0, "already_unloaded");
    }

    const problems: string[] = [];
    if (thread.statusType !== "idle") problems.push(`thread status is ${thread.statusType ?? "unknown"}; retire only an idle thread`);
    if (thread.parentThreadId !== null && parentStatus !== "idle") {
      problems.push(`parent thread ${thread.parentThreadId} is ${parentStatus ?? "unknown"}; retire a sub-agent thread only while its parent is positively idle`);
    }
    if (mailbox === null) {
      if (boundMailbox !== null) problems.push(`a mailbox is bound to this thread (${boundMailbox}); pass it instead of --expect-no-mailbox`);
      if (mappedMailbox !== null) problems.push(`the thread's mailbox mapping names ${mappedMailbox}; pass it instead of --expect-no-mailbox`);
    } else {
      if (boundMailbox !== mailbox) problems.push(`the mailbox bound to this thread is ${boundMailbox}, not ${mailbox}`);
      if (mappedMailbox !== mailbox) problems.push(`the thread's durable mailbox mapping names ${mappedMailbox}, not ${mailbox}`);
      if (boundMailbox === null && mappedMailbox === null) problems.push(`no mailbox evidence ties this thread to ${mailbox} (no runtime binding, no durable mapping); refusing to guess`);
      if (sidecar.kind !== "pinned") problems.push("mailbox sidecar was not positively pinned before archive");
      if (unexpiredClaims > 0) problems.push(`${mailbox} holds ${unexpiredClaims} unexpired claim(s); have the agent acknowledge them first`);
      if (queued > 0 && !options.allowPendingMail) problems.push(`${mailbox} has ${queued} queued message(s); drain them or pass --allow-pending-mail`);
    }
    if (problems.length > 0) {
      emit({ at: stamp(), step: "refused", problems, restore: restoreHint(options.threadId, thread.cwd, options.socketPath) });
      return outcome(3, "refused", "guards_failed", problems.join("; "));
    }
    if (options.dryRun) {
      emit({ at: stamp(), step: "dry_run", note: "preconditions hold; no changes made" });
      return outcome(0, "dry_run");
    }

    // Re-read the mutable guards immediately before the archive so the
    // observational window is as small as the protocol allows. The re-read is
    // its own phase with its own finite budget: on a busy daemon (hundreds of
    // children) the discovery observations alone can use most of the first
    // budget, which made every real run fail here while dry runs passed.
    deadline = monotonic() + PREFLIGHT_TIMEOUT_MS;
    const reread = await readThread(options.threadId);
    const rereadParent = reread.parentThreadId !== null ? (await readThread(reread.parentThreadId)).statusType ?? "unknown" : null;
    const fresh = await ownership();
    const ownershipChanged = JSON.stringify(fresh) !== JSON.stringify(evidence);
    let rereadClaims = 0;
    let rereadQueued = 0;
    if (mailbox !== null) {
      try {
        rereadClaims = await claimCount(mailbox);
        rereadQueued = await store.llen(SESSION_KEYS.queue(mailbox));
        if (!Number.isSafeInteger(rereadQueued) || rereadQueued < 0) throw new Error("invalid queue length");
      } catch (error) {
        throw envError("redis_error", error);
      }
    }
    try {
      await snapshot();
    } catch (error) {
      throw envError(daemonErrorCode(error), error);
    }
    const reproblems: string[] = [];
    if (ownershipChanged) reproblems.push("binding, mapping or sidecar ownership changed before archive");
    if (reread.parentThreadId !== thread.parentThreadId) reproblems.push("thread parent changed before archive");
    if (reread.statusType !== "idle") reproblems.push(`thread status changed to ${reread.statusType ?? "unknown"} before the archive`);
    if (reread.parentThreadId !== null && rereadParent !== "idle") reproblems.push(`parent thread ${reread.parentThreadId} changed to ${rereadParent ?? "unknown"} before the archive`);
    if (mailbox !== null && rereadClaims > 0) reproblems.push(`${mailbox} now holds ${rereadClaims} unexpired claim(s)`);
    if (mailbox !== null && rereadQueued > 0 && !options.allowPendingMail) reproblems.push(`${mailbox} now has ${rereadQueued} queued message(s)`);
    if (reproblems.length > 0) {
      emit({ at: stamp(), step: "refused", problems: reproblems, restore: restoreHint(options.threadId, reread.cwd, options.socketPath) });
      return outcome(3, "refused", "guards_changed", reproblems.join("; "));
    }
  } catch (error) {
    if (error instanceof Refused) {
      emit({ at: stamp(), step: "refused", problems: error.problems });
      return outcome(3, "refused", "guards_failed", error.message);
    }
    const code = error instanceof EnvironmentError ? error.code : "internal_error";
    const message = messageOf(error);
    emit({ at: stamp(), step: "environment_error", code, message });
    return outcome(4, "environment_error", code, message);
  }

  // ---- phase B: the single archive request + bounded verification ----
  // From here on, no error is ever reported as "nothing happened": the
  // archive request was sent, so the worst-case outcome is unknown (exit 1).
  deadline = monotonic() + ARCHIVE_TIMEOUT_MS;
  let archiveAcknowledged = false;
  let archiveError: string | null = null;
  try {
    const result = await request("thread/archive", { threadId: options.threadId }, "archive");
    archiveAcknowledged = !("error" in result) || result.error === undefined;
    if (!archiveAcknowledged) archiveError = "archive response carried an error";
  } catch {
    archiveError = "archive request did not produce a trusted acknowledgement";
  }
  emit({ at: stamp(), step: "archived", outcome: archiveAcknowledged ? "ok" : "unknown", ...(archiveError === null ? {} : { error: archiveError }) });

  const interval = Math.max(50, Math.min(500, Math.floor(options.verifyTimeoutMs / 10)));
  deadline = monotonic() + options.verifyTimeoutMs;
  let unloaded: boolean | "unknown" = "unknown";
  let heartbeat: boolean | "unknown" | null = mailbox !== null ? "unknown" : null;
  let sidecarExited: boolean | "unknown" | null = mailbox !== null ? "unknown" : null;
  let sidecarNote: string | null = null;
  let after: DaemonSnapshotRef | null = null;

  const loadedContains = async (): Promise<boolean | "unknown"> => {
    let cursor: string | null = null;
    for (let pageIndex = 0; pageIndex < LOADED_MAX_PAGES; pageIndex += 1) {
      let result: Record<string, unknown>;
      try {
        result = await request("thread/loaded/list", { limit: LOADED_PAGE_LIMIT, ...(cursor === null ? {} : { cursor }) }, "read");
      } catch {
        return "unknown";
      }
      const parsed = parseLoadedListResult(result, LOADED_PAGE_LIMIT);
      if (typeof parsed === "string") return "unknown";
      if (parsed.ids.includes(options.threadId)) return true;
      if (parsed.cursor !== null) {
        cursor = parsed.cursor;
        continue;
      }
      if (parsed.complete) return false;
      return "unknown"; // a full page with no cursor info never proves completeness
    }
    return "unknown";
  };

  const sidecarExitEvidence = async (): Promise<boolean | "unknown" | null> => {
    switch (sidecar.kind) {
      case "none":
        return null; // no pid was named; heartbeat disappearance is the live signal
      case "gone":
      case "foreign":
        return "unknown";
      case "pinned": {
        try {
          const pin = await pinProcess(sidecar.pid);
          if (pin === null) return true; // positively pinned earlier, now gone
          if (sidecar.nsPid !== null && pin.nsPid !== sidecar.nsPid) return "unknown";
          if (pin.startTicks !== sidecar.startTicks) return true; // the pinned process ended; the pid was reused
          if (pin.argv !== null && isGptqueueSessionSidecarArgv(pin.argv)) return false; // still the pinned sidecar
          return "unknown"; // identity changed without provable process exit
        } catch {
          return "unknown";
        }
      }
      default:
        return "unknown";
    }
  };

  for (;;) {
    if (monotonic() >= deadline) break;
    unloaded = await loadedContains();
    if (mailbox !== null) {
      try {
        const count = await store.exists(SESSION_KEYS.heartbeat(mailbox));
        heartbeat = count === 0 ? false : count === 1 ? true : "unknown";
      } catch {
        heartbeat = "unknown";
      }
      sidecarExited = await sidecarExitEvidence();
      if (sidecarExited === "unknown") sidecarNote = "sidecar identity unreadable during verification; exit not provable";
    }
    try {
      after = await snapshot();
    } catch (error) {
      after = null;
      sidecarNote = sidecarNote ?? `daemon identity check failed during verification: ${messageOf(error)}`;
    }
    const ok = unloaded === false &&
      (mailbox === null || (heartbeat === false && sidecarExited === true));
    if (ok && after !== null) break;
    if (monotonic() >= deadline) break;
    await deps.sleep(Math.min(interval, Math.max(0, deadline - monotonic())));
  }

  // Exit evidence only when the after-snapshot exists: a daemon that vanished
  // mid-verification makes "no longer present under it" vacuous, not proof.
  const exitedSidecarPids = after === null ? null : before.sidecars
    .filter((sidecarBefore) => !after.sidecars.some((current) => current.pid === sidecarBefore.pid && current.startTicks === sidecarBefore.startTicks))
    .map((sidecarBefore) => sidecarBefore.pid);
  const verified = unloaded === false &&
    (mailbox === null || (heartbeat === false && sidecarExited === true)) &&
    after !== null;
  const outcomeName = verified && archiveAcknowledged ? "verified" : archiveAcknowledged ? "verify_failed" : "outcome_unknown";
  emit({
    at: stamp(), step: "verify", outcome: outcomeName,
    unloaded: unloaded === "unknown" ? null : !unloaded,
    heartbeat: heartbeat === null ? null : heartbeat === "unknown" ? null : heartbeat,
    sidecar_exited: sidecarExited === null ? null : sidecarExited === "unknown" ? null : sidecarExited,
    sidecar_evidence: sidecar.kind, ...(sidecarNote === null ? {} : { note: sidecarNote }),
    exited_sidecar_pids: exitedSidecarPids,
    codex_sidecars_before: before.sidecars.length, codex_sidecars_after: after?.sidecars.length ?? null,
    daemon_children_before: before.children.length, daemon_children_after: after?.children.length ?? null,
    daemon: { pid: daemon.pid, basis: daemon.basis },
    restore: restoreHint(options.threadId, thread.cwd, options.socketPath),
        restore_argv: restoreArgv(options.threadId, thread.cwd, options.socketPath),
  });
  return outcome(verified && archiveAcknowledged ? 0 : 1, outcomeName, verified && archiveAcknowledged ? null : archiveAcknowledged ? "verify_failed" : "archive_outcome_unknown", null);
};

// ---------------------------------------------------------------- IO shell

/** ioredis adapter for the read-only store surface the guards use. */
export const storeOf = (redis: Redis): RetireStore => ({
  time: async () => (await redis.time()).map(String),
  scan: async (cursor, pattern, count) => {
    const [next, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", String(count)) as unknown as [string, string[]];
    return [next, keys];
  },
  get: (key) => redis.get(key),
  zcount: (key, min, max) => redis.zcount(key, min, max),
  llen: (key) => redis.llen(key),
  exists: (key) => redis.exists(key),
  hget: (key, field) => redis.hget(key, field),
});



/**
 * The thin CLI shell the bin/ dispatcher calls: wires the real Redis socket
 * URL, the existing daemon socket connection and /proc discovery, prints each
 * event as JSONL on stdout and a structured problem on stderr, and maps the
 * outcome to the process exit code. Returns the exit code instead of exiting,
 * so callers (and tests) stay in control.
 */
export const retireThreadMain = async (argv: readonly string[]): Promise<number> => {
  if (argv.includes("--help") || argv.includes("-h")) {
    process.stdout.write(`${RETIRE_THREAD_HELP}\n`);
    return 0;
  }
  const parsed = parseRetireThreadArgs(argv);
  if (typeof parsed === "string") {
    process.stderr.write(`error: ${parsed}\n${RETIRE_THREAD_USAGE}\n`);
    return 2;
  }
  const socketPath = parsed.socketPath ?? defaultCodexSocketPath();
  const procRoot = "/proc";
  let identity: DaemonIdentity | null = null;
  const redis = new Redis(parsed.redisUrl, {
    maxRetriesPerRequest: 0,
    enableOfflineQueue: false,
    connectTimeout: 2_000,
    commandTimeout: PREFLIGHT_TIMEOUT_MS,
    retryStrategy: () => null,
  });
  redis.on("error", () => undefined); // surfaced as exit-4 redis_error by the core
  const client = new CodexSocketClient(socketPath, PREFLIGHT_TIMEOUT_MS);
  let generation: number | null = null;
  const deps: RetireDeps = {
    store: storeOf(redis),
    rpc: { request: async (method, params, _kind, signal = AbortSignal.timeout(PREFLIGHT_TIMEOUT_MS)) => {
      if (identity === null) throw new Error("daemon not pinned");
      // Initial connection only; every subsequent observation and action is fenced.
      if (generation === null) generation = await client.connectGeneration(signal);
      await assertDaemonIdentity(identity, { procRoot, signal });
      const result = await client.requestOnGeneration(method, params, signal, generation);
      await assertDaemonIdentity(identity, { procRoot, signal });
      return result;
    } },
    discover: async (signal) => {
      identity = await discoverCodexDaemon({ socketPath, procRoot, signal });
      return identity;
    },
    snapshot: async (signal) => {
      if (identity === null) throw new Error("daemon not discovered yet");
      const snapshot = await snapshotDaemon(identity, { procRoot, signal });
      return { daemon: snapshot.identity, children: snapshot.children, sidecars: snapshot.sidecars };
    },
    pinProcess: (pid, signal) => readProcessPin(procRoot, pid, signal),
    cancel: async () => { redis.disconnect(); await client.close(); },
    monotonic: () => performance.now(),
    clock: () => Date.now(),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    onEvent: (event) => process.stdout.write(`${JSON.stringify(event)}\n`),
  };
  try {
    const result = await runRetireThread(
      {
        threadId: parsed.threadId,
        expectedMailbox: parsed.expectedMailbox,
        expectNoMailbox: parsed.expectNoMailbox,
        dryRun: parsed.dryRun,
        allowPendingMail: parsed.allowPendingMail,
        verifyTimeoutMs: parsed.verifyTimeoutMs,
        socketPath,
      },
      deps,
    );
    if (result.exitCode !== 0 && result.problem !== null) {
      process.stderr.write(`${JSON.stringify({ error: result.code ?? "error", message: result.problem })}\n`);
    }
    return result.exitCode;
  } finally {
    redis.disconnect();
    await client.close().catch(() => undefined);
  }
};
