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
import { SESSION_KEYS, CLAIM_KEYS } from "./keys.js";
import {
  CodexSocketClient,
  defaultCodexSocketPath,
} from "../registered-shell/codex-socket.js";
import {
  discoverCodexDaemon,
  isGptqueueSessionSidecarArgv,
  readProcessPin,
  snapshotDaemon,
  type DaemonIdentity,
} from "../registered-shell/codex-daemon.js";

// ---------------------------------------------------------------- CLI surface

export const DEFAULT_VERIFY_TIMEOUT_MS = 30_000;

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
  "  --verify-timeout-ms  bounded post-archive verification budget (default 30000)",
  "",
  "Connects to the existing daemon behind the selected socket only; it never launches or",
  "restarts a Codex process, and never unarchives or cleans anything up.",
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
  let verifyTimeoutMs = DEFAULT_VERIFY_TIMEOUT_MS;
  const timeoutRaw = values.get("--verify-timeout-ms");
  if (timeoutRaw !== undefined) {
    if (!/^\d+$/.test(timeoutRaw) || Number(timeoutRaw) <= 0) return "--verify-timeout-ms must be a positive integer of milliseconds";
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
  scan(cursor: string, pattern: string, count: number): Promise<readonly [string, readonly string[]]>;
  get(key: string): Promise<string | null>;
  zcount(key: string, min: string, max: string): Promise<number>;
  llen(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  hget(key: string, field: string): Promise<string | null>;
}>;

export type RpcKind = "read" | "archive";

export type RetireRpc = Readonly<{
  request(method: string, params: Record<string, unknown>, kind: RpcKind): Promise<Record<string, unknown>>;
}>;

/** Minimal structural daemon/process views (satisfied by registered-shell/codex-daemon). */
export type DaemonRef = Readonly<{ pid: number; startTicks: string; argv: readonly string[]; basis: string; socketPath: string }>;
export type ChildRef = Readonly<{ pid: number; startTicks: string; argv: readonly string[] | null }>;
export type DaemonSnapshotRef = Readonly<{ daemon: DaemonRef; children: readonly ChildRef[]; sidecars: readonly ChildRef[] }>;
export type ProcPinRef = Readonly<{ pid: number; startTicks: string; ppid: number | null; argv: readonly string[] | null }>;

export type RetireDeps = Readonly<{
  store: RetireStore;
  rpc: RetireRpc;
  /** Resolve the daemon behind the selected socket; throws with a `mode` on refusal. */
  discover: () => Promise<DaemonRef>;
  /** Re-verify the pinned daemon identity and snapshot children/sidecars. */
  snapshot: () => Promise<DaemonSnapshotRef>;
  /** Pin a process by pid, or null when gone; throws when present but unreadable. */
  pinProcess: (pid: number) => Promise<ProcPinRef | null>;
  clock: () => number;
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
const SCAN_COUNT = 500;
const LOADED_PAGE_LIMIT = 500;
const LOADED_MAX_PAGES = 100;

type SidecarEvidence =
  | Readonly<{ kind: "none" }>
  | Readonly<{ kind: "pinned"; pid: number; startTicks: string }>
  | Readonly<{ kind: "gone"; pid: number }>
  | Readonly<{ kind: "foreign"; pid: number; reason: string; argv0: string | null }>
  | Readonly<{ kind: "unreadable"; pid: number }>;

const quoteIfNeeded = (value: string): string => (/^[A-Za-z0-9_@%+=:,./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`);

const restoreHint = (threadId: string, cwd: string | null, socketPath: string): string =>
  `thread/unarchive, then: codex resume ${threadId} --remote unix://${socketPath}${cwd ? ` -C ${quoteIfNeeded(cwd)}` : ""} -c check_for_update_on_startup=false`;

type ParsedThread = Readonly<{ statusType: string | null; parentThreadId: string | null; cwd: string | null; threadSource: string | null }>;

/** Defensive shape read of a thread/read result ({thread: {...}} or the fields inline). */
export const parseThreadResult = (result: Record<string, unknown>): ParsedThread | string => {
  const source = result.thread !== null && typeof result.thread === "object" && !Array.isArray(result.thread) ? result.thread as Record<string, unknown> : result;
  const status = source.status;
  const statusType = status !== null && typeof status === "object" && typeof (status as Record<string, unknown>).type === "string"
    ? (status as Record<string, unknown>).type as string
    : null;
  if (!("cwd" in source) && !("status" in source)) return "response carries neither cwd nor status";
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
  const mailbox = options.expectNoMailbox ? null : options.expectedMailbox;
  const events: RetireEvent[] = [];
  const emit = (event: RetireEvent): void => {
    events.push(event);
    deps.onEvent?.(event);
  };
  const stamp = (): string => new Date(deps.clock()).toISOString();
  const envError = (code: string, error: unknown): EnvironmentError =>
    new EnvironmentError(code, `${code}: ${messageOf(error)}`);

  const outcome = (exitCode: number, outcomeName: string, code: string | null = null, problem: string | null = null): RetireOutcome =>
    ({ exitCode, outcome: outcomeName, code, problem, events });

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
      daemon = await deps.discover();
    } catch (error) {
      throw envError(daemonErrorCode(error), error);
    }
    const readThread = async (threadId: string): Promise<ParsedThread> => {
      let result: Record<string, unknown>;
      try {
        result = await deps.rpc.request("thread/read", { threadId, includeTurns: false }, "read");
      } catch (error) {
        throw envError("rpc_error", error);
      }
      const parsed = parseThreadResult(result);
      if (typeof parsed === "string") throw envError("unexpected_response", new Error(`thread/read for ${threadId}: ${parsed}`));
      return parsed;
    };

    thread = await readThread(options.threadId);
    parentStatus = null;
    if (thread.parentThreadId !== null) {
      const parent = await readThread(thread.parentThreadId);
      parentStatus = parent.statusType ?? "unknown";
    }

    // Runtime bindings: bounded read-only SCAN; any unreadable value fails
    // closed (it could be the binding that names this thread).
    const matches: string[] = [];
    let cursor = "0";
    let examined = 0;
    do {
      let page: readonly [string, readonly string[]];
      try {
        page = await deps.store.scan(cursor, BINDING_PATTERN, SCAN_COUNT);
      } catch (error) {
        throw envError("redis_error", error);
      }
      cursor = page[0];
      examined += page[1].length;
      if (examined > BINDING_SCAN_BUDGET) {
        throw envError("binding_scan_exceeded", new Error(`runtime-binding scan exceeded ${BINDING_SCAN_BUDGET} keys; refusing to guess`));
      }
      for (const key of page[1]) {
        let raw: string | null;
        try {
          raw = await deps.store.get(key);
        } catch (error) {
          throw envError("redis_error", error);
        }
        if (raw === null) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(raw);
        } catch {
          throw envError("binding_unreadable", new Error(`runtime binding ${key} holds malformed JSON`));
        }
        const runtimeId = parsed !== null && typeof parsed === "object" ? (parsed as Record<string, unknown>).runtime_id : undefined;
        if (typeof runtimeId !== "string") {
          throw envError("binding_unreadable", new Error(`runtime binding ${key} carries no runtime_id string`));
        }
        if (runtimeId === options.threadId && key.startsWith(BINDING_PREFIX)) matches.push(key.slice(BINDING_PREFIX.length));
      }
    } while (cursor !== "0");
    if (matches.length > 1) throw new Refused([`more than one mailbox is bound to this thread (${matches.join(", ")}); duplicate evidence refuses`]);
    boundMailbox = matches[0] ?? null;

    // Durable runtime-mailbox mapping.
    const mappingKey = SESSION_KEYS.runtimeMailbox("codex", options.threadId);
    let mappingRaw: string | null;
    try {
      mappingRaw = await deps.store.get(mappingKey);
    } catch (error) {
      throw envError("redis_error", error);
    }
    if (mappingRaw === null) {
      mappedMailbox = null;
    } else {
      let mappingParsed: unknown;
      try {
        mappingParsed = JSON.parse(mappingRaw);
      } catch {
        throw envError("mapping_unreadable", new Error(`runtime mailbox mapping for this thread holds malformed JSON`));
      }
      const agent = mappingParsed !== null && typeof mappingParsed === "object" ? (mappingParsed as Record<string, unknown>).agent : undefined;
      if (typeof agent !== "string") throw envError("mapping_unreadable", new Error("runtime mailbox mapping carries no agent string"));
      mappedMailbox = agent;
    }

    const now = String(deps.clock());
    if (mailbox !== null) {
      try {
        unexpiredClaims = await deps.store.zcount(CLAIM_KEYS.index(mailbox), now, "+inf");
        queued = await deps.store.llen(SESSION_KEYS.queue(mailbox));
      } catch (error) {
        throw envError("redis_error", error);
      }
      // The registry value names the sidecar pid; unreadable evidence fails closed.
      let registryRaw: string | null;
      try {
        registryRaw = await deps.store.hget(SESSION_KEYS.registry, mailbox);
      } catch (error) {
        throw envError("redis_error", error);
      }
      if (registryRaw !== null) {
        let registryParsed: unknown;
        try {
          registryParsed = JSON.parse(registryRaw);
        } catch {
          throw envError("registry_unreadable", new Error(`registry entry for ${mailbox} holds malformed JSON`));
        }
        const pidField = registryParsed !== null && typeof registryParsed === "object" ? (registryParsed as Record<string, unknown>).pid : undefined;
        if (pidField === undefined) {
          sidecarPid = null;
        } else if (typeof pidField === "number" && Number.isInteger(pidField) && pidField > 0) {
          sidecarPid = pidField;
        } else {
          throw envError("registry_unreadable", new Error(`registry entry for ${mailbox} carries an unusable pid`));
        }
        if (sidecarPid !== null) {
          try {
            const pin = await deps.pinProcess(sidecarPid);
            if (pin === null) sidecar = { kind: "gone", pid: sidecarPid };
            else if (pin.argv !== null && isGptqueueSessionSidecarArgv(pin.argv)) {
              if (pin.ppid === daemon.pid) sidecar = { kind: "pinned", pid: sidecarPid, startTicks: pin.startTicks };
              else sidecar = { kind: "foreign", pid: sidecarPid, reason: "sidecar of another daemon (parent pid differs)", argv0: pin.argv[0] ?? null };
            } else {
              sidecar = { kind: "foreign", pid: sidecarPid, reason: "pid runs a non-sidecar process", argv0: pin.argv?.[0] ?? null };
            }
          } catch (error) {
            throw envError("sidecar_proc_unreadable", error);
          }
        }
      }
    }

    try {
      before = await deps.snapshot();
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
      if (boundMailbox !== null && boundMailbox !== mailbox) problems.push(`the mailbox bound to this thread is ${boundMailbox}, not ${mailbox}`);
      if (mappedMailbox !== null && mappedMailbox !== mailbox) problems.push(`the thread's durable mailbox mapping names ${mappedMailbox}, not ${mailbox}`);
      if (boundMailbox === null && mappedMailbox === null) problems.push(`no mailbox evidence ties this thread to ${mailbox} (no runtime binding, no durable mapping); refusing to guess`);
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
    // observational window is as small as the protocol allows.
    const reread = await readThread(options.threadId);
    const rereadParent = reread.parentThreadId !== null ? (await readThread(reread.parentThreadId)).statusType ?? "unknown" : null;
    let rereadClaims = 0;
    let rereadQueued = 0;
    if (mailbox !== null) {
      const nowAgain = String(deps.clock());
      try {
        rereadClaims = await deps.store.zcount(CLAIM_KEYS.index(mailbox), nowAgain, "+inf");
        rereadQueued = await deps.store.llen(SESSION_KEYS.queue(mailbox));
      } catch (error) {
        throw envError("redis_error", error);
      }
    }
    try {
      await deps.snapshot();
    } catch (error) {
      throw envError(daemonErrorCode(error), error);
    }
    const reproblems: string[] = [];
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
  let archiveAcknowledged = false;
  let archiveError: string | null = null;
  try {
    const result = await deps.rpc.request("thread/archive", { threadId: options.threadId }, "archive");
    archiveAcknowledged = !("error" in result) || result.error === undefined;
    if (!archiveAcknowledged) archiveError = `archive response carried an error: ${JSON.stringify(result.error)}`;
  } catch (error) {
    archiveError = messageOf(error);
  }
  emit({ at: stamp(), step: "archived", outcome: archiveAcknowledged ? "ok" : "unknown", ...(archiveError === null ? {} : { error: archiveError }) });

  const interval = Math.max(50, Math.min(500, Math.floor(options.verifyTimeoutMs / 10)));
  const deadline = deps.clock() + options.verifyTimeoutMs;
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
        result = await deps.rpc.request("thread/loaded/list", { limit: LOADED_PAGE_LIMIT, ...(cursor === null ? {} : { cursor }) }, "read");
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
        return true; // the named process no longer exists
      case "foreign": {
        // The registry pid is not a live sidecar of this daemon; re-check in
        // case a new sidecar generation adopted the very same pid.
        try {
          const pin = await deps.pinProcess(sidecar.pid);
          if (pin !== null && pin.argv !== null && isGptqueueSessionSidecarArgv(pin.argv) && pin.ppid === daemon.pid) return false;
        } catch {
          return "unknown";
        }
        return true;
      }
      case "pinned": {
        try {
          const pin = await deps.pinProcess(sidecar.pid);
          if (pin === null) return true; // positively pinned earlier, now gone
          if (pin.startTicks !== sidecar.startTicks) return true; // the pinned process ended; the pid was reused
          if (pin.argv !== null && isGptqueueSessionSidecarArgv(pin.argv)) return false; // still the pinned sidecar
          return true; // same start but no longer a sidecar: stale evidence, the sidecar is gone
        } catch {
          return "unknown";
        }
      }
      default:
        return "unknown";
    }
  };

  for (;;) {
    unloaded = await loadedContains();
    if (mailbox !== null) {
      try {
        heartbeat = (await deps.store.exists(SESSION_KEYS.heartbeat(mailbox))) === 1;
      } catch {
        heartbeat = "unknown";
      }
      sidecarExited = await sidecarExitEvidence();
      if (sidecarExited === "unknown") sidecarNote = "sidecar identity unreadable during verification; exit not provable";
    }
    try {
      after = await deps.snapshot();
    } catch (error) {
      after = null;
      sidecarNote = sidecarNote ?? `daemon identity check failed during verification: ${messageOf(error)}`;
    }
    const ok = unloaded === false &&
      (mailbox === null || (heartbeat === false && sidecarExited === true));
    if (ok) break;
    if (deps.clock() >= deadline) break;
    await deps.sleep(interval);
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
  });
  return outcome(verified && archiveAcknowledged ? 0 : 1, outcomeName, verified && archiveAcknowledged ? null : archiveAcknowledged ? "verify_failed" : "archive_outcome_unknown", null);
};

// ---------------------------------------------------------------- IO shell

/** ioredis adapter for the read-only store surface the guards use. */
export const storeOf = (redis: Redis): RetireStore => ({
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

const RPC_TIMEOUTS: Readonly<Record<RpcKind, number>> = { read: 20_000, archive: 60_000 };

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
    retryStrategy: () => null,
  });
  redis.on("error", () => undefined); // surfaced as exit-4 redis_error by the core
  const client = new CodexSocketClient(socketPath, 120_000);
  const deps: RetireDeps = {
    store: storeOf(redis),
    rpc: { request: (method, params, kind) => client.request(method, params, AbortSignal.timeout(RPC_TIMEOUTS[kind])) },
    discover: async () => {
      identity = await discoverCodexDaemon({ socketPath, procRoot });
      return identity;
    },
    snapshot: async () => {
      if (identity === null) throw new Error("daemon not discovered yet");
      const snapshot = await snapshotDaemon(identity, { procRoot });
      return { daemon: snapshot.identity, children: snapshot.children, sidecars: snapshot.sidecars };
    },
    pinProcess: (pid) => readProcessPin(procRoot, pid),
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
