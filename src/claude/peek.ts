/**
 * Non-consuming inbox check; never pops, claims or acknowledges.
 *   gptqueue-inbox-peek [--name N] [--dir D] [--json]
 *       print the unclaimed count (or, with --json, every count and the identity)
 *   gptqueue-inbox-peek --watch [--name N] [--session ID] [--timeout S] [--block-ms MS]
 *       wait until the inbox is non-empty, print one JSON line, exit 0.
 *       Without --name it resolves this session's registered name from the
 *       session record, waiting up to GPTQ_CLAUDE_RECORD_WAIT_MS (default 40000).
 *       Exit 3 on timeout (0 = none), 4 when Redis stays unreachable (session mode only;
 *       with --name it retries at --interval forever, like the old script),
 *       5 when no session record appears.
 *
 * The watcher waits on the recipient's inbox-events stream (XREAD BLOCK, the
 * stream every send appends to in the same Lua call as the RPUSH) and
 * re-checks LLEN after each wake or block timeout, so requeued/recovered mail
 * that emits no event is still seen within one block period. With --session
 * it writes a pidfile the Stop hook uses to tell whether a watcher is running.
 */
import { Redis } from "ioredis";
import { rmSync } from "node:fs";
import { SESSION_KEYS } from "../core/keys.js";
import { isConfirmed, probeClient, probeInbox, recordWaitMs, resolveIdentity, waitForIdentity } from "./inbox-probe.js";
import { ensureRuntimeDir, readProcess, readWatcherPid, watcherPidPath, writeWatcherPid } from "./session-record.js";

const USAGE = `Non-consuming gptqueue inbox check; never pops a message.
  gptqueue-inbox-peek [--name N] [--dir D] [--json]          print the pending count
  gptqueue-inbox-peek --watch [--name N] [--session ID] [--interval S] [--timeout S]
      wait until the inbox is non-empty, print one JSON line, exit 0.
      Exit 3 on timeout (0 = no timeout), 2 on bad arguments.
`;

type PeekArgs = Readonly<{ intervalSec: number; name: string | null; dir: string | null; watch: boolean; json: boolean; session: string | null; timeoutSec: number; blockMs: number }>;

export const parsePeekArgs = (argv: readonly string[]): PeekArgs | string => {
  const out = { intervalSec: 30, name: null as string | null, dir: null as string | null, watch: false, json: false, session: null as string | null, timeoutSec: 0, blockMs: 5000 };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const value = () => { const v = argv[++i]; if (v === undefined) throw new Error(`${arg} needs a value`); return v; };
    try {
      switch (arg) {
        case "--name": out.name = value(); break;
        case "--dir": out.dir = value(); break;
        case "--watch": out.watch = true; break;
        case "--json": out.json = true; break;
        case "--session": out.session = value(); break;
        case "--timeout": out.timeoutSec = Number(value()); break;
        case "--block-ms": out.blockMs = Number(value()); break;
        case "--interval": out.intervalSec = Number(value()); break; // legacy outage retry cadence; the watch itself is event-driven
        case "-h": case "--help": return "help";
        default: return `unknown argument: ${String(arg)}`;
      }
    } catch (error) { return (error as Error).message; }
  }
  if (!Number.isFinite(out.intervalSec) || out.intervalSec <= 0 || !Number.isFinite(out.timeoutSec) || out.timeoutSec < 0 || !Number.isFinite(out.blockMs) || out.blockMs < 10) return "invalid --timeout or --block-ms";
  return Object.freeze(out);
};

const latestId = async (redis: Redis, agent: string): Promise<string> =>
  (await redis.xrevrange(SESSION_KEYS.inboxEvents(agent), "+", "-", "COUNT", 1))[0]?.[0] ?? "0-0";

/**
 * The name-free watcher waits this long for the session record: longer than
 * the MCP server's worst-case startup registration (lease TTL + 2 s = 32 s
 * for an old-format holder of the name), since the watcher runs in the background.
 */
export const WATCH_RECORD_WAIT_MS = 40_000;

/**
 * Redis-outage policy. Legacy invocations (explicit --name) keep the old
 * script's behaviour: retry at --interval and never exit on Redis errors.
 * The name-free session mode gives up after 30 s (exit 4) so the woken model
 * sees the outage.
 */
export const outagePolicy = (args: Pick<PeekArgs, "name" | "intervalSec">): Readonly<{ giveUpAfterMs: number | null; retryMs: number }> =>
  args.name !== null ? { giveUpAfterMs: null, retryMs: args.intervalSec * 1000 } : { giveUpAfterMs: 30_000, retryMs: 1000 };

export type WatchOutcome =
  | Readonly<{ kind: "mail"; pending: number }>
  | Readonly<{ kind: "timeout" }>
  | Readonly<{ kind: "unavailable"; message: string }>;

/** Wait for unclaimed mail. Non-consuming: only XREVRANGE, LLEN and XREAD. */
export const watchInbox = async (url: string, agent: string, opts: Readonly<{ timeoutSec: number; blockMs: number; signal?: AbortSignal; giveUpAfterMs: number | null; retryMs: number }>): Promise<WatchOutcome> => {
  const deadline = opts.timeoutSec > 0 ? Date.now() + opts.timeoutSec * 1000 : Infinity;
  const redis = new Redis(url, { maxRetriesPerRequest: 1, connectTimeout: 2000, retryStrategy: (n) => Math.min(n * 200, 1000) });
  redis.on("error", () => undefined);
  let failingSince: number | null = null;
  let after: string | null = null;
  try {
    while (!opts.signal?.aborted) {
      try {
        // Read the stream position BEFORE LLEN: the send appends both in one
        // Lua call, so a message is either counted or wakes the XREAD.
        after ??= await latestId(redis, agent);
        const pending = await redis.llen(SESSION_KEYS.queue(agent));
        if (pending > 0) return { kind: "mail", pending };
        const remaining = deadline - Date.now();
        if (remaining <= 0) return { kind: "timeout" };
        const result = await redis.xread("BLOCK", Math.max(1, Math.min(opts.blockMs, remaining)), "STREAMS", SESSION_KEYS.inboxEvents(agent), after) as [string, [string, string[]][]][] | null;
        after = result?.[0]?.[1]?.at(-1)?.[0] ?? after;
        failingSince = null;
      } catch (error) {
        failingSince ??= Date.now();
        if (opts.giveUpAfterMs !== null && Date.now() - failingSince > opts.giveUpAfterMs) return { kind: "unavailable", message: (error as Error).message };
        if (Date.now() >= deadline) return { kind: "timeout" };
        await new Promise((r) => setTimeout(r, Math.max(1, Math.min(opts.retryMs, deadline - Date.now())))); // LLEN fallback cadence while Redis fails
      }
    }
    return { kind: "timeout" };
  } finally { redis.disconnect(); }
};

export const runPeek = async (argv: readonly string[], env: NodeJS.ProcessEnv = process.env): Promise<number> => {
  const args = parsePeekArgs(argv);
  if (args === "help") { process.stdout.write(USAGE); return 0; }
  if (typeof args === "string") { process.stderr.write(`${args}\n`); return 2; }
  const url = env.REDIS_URL || "redis://127.0.0.1:6379/0";
  // An explicit --dir wins over the Claude project dir for the derived fallback name.
  if (args.dir) env = { ...env, CLAUDE_PROJECT_DIR: "" };
  // A watcher without --name must watch this session's registered inbox, never
  // a guessed one: wait (bounded) for the session record, else fail clearly.
  const identity = args.name
    ? Object.freeze({ agent: args.name, gptqueue_session_id: null, source: "env" as const, note: "name given with --name" })
    : args.watch ? await waitForIdentity(args.dir ?? undefined, recordWaitMs(env, WATCH_RECORD_WAIT_MS), env) : resolveIdentity(args.dir ?? undefined, env);
  if (args.watch && !args.name && !isConfirmed(identity)) {
    process.stdout.write(`${JSON.stringify({ error: "session_identity_unconfirmed", message: `no gptqueue session record for this Claude session (${identity.note}); not watching a guessed mailbox` })}\n`);
    return 5;
  }
  // --name skips identity lookup, but this session's claims still need its session id.
  const sessionId = args.name ? (() => { const id = resolveIdentity(args.dir ?? undefined, env); return id.agent === args.name ? id.gptqueue_session_id : null; })() : identity.gptqueue_session_id;
  if (!args.watch) {
    const redis = probeClient(url);
    const probe = await probeInbox(redis, identity.agent, sessionId).finally(() => redis.disconnect());
    if (!probe.ok) { process.stderr.write(`gptqueue-inbox-peek: ${probe.code}: ${probe.message}\n`); return 1; }
    process.stdout.write(args.json ? `${JSON.stringify({ agent: identity.agent, source: identity.source, note: identity.note, ...probe.counts })}\n` : `${probe.counts.unclaimed}\n`);
    return 0;
  }

  let pidPath: string | null = null;
  if (args.session) {
    pidPath = watcherPidPath(ensureRuntimeDir(env), args.session);
    writeWatcherPid(pidPath, { pid: process.pid, start: readProcess(process.pid)?.start ?? "", agent: identity.agent });
  }
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  try {
    const outcome = await watchInbox(url, identity.agent, { timeoutSec: args.timeoutSec, blockMs: args.blockMs, signal: controller.signal, ...outagePolicy(args) });
    switch (outcome.kind) {
      case "mail": process.stdout.write(`${JSON.stringify({ agent: identity.agent, pending: outcome.pending, at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z") })}\n`); return 0;
      case "timeout": process.stdout.write(`${JSON.stringify({ agent: identity.agent, pending: 0, timeout: true })}\n`); return 3;
      case "unavailable": process.stdout.write(`${JSON.stringify({ agent: identity.agent, error: "redis_unavailable", message: outcome.message })}\n`); return 4;
      default: { const unreachable: never = outcome; throw new Error(String(unreachable)); }
    }
  } finally {
    if (pidPath && readWatcherPid(pidPath)?.pid === process.pid) rmSync(pidPath, { force: true });
  }
};
