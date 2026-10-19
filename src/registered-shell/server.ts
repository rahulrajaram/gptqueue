import { randomUUID } from "node:crypto";
import { basename, resolve } from "node:path";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { RedisClient } from "../mcp-server/redis-client.js";
import { createBoundMcpServer } from "../experimental-wrapper/bridge.js";
import { createLifecycleLog, safeLifecycleCode } from "./lifecycle-log.js";
import { createRuntimeController } from "./runtime-controller.js";
import { createCodexRuntime } from "./codex-runtime.js";
import { bindingLifecycleEntry, bootstrapCodexBinding, withBindingState, type CodexBindingState } from "./codex-bootstrap.js";
import { activationOutcomeSchema, type ActivationOutcome } from "./runtime.js";
import { SESSION_KEYS } from "../core/keys.js";
import { parseRedisSocketUrl } from "../core/redis-socket-url.js";

export type ShellClient = "codex" | "pi";
export interface RegisteredShellOptions {
  readonly client: ShellClient;
  readonly redisUrl: string;
  readonly cwd?: string;
  readonly startupTimeoutMs?: number;
  readonly cleanupTimeoutMs?: number;
}
export interface RegisteredShellHandle {
  readonly agentName: string;
  readonly sessionId: string;
  readonly closed: Promise<void>;
  readonly close: () => Promise<void>;
}

export interface ShellIdentity {
  readonly name: string;
  readonly label: string;
  readonly uuid: string;
  readonly client: ShellClient;
  readonly working_directory: string;
}

export const shellIdentity = (client: ShellClient, cwd = process.cwd(), uuid = randomUUID()): ShellIdentity => {
  const working_directory = resolve(cwd);
  const directory = basename(working_directory) || working_directory;
  const safe = directory.replace(/[^a-zA-Z0-9_-]/gu, "-").slice(0, 32) || "shell";
  return Object.freeze({ name: `gptqueue-shell-${client}-${safe}-${uuid}`, label: `${directory} · ${client} · ${uuid.slice(0, 8)}`, uuid, client, working_directory });
};

export const shellAgentName = (client: ShellClient, cwd = process.cwd()): string => {
  return shellIdentity(client, cwd).name;
};

const parsePairs = (
  tokens: readonly string[],
  values: Readonly<Record<string, string>> = {}
): Readonly<Record<string, string>> => {
  if (tokens.length === 0) return values;
  const [key, value, ...rest] = tokens;
  if ((key !== "--client" && key !== "--redis-url") || !value || value.startsWith("--")) {
    throw new Error("Expected --client and --redis-url option/value pairs");
  }
  if (key in values) throw new Error("Duplicate registered-shell option");
  return parsePairs(rest, { ...values, [key]: value });
};

export const parseRegisteredShellArgs = (argv: readonly string[]): RegisteredShellOptions => {
  const values = parsePairs(argv);
  const client = values["--client"];
  const redisUrl = values["--redis-url"];
  if (client !== "codex" && client !== "pi") throw new Error("--client must be codex or pi");
  if (!redisUrl) throw new Error("--redis-url is required and must be explicit");
  // Unix-socket form (/abs/path.sock[?db=N]); throws its own descriptive error.
  if (parseRedisSocketUrl(redisUrl)) return Object.freeze({ client, redisUrl, cwd: process.cwd() });
  try {
    const parsed = new URL(redisUrl);
    if (!['redis:', 'rediss:'].includes(parsed.protocol) || !parsed.hostname ||
        parsed.search || parsed.hash || !/^\/(?:[0-9]+)?$/u.test(parsed.pathname || "/")) {
      throw new Error("invalid");
    }
  } catch { throw new Error("Invalid Redis URL (expected redis://host/database)"); }
  return Object.freeze({ client, redisUrl, cwd: process.cwd() });
};

const bounded = async <T>(
  work: Promise<T>, milliseconds: number, message: string, signal?: AbortSignal
): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      onAbort = () => reject(new Error("Registered shell operation aborted"));
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    })]);
  } finally {
    if (timer) clearTimeout(timer);
    if (onAbort) signal?.removeEventListener("abort", onAbort);
  }
};

/** Register before MCP initialize can succeed; close only this connection's session. */
export const startRegisteredShell = async (
  options: RegisteredShellOptions,
  callerSignal: AbortSignal = new AbortController().signal,
  transportFactory: () => Transport = () => new StdioServerTransport()
): Promise<RegisteredShellHandle> => {
  callerSignal.throwIfAborted();
  const lifecycleStart = process.hrtime.bigint();
  const identity = shellIdentity(options.client, options.cwd);
  const agentName = identity.name;
  const lifecycle = createLifecycleLog(agentName, options.client, lifecycleStart);
  lifecycle.emit({ event: "startup_started", phase: "startup" });
  const registrationStarted = lifecycle.elapsed();
  const redis = new RedisClient(null, options.redisUrl);
  const shutdown = new AbortController();
  let server: ReturnType<typeof createBoundMcpServer> | undefined;
  let bindingState: CodexBindingState | undefined;
  const runtime = createRuntimeController(redis,
    { client: options.client, working_directory: identity.working_directory }, async (binding) => {
      if (binding.client === "codex") return createCodexRuntime(binding, { expectedAgent: redis.requireRegistered() });
      return {
        binding,
        activate: async (request, signal) => {
          if (!server) return { status: "unavailable" as const };
          try {
            return await server.server.request({ method: "gptqueue/activate", params: { binding, request } },
              activationOutcomeSchema,
              { signal, timeout: 10_000 }) as ActivationOutcome;
          } catch { return { status: "ambiguous" as const }; }
        },
        close: async () => undefined,
      };
    // The host closed this sidecar's thread: retire the session so its heartbeat
    // and lease stop claiming the mailbox and a resumed thread can adopt it.
    }, () => { void cleanup("runtime_closed").catch(() => undefined); });
  let registrationComplete = false;
  let cleanupPromise: Promise<void> | undefined;
  let initializedLogged = false;
  let resolveClosed!: () => void;
  const closed = new Promise<void>((resolve) => { resolveClosed = resolve; });
  const cleanup = (requestedReason: "caller_abort" | "transport_closed" | "explicit_close" | "startup_failed" | "runtime_closed" = "explicit_close"): Promise<void> => {
    // Defer the body until the memoized promise is assigned: transport.close re-enters onclose.
    cleanupPromise ??= Promise.resolve().then(async () => {
      const reason = requestedReason;
      const sessionId = redis.sessionId ?? undefined;
      const shutdownStarted = lifecycle.elapsed();
      lifecycle.emit({ event: "shutdown_started", reason, session_id: sessionId });
      shutdown.abort();
      try {
        await bounded((async () => {
          await runtime.close();
          // Neither a failed nor a hanging transport may prevent session retirement.
          const outcomes = await Promise.allSettled([
            Promise.resolve().then(() => server?.close()),
            redis.registered ? redis.closeCurrentSession() : Promise.resolve(),
          ]);
          const failures = outcomes.flatMap((outcome) => outcome.status === "rejected" ? [outcome.reason] : []);
          if (failures.length) throw new AggregateError(failures, "Registered shell cleanup failed");
        })(), options.cleanupTimeoutMs ?? 2_000, "Registered shell cleanup timed out");
        lifecycle.emit({ event: "shutdown_complete", phase: "shutdown", duration_ms: lifecycle.elapsed() - shutdownStarted, session_id: sessionId, reason });
      } catch (error) {
        lifecycle.emit({ event: "shutdown_failed", phase: "shutdown", duration_ms: lifecycle.elapsed() - shutdownStarted, code: safeLifecycleCode(error), session_id: sessionId, reason });
        throw error;
      } finally {
        redis.forceDisconnect();
        callerSignal.removeEventListener("abort", onAbort);
        // Slow or broken logging cannot block Redis cleanup or hold shutdown open.
        await bounded(lifecycle.close(), 200, "Lifecycle log close timed out").catch(() => undefined);
        resolveClosed();
      }
    });
    return cleanupPromise;
  };
  const onAbort = () => {
    shutdown.abort();
    // The owner observes any cleanup failure through close(); avoid an unhandled rejection.
    void cleanup("caller_abort").catch(() => undefined);
  };
  callerSignal.addEventListener("abort", onAbort, { once: true });
  try {
    await bounded(redis.register("both", agentName, `${options.client} interactive shell in ${identity.working_directory}`, identity, { provisional: true }),
      options.startupTimeoutMs ?? 10_000, "Registered shell startup timed out", shutdown.signal);
    registrationComplete = true;
    lifecycle.emit({ event: "registration_complete", phase: "registration", duration_ms: lifecycle.elapsed() - registrationStarted, session_id: redis.sessionId ?? undefined });
    shutdown.signal.throwIfAborted();
    const sessionId = redis.sessionId;
    if (!sessionId) throw new Error("Registration returned no session");
    // get_runtime_status reports the binding supervisor's state while unbound (WK-6).
    const runtimeTools = { ...runtime, status: () => withBindingState(runtime.status(), bindingState) };
    server = createBoundMcpServer({ agentName, redisClient: redis, runtime: runtimeTools }, shutdown.signal);
    // Describe the implementation instantiated by this process, not files currently on disk.
    const registryKey = SESSION_KEYS.registry;
    const registrationRaw = await redis.adapterConnection.hget(registryKey, agentName);
    if (!registrationRaw) throw new Error("Registered shell metadata disappeared");
    const registration = JSON.parse(registrationRaw);
    // Publishing a completed value also drops the name's rollback bookkeeping (R2).
    await redis.sessions.publishRegistration(agentName, JSON.stringify({ ...registration,
      metadata: { ...registration.metadata, protocol_version: 2,
        tool_names: ["send_message", "receive_message", "list_agents", "get_queue_status",
          "claim_tasks", "acknowledge_tasks", "renew_claim", "bind_runtime", "get_runtime_status",
          "acknowledge_messages", "find_agents", "get_agent_details", "get_delivery_status", "set_agent_profile"] },
    }));
    const transportStarted = lifecycle.elapsed();
    const previousInitialized = server.server.oninitialized;
    server.server.oninitialized = () => {
      try { previousInitialized?.(); }
      finally {
        if (!initializedLogged && !shutdown.signal.aborted) {
          initializedLogged = true;
          lifecycle.emit({ event: "mcp_initialized", phase: "initialization", duration_ms: lifecycle.elapsed() - transportStarted, session_id: sessionId });
        }
      }
    };
    server.server.onclose = () => { void cleanup("transport_closed").catch(() => undefined); };
    await bounded(server.connect(transportFactory()), options.startupTimeoutMs ?? 10_000,
      "Registered shell transport startup timed out", shutdown.signal);
    lifecycle.emit({ event: "transport_connected", phase: "transport", duration_ms: lifecycle.elapsed() - transportStarted, session_id: sessionId });
    shutdown.signal.throwIfAborted();
    if (options.client === "codex") {
      lifecycle.emit({ event: "native_binding_started", phase: "binding", session_id: sessionId });
      // Status follows every attempt; the lifecycle log only state changes.
      const onProgress = (state: CodexBindingState) => { bindingState = state; };
      const onState = (state: CodexBindingState) => {
        lifecycle.emit({ ...bindingLifecycleEntry(state, runtime.status()), session_id: sessionId });
      };
      const provisional = redis.requireRegistered();
      // The current name: a bind may adopt a different mailbox (COR-F4).
      void bootstrapCodexBinding(runtime, () => redis.agentName ?? provisional, identity.working_directory, shutdown.signal,
        undefined, { onState, onProgress })
        .catch(() => lifecycle.emit({ event: "native_binding_unavailable", phase: "binding",
          session_id: sessionId, code: "native_transport_error" }));
    }
    return Object.freeze({ get agentName() { return redis.agentName ?? agentName; },
      get sessionId() { return redis.sessionId ?? sessionId; }, closed, close: cleanup });
  } catch (error) {
    lifecycle.emit({ event: "startup_failed", phase: registrationComplete ? "transport" : "registration", code: safeLifecycleCode(error), session_id: redis.sessionId ?? undefined });
    // Break blocked registration immediately, but retain healthy Redis for transport-failure cleanup.
    if (!registrationComplete) redis.forceDisconnect();
    await cleanup("startup_failed").catch(() => undefined);
    throw error;
  }
};

export const runRegisteredShell = async (argv: readonly string[] = process.argv.slice(2)): Promise<void> => {
  const options = parseRegisteredShellArgs(argv);
  const stop = new AbortController();
  const interrupt = () => { process.exitCode = 130; stop.abort(); };
  const terminate = () => { process.exitCode = 143; stop.abort(); };
  const eof = () => stop.abort();
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", terminate);
  process.stdin.once("end", eof);
  try {
    const handle = await startRegisteredShell(options, stop.signal);
    if (process.stdin.readableEnded) stop.abort();
    await handle.closed;
    await handle.close();
  } finally {
    process.off("SIGINT", interrupt);
    process.off("SIGTERM", terminate);
    process.stdin.off("end", eof);
  }
};
