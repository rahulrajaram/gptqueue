import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { validateIdentity as validateSessionIdentity } from "./opencode-sessions.js";
import { RedisClient } from "../mcp-server/redis-client.js";
import { createBoundMcpServer } from "../experimental-wrapper/bridge.js";
import { startInboxDispatcher, type InboxDispatcher } from "./inbox-dispatcher.js";
import {
  createOpenCodeRuntime,
  type OpenCodeRuntimeOptions,
  type OpenCodeRuntimePort,
} from "./opencode-runtime.js";
import {
  OpenCodeSessionOwner,
  type OpenCodeSessionBackend,
  type OpenCodeSessionBackendFactory,
  type OpenCodeSessionIdentity,
} from "./opencode-sessions.js";
import type { RuntimeTools } from "./runtime-tools.js";
import type { RuntimeAdapter } from "./runtime.js";
import { VERSION } from "../version.js";
import { GptQueueError } from "../mcp-server/errors.js";
import { isExplicitWorker } from "../claude/worker-mode.js";
import { HEADLESS_WORKER_MESSAGE } from "./worker-shell.js";

export type OpenCodeBackendOptions = Readonly<{
  redisUrl: string;
  runtimePort: OpenCodeRuntimePort;
  /** Environment for the worker-mode guard (default process.env). */
  env?: NodeJS.ProcessEnv;
  epoch?: string;
  runtimeOptions?: OpenCodeRuntimeOptions;
  dispatcherOptions?: Readonly<{ intervalMs?: number; maxAttempts?: number }>;
  /** How often a backend without a live dispatcher retries taking activation ownership. */
  activationRetryMs?: number;
}>;

export type OpenCodeBackendFactoryOptions = Readonly<{
  redisUrl: string;
  runtimePort: (
    identity: OpenCodeSessionIdentity
  ) => OpenCodeRuntimePort | Promise<OpenCodeRuntimePort>;
  epoch?: string | ((identity: OpenCodeSessionIdentity) => string);
  runtimeOptions?: OpenCodeRuntimeOptions;
  dispatcherOptions?: Readonly<{ intervalMs?: number; maxAttempts?: number }>;
  activationRetryMs?: number;
}>;

const DEFAULT_ACTIVATION_RETRY_MS = 5_000;

export const opencodeAgentName = (sessionID: string): string =>
  `gptqueue-opencode-${sessionID}`;

const validateIdentity = (identity: OpenCodeSessionIdentity): void => {
  try {
    validateSessionIdentity(identity);
  } catch {
    throw new Error("OpenCode backend requires a bounded sessionID and absolute directory");
  }
};

const cleanup = async (
  redis: RedisClient,
  dispatcher: InboxDispatcher | undefined,
  server: ReturnType<typeof createBoundMcpServer> | undefined,
  client: Client | undefined,
  sessionRegistered: boolean,
): Promise<void> => {
  const failures: unknown[] = [];
  if (dispatcher) {
    try { await dispatcher.close(); } catch (error) { failures.push(error); }
  }
  if (server) {
    try { await server.close(); } catch (error) { failures.push(error); }
  }
  if (client) {
    try { await client.close(); } catch (error) { failures.push(error); }
  }
  if (sessionRegistered) {
    try { await redis.closeCurrentSession(); } catch (error) { failures.push(error); }
  }
  try { await redis.shutdown(); } catch (error) { failures.push(error); }
  if (failures.length > 0) throw new AggregateError(failures, "OpenCode backend cleanup failed");
};

/**
 * Create one identity-bound MCP connection for one trusted native session.
 * The returned surface forwards only bound MCP calls; Redis/session credentials
 * remain private to this backend.
 */
export const createOpenCodeBackend = async (
  identity: OpenCodeSessionIdentity,
  options: OpenCodeBackendOptions,
): Promise<OpenCodeSessionBackend> => {
  validateIdentity(identity);
  // Defense in depth: the plugin never calls this in worker mode; refuse before any Redis connection.
  if (isExplicitWorker(options.env ?? process.env)) throw new Error(HEADLESS_WORKER_MESSAGE);
  const agentName = opencodeAgentName(identity.sessionID);
  const redis = new RedisClient(null, options.redisUrl);
  const shutdown = new AbortController();
  // Stops activation supervision first; `shutdown` still ends the MCP server after cleanup.
  const stopping = new AbortController();
  let dispatcher: InboxDispatcher | undefined;
  let server: ReturnType<typeof createBoundMcpServer> | undefined;
  let client: Client | undefined;
  let runtime: RuntimeAdapter | undefined;
  let sessionRegistered = false;
  let dispatcherClosed = false;
  // Why activation is not running, when it is not: e.g. another process holds
  // the runtime-binding lease. Tools keep working; activation is retried.
  let activationError: string | undefined;
  // The typed cause (e.g. RUNTIME_BINDING_HELD), for callers that branch on it.
  let activationErrorCode: string | undefined;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let closeResult: Promise<void> | undefined;

  try {
    await redis.register(
      "both",
      agentName,
      `OpenCode native session ${identity.sessionID}`,
      { label: agentName, uuid: null, client: null, working_directory: identity.directory },
      // The name is derived from the native session id, so only processes on
      // this same OpenCode session share it (D2 opt-in). A second process keeps
      // its tools and waits in standby; the runtime-binding lease still admits
      // exactly one activation owner.
      { shared: true },
    );
    sessionRegistered = true;
    const binding = {
      client: "opencode" as const,
      runtime_id: identity.sessionID,
      epoch: typeof options.epoch === "string" ? options.epoch : identity.sessionID,
      working_directory: identity.directory,
    };
    const createdRuntime = await createOpenCodeRuntime(binding, options.runtimePort,
      options.runtimeOptions);
    runtime = createdRuntime;
    // The backend owns the adapter for its whole life: a dispatcher that stops
    // (e.g. lease lost) must not close it, or every restarted dispatcher would
    // activate through a closed adapter while status reports ready.
    const dispatcherRuntime: RuntimeAdapter = Object.freeze({ ...createdRuntime, close: async () => undefined });
    let runtimeTools: RuntimeTools;
    runtimeTools = {
      bind: async () => ({ status: "error", code: "runtime_binding_host_owned" }),
      status: () => {
        const ready = dispatcher !== undefined && !dispatcherClosed;
        return {
          status: "ok",
          agent: agentName,
          activation_ready: ready,
          ...(ready ? {} : { activation_state: "standby", activation_error: activationError ?? "activation dispatcher is not running",
            ...(activationErrorCode ? { activation_error_code: activationErrorCode } : {}) }),
          runtime: createdRuntime.binding,
        };
      },
    };
    server = createBoundMcpServer({ agentName, redisClient: redis, runtime: runtimeTools }, shutdown.signal);
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    client = new Client({ name: "gptqueue-opencode-host", version: VERSION });
    await client.connect(clientTransport);
    const retryMs = options.activationRetryMs ?? DEFAULT_ACTIVATION_RETRY_MS;
    // Activation is owned by exactly one process per session (the dispatcher's
    // lease). Losing that race, a lost lease or a Redis blip must not take the
    // session's tools down: stay in standby and retry until this backend closes.
    const superviseActivation = async (): Promise<void> => {
      retryTimer = undefined;
      if (stopping.signal.aborted) return;
      try {
        const started = await startInboxDispatcher(
          redis.adapterConnection, agentName, dispatcherRuntime, options.dispatcherOptions,
        );
        if (stopping.signal.aborted) { await started.close().catch(() => undefined); return; }
        dispatcher = started; dispatcherClosed = false; activationError = undefined; activationErrorCode = undefined;
        void started.closed.then(onDispatcherClosed, onDispatcherClosed);
      } catch (error) {
        activationError = error instanceof Error ? error.message : String(error);
        activationErrorCode = error instanceof GptQueueError ? error.code : undefined;
        scheduleActivation();
      }
    };
    const onDispatcherClosed = () => {
      dispatcherClosed = true;
      if (stopping.signal.aborted) return;
      activationError ??= "activation dispatcher stopped (runtime-binding lease lost)";
      activationErrorCode ??= "RUNTIME_BINDING_LOST";
      scheduleActivation();
    };
    const scheduleActivation = () => {
      if (stopping.signal.aborted || retryTimer) return;
      retryTimer = setTimeout(() => { void superviseActivation(); }, retryMs);
      retryTimer.unref?.();
    };
    await superviseActivation();

    const boundClient = client;
    return Object.freeze({
      callTool: (name: string, args: Readonly<Record<string, unknown>>, signal?: AbortSignal) =>
        boundClient.callTool({ name, arguments: { ...args } }, undefined, signal ? { signal } : undefined),
      close: () => {
        dispatcherClosed = true;
        stopping.abort();
        if (retryTimer) clearTimeout(retryTimer);
        // Dispatchers never close the shared adapter; the backend always does.
        closeResult ??= cleanup(redis, dispatcher, server, boundClient, sessionRegistered)
          .finally(async () => { await createdRuntime.close(); })
          .finally(() => shutdown.abort());
        return closeResult;
      },
    });
  } catch (error) {
    shutdown.abort();
    stopping.abort();
    if (retryTimer) clearTimeout(retryTimer);
    dispatcherClosed = true;
    const startupFailures: unknown[] = [];
    if (runtime) {
      try { await runtime.close(); } catch (runtimeError) { startupFailures.push(runtimeError); }
    }
    try { await cleanup(redis, dispatcher, server, client, sessionRegistered); }
    catch (cleanupError) { throw new AggregateError([error, cleanupError], "OpenCode backend startup failed"); }
    if (startupFailures.length > 0) throw new AggregateError([error, ...startupFailures], "OpenCode backend startup failed");
    throw error;
  }
};

/** Build the pool factory used by a host plugin; one native port is resolved per session. */
export const createOpenCodeBackendFactory = (
  options: OpenCodeBackendFactoryOptions,
): { factory: OpenCodeSessionBackendFactory; owner: OpenCodeSessionOwner } => {
  const factory: OpenCodeSessionBackendFactory = async (identity) => createOpenCodeBackend(identity, {
    redisUrl: options.redisUrl,
    runtimePort: await options.runtimePort(identity),
    epoch: typeof options.epoch === "function" ? options.epoch(identity) : options.epoch,
    runtimeOptions: options.runtimeOptions,
    dispatcherOptions: options.dispatcherOptions,
    activationRetryMs: options.activationRetryMs,
  });
  return Object.freeze({ factory, owner: new OpenCodeSessionOwner(factory) });
};
