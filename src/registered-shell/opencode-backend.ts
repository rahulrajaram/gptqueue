import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { isAbsolute } from "node:path";
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

const MAX_SESSION_ID_LENGTH = 200;

export type OpenCodeBackendOptions = Readonly<{
  redisUrl: string;
  runtimePort: OpenCodeRuntimePort;
  epoch?: string;
  runtimeOptions?: OpenCodeRuntimeOptions;
  dispatcherOptions?: Readonly<{ intervalMs?: number; maxAttempts?: number }>;
}>;

export type OpenCodeBackendFactoryOptions = Readonly<{
  redisUrl: string;
  runtimePort: (
    identity: OpenCodeSessionIdentity
  ) => OpenCodeRuntimePort | Promise<OpenCodeRuntimePort>;
  epoch?: string | ((identity: OpenCodeSessionIdentity) => string);
  runtimeOptions?: OpenCodeRuntimeOptions;
  dispatcherOptions?: Readonly<{ intervalMs?: number; maxAttempts?: number }>;
}>;

export const opencodeAgentName = (sessionID: string): string =>
  `gptqueue-opencode-${sessionID}`;

const validateIdentity = (identity: OpenCodeSessionIdentity): void => {
  if (
    identity.sessionID.trim().length === 0 ||
    identity.sessionID.length > MAX_SESSION_ID_LENGTH ||
    identity.directory.trim().length === 0 ||
    !isAbsolute(identity.directory)
  ) {
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
  const agentName = opencodeAgentName(identity.sessionID);
  const redis = new RedisClient(null, options.redisUrl);
  const shutdown = new AbortController();
  let dispatcher: InboxDispatcher | undefined;
  let server: ReturnType<typeof createBoundMcpServer> | undefined;
  let client: Client | undefined;
  let runtime: RuntimeAdapter | undefined;
  let sessionRegistered = false;
  let dispatcherClosed = false;
  let closeResult: Promise<void> | undefined;

  try {
    await redis.register(
      "both",
      agentName,
      `OpenCode native session ${identity.sessionID}`,
      { label: agentName, uuid: null, client: null, working_directory: identity.directory },
    );
    sessionRegistered = true;
    const binding = {
      client: "opencode" as const,
      runtime_id: identity.sessionID,
      epoch: typeof options.epoch === "string" ? options.epoch : identity.sessionID,
      working_directory: identity.directory,
    };
    const createdRuntime = await createOpenCodeRuntime(binding, options.runtimePort, options.runtimeOptions);
    runtime = createdRuntime;
    let runtimeTools: RuntimeTools;
    runtimeTools = {
      bind: async () => ({ status: "error", code: "runtime_binding_host_owned" }),
      status: () => ({
        status: "ok",
        agent: agentName,
        activation_ready: dispatcher !== undefined && !dispatcherClosed,
        runtime: createdRuntime.binding,
      }),
    };
    server = createBoundMcpServer({ agentName, redisClient: redis, runtime: runtimeTools }, shutdown.signal);
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    client = new Client({ name: "gptqueue-opencode-host", version: VERSION });
    await client.connect(clientTransport);
    dispatcher = await startInboxDispatcher(
      redis.adapterConnection,
      agentName,
      createdRuntime,
      options.dispatcherOptions,
    );
    void dispatcher.closed.then(() => { dispatcherClosed = true; }, () => { dispatcherClosed = true; });

    const boundClient = client;
    return Object.freeze({
      callTool: (name: string, args: Readonly<Record<string, unknown>>, signal?: AbortSignal) =>
        boundClient.callTool({ name, arguments: { ...args } }, undefined, signal ? { signal } : undefined),
      close: () => {
        dispatcherClosed = true;
        closeResult ??= cleanup(redis, dispatcher, server, boundClient, sessionRegistered)
          .finally(() => shutdown.abort());
        return closeResult;
      },
    });
  } catch (error) {
    shutdown.abort();
    dispatcherClosed = true;
    const startupFailures: unknown[] = [];
    if (!dispatcher && runtime) {
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
  });
  return Object.freeze({ factory, owner: new OpenCodeSessionOwner(factory) });
};
