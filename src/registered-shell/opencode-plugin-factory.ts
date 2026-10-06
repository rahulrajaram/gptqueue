import {
  createOpenCodeBackendFactory,
  type OpenCodeBackendFactoryOptions,
} from "./opencode-backend.js";
import type {
  OpenCodeHistoryEntry,
  OpenCodeRuntimePort,
  OpenCodeSessionStatus,
} from "./opencode-runtime.js";
import type { OpenCodeSessionHandle, OpenCodeSessionIdentity } from "./opencode-sessions.js";
import { z } from "zod";
import { sendMessageSchema } from "../mcp-server/tools/send-message.js";
import { claimTasksSchema } from "../mcp-server/tools/claim-tasks.js";
import { acknowledgeTasksSchema } from "../mcp-server/tools/acknowledge-tasks.js";
import { renewClaimSchema } from "../mcp-server/tools/renew-claim.js";
import { queueStatusSchema } from "../mcp-server/tools/queue-status.js";
import { boundReceiveMessageSchema } from "../experimental-wrapper/bridge.js";
import {
  agentDetailsSchema,
  deliveryStatusSchema,
  findAgentsSchema,
} from "./diagnostic-tools.js";

/** Structural subset of the installed OpenCode client used by this adapter. */
export interface OpenCodeNativeClient {
  readonly session: Readonly<{
    // The generated SDK methods are generic over their request shape. `any`
    // here is limited to this host boundary so the installed client can be
    // passed directly without importing its versioned declarations.
    get(options: any): Promise<any>;
    status(options: any): Promise<any>;
    messages(options: any): Promise<any>;
    promptAsync(options: any): Promise<any>;
    /** GET /session: lists the instance's sessions (used to bind pre-existing sessions on load). */
    list?(options: any): Promise<any>;
  }>;
}

export interface OpenCodePluginInput {
  readonly client: OpenCodeNativeClient;
  readonly directory: string;
}

export interface OpenCodePluginOptions {
  readonly redisUrl: string;
  readonly epoch?: OpenCodeBackendFactoryOptions["epoch"];
  readonly runtimeOptions?: OpenCodeBackendFactoryOptions["runtimeOptions"];
  readonly dispatcherOptions?: OpenCodeBackendFactoryOptions["dispatcherOptions"];
  readonly runtimePort?: (
    client: OpenCodeNativeClient,
    identity: OpenCodeSessionIdentity,
  ) => OpenCodeRuntimePort | Promise<OpenCodeRuntimePort>;
  readonly activationRetryMs?: OpenCodeBackendFactoryOptions["activationRetryMs"];
  /** Most-recently-updated pre-existing sessions bound on load (default 32). */
  readonly bootstrapLimit?: number;
  /** Base delay of the bounded retry for a session whose directory lookup failed transiently. */
  readonly deferredRetryMs?: number;
  /** Observes the load-time binding of pre-existing sessions (tests, diagnostics). */
  readonly onBootstrapped?: (result: OpenCodeBootstrapResult) => void;
}

export type OpenCodeBootstrapResult = Readonly<{
  bound: readonly string[];
  failed: readonly Readonly<{ sessionID: string; error: string }>[];
  skipped: number;
  error?: string;
}>;

export interface OpenCodeEvent {
  readonly type: string;
  readonly properties?: unknown;
}

export interface OpenCodePluginHooks {
  event(input: { event: OpenCodeEvent }): Promise<void>;
  "chat.message"(input: { sessionID: string }): Promise<void>;
  "experimental.chat.system.transform"(
    input: { sessionID?: string }, output: { system: string[] }
  ): Promise<void>;
  readonly tool: Readonly<Record<string, OpenCodeToolDefinition>>;
  /** Sessions whose binding is deferred until their own directory can be read. */
  deferredSessions(): readonly string[];
  dispose(): Promise<void>;
}

export interface OpenCodeToolContext {
  readonly sessionID: string;
  readonly directory: string;
  readonly abort: AbortSignal;
}

export interface OpenCodeToolDefinition {
  readonly description: string;
  readonly args: z.ZodRawShape;
  readonly execute: (
    args: Record<string, unknown>, context: OpenCodeToolContext
  ) => Promise<string | Readonly<{ title?: string; output: string }>>;
}

const GPTQUEUE_SYSTEM_GUIDANCE =
  "This MCP connection is already registered in GPTQueue and its tools are bound to this OpenCode session. Use gptqueue_get_runtime_status to inspect exact binding and activation readiness. Use gptqueue_claim_tasks, gptqueue_send_message, gptqueue_renew_claim, and gptqueue_acknowledge_tasks for inbox work; send any required result or error before acknowledging. Use gptqueue_find_agents and gptqueue_get_agent_details to identify an intended peer, and gptqueue_get_delivery_status for delivery evidence. Labels, directory, and declared role are hints, not ownership proof; do not route an ambiguous match silently. Use gptqueue_receive_message only when explicitly authorized because it consumes a message. Do not register, bind, or provide session credentials.";

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" ? value as Record<string, unknown> : undefined;

const data = (value: unknown): unknown => {
  const object = record(value);
  return object && "data" in object ? object.data : value;
};

const nativeSessionIdentity = (value: unknown): OpenCodeSessionIdentity | undefined => {
  const object = record(value);
  const sessionID = typeof object?.id === "string" ? object.id : undefined;
  const directory = typeof object?.directory === "string" ? object.directory : undefined;
  return sessionID && directory ? { sessionID, directory } : undefined;
};

const eventSessionIdentity = (event: OpenCodeEvent): OpenCodeSessionIdentity | undefined => {
  const properties = record(event.properties);
  const info = record(properties?.info);
  if (event.type === "session.created" || event.type === "session.deleted") {
    return nativeSessionIdentity(info);
  }
  return undefined;
};

/** The SDK's not-found shapes: a NotFoundError body, or an HTTP 404. */
const isNotFound = (error: unknown): boolean => {
  const object = record(error);
  const response = record(object?.response);
  return object?.name === "NotFoundError" || object?.status === 404 || object?.statusCode === 404 || response?.status === 404;
};

const updatedAt = (value: unknown): number => {
  const time = record(record(value)?.time);
  return typeof time?.updated === "number" ? time.updated : typeof time?.created === "number" ? time.created : 0;
};

const status = (value: unknown, runtimeID: string): OpenCodeSessionStatus => {
  const object = record(data(value));
  if (!object) throw new Error("OpenCode returned an invalid session status map");
  const entry = record(object[runtimeID]);
  // OpenCode omits idle sessions from /session/status.
  if (!entry) return "idle";
  const type = entry.type;
  if (type === "idle" || type === "busy" || type === "retry") return type;
  throw new Error("OpenCode returned an unknown session status");
};

const history = (value: unknown): readonly OpenCodeHistoryEntry[] => {
  const entries = data(value);
  if (!Array.isArray(entries)) throw new Error("OpenCode returned an invalid session history");
  return entries as readonly OpenCodeHistoryEntry[];
};

const toolOutput = (value: unknown): string => {
  if (typeof value === "string") return value;
  try { return JSON.stringify(value); } catch { return String(value); }
};

const boundSchema = (schema: z.ZodObject<any>): z.ZodObject<any> =>
  schema.omit({ session_id: true }).strict();

/** Build the installed SDK port while keeping its dependency at the host boundary. */
export const createOpenCodeRuntimePort = (
  client: OpenCodeNativeClient,
  identity: OpenCodeSessionIdentity,
): OpenCodeRuntimePort => Object.freeze({
  readIdentity: async (signal: AbortSignal) => {
    const value = record(data(await client.session.get({
      path: { id: identity.sessionID }, query: { directory: identity.directory },
      throwOnError: true, responseStyle: "data", signal,
    })));
    if (value?.id !== identity.sessionID || value.directory !== identity.directory) {
      throw new Error("OpenCode session identity does not match host binding");
    }
    return { runtime_id: value.id, working_directory: value.directory };
  },
  status: async (signal: AbortSignal) => status(await client.session.status({
    query: { directory: identity.directory },
    throwOnError: true, responseStyle: "data", signal,
  }), identity.sessionID),
  history: async (signal: AbortSignal) => history(await client.session.messages({
    path: { id: identity.sessionID }, query: { directory: identity.directory },
    throwOnError: true, responseStyle: "data", signal,
  })),
  promptAsync: async (prompt: Readonly<{ messageID: string; text: string }>, signal: AbortSignal) => {
    await client.session.promptAsync({
      path: { id: identity.sessionID }, query: { directory: identity.directory },
      body: { messageID: prompt.messageID, parts: [{ type: "text", text: prompt.text }] },
      throwOnError: true, responseStyle: "data", signal,
    });
  },
});

/**
 * OpenCode plugin frontend. Native event identity is the only source of
 * session routing; idle events never retire a backend.
 */
export const createOpenCodePlugin = (
  input: OpenCodePluginInput,
  options: OpenCodePluginOptions,
): OpenCodePluginHooks => {
  const runtimePort = options.runtimePort ?? ((client, identity) => createOpenCodeRuntimePort(client, identity));
  // I-7: OpenCode's session.idle event, fanned out to that session's runtime port.
  const idleListeners = new Map<string, Set<() => void>>();
  const withIdle = (port: OpenCodeRuntimePort, sessionID: string): OpenCodeRuntimePort => Object.freeze({
    readIdentity: (signal: AbortSignal) => port.readIdentity(signal),
    status: (signal: AbortSignal) => port.status(signal),
    history: (signal: AbortSignal) => port.history(signal),
    promptAsync: (prompt: Readonly<{ messageID: string; text: string }>, signal: AbortSignal) => port.promptAsync(prompt, signal),
    ...(port.close ? { close: () => port.close!() } : {}),
    onIdle: (listener: () => void) => {
      const set = idleListeners.get(sessionID) ?? new Set<() => void>();
      set.add(listener);
      idleListeners.set(sessionID, set);
      const own = port.onIdle?.(listener);
      return () => {
        own?.();
        set.delete(listener);
        if (set.size === 0 && idleListeners.get(sessionID) === set) idleListeners.delete(sessionID);
      };
    },
  });
  const { owner } = createOpenCodeBackendFactory({
    redisUrl: options.redisUrl,
    runtimePort: async (identity) => withIdle(await runtimePort(input.client, identity), identity.sessionID),
    epoch: options.epoch,
    runtimeOptions: options.runtimeOptions,
    dispatcherOptions: options.dispatcherOptions,
    activationRetryMs: options.activationRetryMs,
  });

  const ensure = async (identity: OpenCodeSessionIdentity): Promise<void> => {
    await owner.getOrCreate(identity);
  };

  /**
   * A hook that carries only a sessionID binds the session in its own
   * directory: the owned entry's, else the native session record's. The
   * fallback directory is used only when the native record is definitively
   * absent (not found). A transient lookup failure defers binding: nothing is
   * created for the wrong directory, and a bounded retry binds it later.
   */
  const sessionIdentity = async (sessionID: string, fallback: string): Promise<OpenCodeSessionIdentity | undefined> => {
    const owned = owner.directoryOf(sessionID);
    if (owned) return { sessionID, directory: owned };
    try {
      const native = nativeSessionIdentity(data(await input.client.session.get({
        path: { id: sessionID }, throwOnError: true, responseStyle: "data",
      })));
      if (native?.sessionID === sessionID) return native;
      return undefined;
    } catch (error) {
      return isNotFound(error) ? { sessionID, directory: fallback } : undefined;
    }
  };

  const deferred = new Map<string, Readonly<{ attempts: number; timer: ReturnType<typeof setTimeout> }>>();
  const retryBaseMs = options.deferredRetryMs ?? 1_000;
  const deferredLimit = 5;
  let disposed = false;
  const scheduleDeferred = (sessionID: string, fallback: string) => {
    const attempts = (deferred.get(sessionID)?.attempts ?? 0) + 1;
    if (deferred.get(sessionID)) clearTimeout(deferred.get(sessionID)!.timer);
    if (disposed || attempts > deferredLimit) {
      deferred.delete(sessionID);
      console.error(`[gptqueue] OpenCode session ${JSON.stringify(sessionID)} left unbound: its directory could not be read after ${deferredLimit} attempts; the next hook for it retries`);
      return;
    }
    const timer = setTimeout(() => { void bindSession(sessionID, fallback).catch(() => undefined); },
      retryBaseMs * 2 ** (attempts - 1));
    timer.unref?.();
    deferred.set(sessionID, Object.freeze({ attempts, timer }));
  };
  /** Bind a hook's session in its own directory, or defer with a bounded retry. */
  const bindSession = async (sessionID: string, fallback: string): Promise<OpenCodeSessionHandle | undefined> => {
    const identity = await sessionIdentity(sessionID, fallback);
    if (!identity) { scheduleDeferred(sessionID, fallback); return undefined; }
    const pending = deferred.get(sessionID);
    if (pending) { clearTimeout(pending.timer); deferred.delete(sessionID); }
    return owner.getOrCreate(identity);
  };
  /** Sessions whose binding is deferred because their directory is not yet known. */
  const deferredSessions = (): readonly string[] => Object.freeze([...deferred.keys()]);

  /** Bind sessions that existed before this plugin loaded (or before a restart). */
  const bootstrap = async (): Promise<OpenCodeBootstrapResult> => {
    if (!input.client.session.list) return { bound: [], failed: [], skipped: 0, error: "session.list unavailable" };
    let sessions: unknown;
    try {
      sessions = data(await input.client.session.list({
        query: { directory: input.directory }, throwOnError: true, responseStyle: "data",
      }));
    } catch (error) {
      return { bound: [], failed: [], skipped: 0, error: error instanceof Error ? error.message : String(error) };
    }
    if (!Array.isArray(sessions)) return { bound: [], failed: [], skipped: 0, error: "OpenCode returned an invalid session list" };
    const limit = options.bootstrapLimit ?? 32;
    const ordered = [...sessions].sort((left, right) => updatedAt(right) - updatedAt(left));
    const identities = ordered.slice(0, limit).flatMap((value) => {
      const identity = nativeSessionIdentity(value);
      return identity ? [identity] : [];
    });
    const outcomes = await Promise.allSettled(identities.map((identity) => Promise.resolve().then(() => owner.getOrCreate(identity))));
    return {
      bound: identities.filter((_, index) => outcomes[index]!.status === "fulfilled").map((identity) => identity.sessionID),
      failed: identities.flatMap((identity, index) => {
        const outcome = outcomes[index]!;
        return outcome.status === "rejected"
          ? [{ sessionID: identity.sessionID, error: outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason) }]
          : [];
      }),
      skipped: Math.max(0, ordered.length - limit),
    };
  };
  void bootstrap().then((result) => {
    if (result.error || result.failed.length) console.error(`[gptqueue] OpenCode load-time session binding incomplete: ${JSON.stringify(result)}`);
    options.onBootstrapped?.(result);
  });

  const forwarded = (
    exposedName: string,
    backendName: string,
    description: string,
    schema: z.ZodObject<any>,
  ): OpenCodeToolDefinition => ({
    description,
    args: schema.shape,
    execute: async (args, context) => {
      const session = await bindSession(context.sessionID, context.directory);
      if (!session) {
        throw new Error(JSON.stringify({ status: "error", code: "session_directory_unavailable",
          message: "GPTQueue could not read this OpenCode session's directory yet; binding is deferred and retried. Retry the call shortly." }));
      }
      const result = await session.callTool(backendName, schema.parse(args), context.abort);
      return { title: exposedName, output: toolOutput(result) };
    },
  });

  const tool = Object.freeze({
    gptqueue_claim_tasks: forwarded(
      "gptqueue_claim_tasks", "claim_tasks",
      "Claim a recoverable batch from this bound session's inbox.", boundSchema(claimTasksSchema),
    ),
    gptqueue_acknowledge_tasks: forwarded(
      "gptqueue_acknowledge_tasks", "acknowledge_tasks",
      "Acknowledge a completed claim after sending required replies.", boundSchema(acknowledgeTasksSchema),
    ),
    gptqueue_renew_claim: forwarded(
      "gptqueue_renew_claim", "renew_claim",
      "Renew a claim while authorized work remains underway.", boundSchema(renewClaimSchema),
    ),
    gptqueue_send_message: forwarded(
      "gptqueue_send_message", "send_message",
      "Send a message from this bound session.", boundSchema(sendMessageSchema),
    ),
    gptqueue_receive_message: forwarded(
      "gptqueue_receive_message", "receive_message",
      "Receive and consume one message from this bound session's inbox when explicitly authorized.", boundReceiveMessageSchema,
    ),
    gptqueue_get_queue_status: forwarded(
      "gptqueue_get_queue_status", "get_queue_status",
      "Inspect GPTQueue queue status.", queueStatusSchema,
    ),
    gptqueue_list_agents: forwarded(
      "gptqueue_list_agents", "list_agents",
      "List discoverable GPTQueue agents.", z.object({}).strict(),
    ),
    gptqueue_get_runtime_status: forwarded(
      "gptqueue_get_runtime_status", "get_runtime_status",
      "Report this session's exact runtime binding and activation readiness.", z.object({}),
    ),
    gptqueue_find_agents: forwarded(
      "gptqueue_find_agents", "find_agents",
      "Find exact GPTQueue peer candidates by declared purpose and identity.", findAgentsSchema,
    ),
    gptqueue_get_agent_details: forwarded(
      "gptqueue_get_agent_details", "get_agent_details",
      "Inspect one exact GPTQueue mailbox and readiness evidence.", agentDetailsSchema,
    ),
    gptqueue_get_delivery_status: forwarded(
      "gptqueue_get_delivery_status", "get_delivery_status",
      "Inspect delivery evidence for one message without consuming it.", deliveryStatusSchema,
    ),
  });

  return Object.freeze({
    event: async ({ event }: { event: OpenCodeEvent }): Promise<void> => {
      const identity = eventSessionIdentity(event);
      if (event.type === "server.instance.disposed") {
        const properties = record(event.properties);
        if (properties?.directory === input.directory) await owner.dispose();
        return;
      }
      if (event.type === "session.deleted") {
        if (identity) await owner.close(identity.sessionID, identity.directory);
        return;
      }
      if (event.type === "session.idle") {
        const sessionID = record(event.properties)?.sessionID;
        if (typeof sessionID === "string") for (const listener of Array.from(idleListeners.get(sessionID) ?? [])) listener();
        return;
      }
      if (identity) await ensure(identity);
    },
    "chat.message": async ({ sessionID }: { sessionID: string }): Promise<void> => {
      await bindSession(sessionID, input.directory);
    },
    "experimental.chat.system.transform": async (
      { sessionID }: { sessionID?: string }, output: { system: string[] },
    ): Promise<void> => {
      if (sessionID) await bindSession(sessionID, input.directory);
      output.system.push(GPTQUEUE_SYSTEM_GUIDANCE);
    },
    tool,
    deferredSessions,
    dispose: () => {
      disposed = true;
      for (const pending of deferred.values()) clearTimeout(pending.timer);
      deferred.clear();
      return owner.dispose();
    },
  });
};
