import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { isAbsolute } from "node:path";
import { createPiRuntime, type PiRuntimeHost } from "./pi-runtime.js";
import type { ActivationRequest, ActivationOutcome, RuntimeBinding } from "./runtime.js";
import { inboxNoToolPrompt, inboxReceivePrompt, runtimeBindingSchema } from "./runtime.js";
import { z } from "zod";
import { VERSION } from "../version.js";
import { RUNTIME_TOOL_NAMES, SHELL_TOOL_NAMES } from "./tool-names.js";

export const GPTQUEUE_TOOLS = ["send_message", "receive_message", "list_agents", "get_queue_status"] as const;
/** Every registered-shell tool; the runtime subset is required. */
export const SHELL_TOOLS = SHELL_TOOL_NAMES;
const REQUIRED_RUNTIME_TOOLS = RUNTIME_TOOL_NAMES;
/** Tools a wake prompt may tell the model to consume mail with, best first. */
const CLAIM_PATH = ["claim_tasks", "acknowledge_tasks"] as const;
const RECEIVE_PATH = ["receive_message"] as const;
const hasAll = (active: readonly string[], names: readonly string[]) => names.every((name) => active.includes(name));

/**
 * The wake prompt must only name tools this Pi session can call. A host tool
 * allowlist (`pi --tools ...`) removes excluded extension tools from Pi's
 * registry, so the dispatcher's claim prompt is replaced when they are absent.
 */
export const piActivationPrompt = (request: Readonly<{ operation_id: string; prompt: string }>, active: readonly string[]): string =>
  hasAll(active, CLAIM_PATH) ? request.prompt
    : hasAll(active, RECEIVE_PATH) ? inboxReceivePrompt(request.operation_id)
      : inboxNoToolPrompt(request.operation_id);
const STARTUP_TIMEOUT_MS = 10_000;
type CatalogTool = { name?: string; description?: string; inputSchema?: unknown };
type BoundTool = { name: string; description?: string; inputSchema: Record<string, unknown> };
type ToolDefinition = {
  name: string; label: string; description: string; parameters: Record<string, unknown>;
  execute(id: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<{
    content: Array<{ type: "text"; text: string }>; details: CallToolResult;
  }>;
};
/** Structural boundary: Pi is the host, not a dependency installed by GPTQueue. */
export type PiContext = ReturnType<PiRuntimeHost["getContext"]>;
export interface PiAPI {
  registerTool(definition: ToolDefinition): void;
  on(event: "before_agent_start", handler: (event: { systemPrompt: string }) => Promise<{ systemPrompt: string }>): void;
  on(event: "session_shutdown", handler: () => Promise<void>): void;
  on(event: "session_start" | "session_before_switch", handler: (event: unknown, context: PiContext) => Promise<void> | void): void;
  on(event: "agent_end", handler: (event: { messages?: readonly unknown[] }, context: PiContext) => Promise<void> | void): void;
  appendEntry?(customType: string, data: unknown): void;
  getActiveTools(): string[];
  setActiveTools(names: string[]): void;
  sendMessage?(payload: unknown, options: { deliverAs: "followUp"; triggerTurn: true }): void;
}
export interface SessionClient {
  listTools(params?: undefined, options?: { signal?: AbortSignal }): Promise<{ tools?: CatalogTool[] }>;
  callTool(args: { name: string; arguments?: Record<string, unknown> }, schema?: undefined,
    options?: { signal?: AbortSignal }): Promise<CallToolResult>;
  getInstructions?(): string | undefined;
  close(): Promise<void>;
  /** Forcefully end the connection when a graceful close() hangs. */
  destroy?(): void;
  setActivationHandler?(handler: (binding: RuntimeBinding, request: ActivationRequest, signal: AbortSignal) => Promise<ActivationOutcome>): void;
}

const withTimeout = async <T>(work: (signal: AbortSignal) => Promise<T>, timeout: number): Promise<T> => {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([Promise.resolve().then(() => work(controller.signal)), new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new Error(`GPTQueue startup timed out after ${timeout}ms`));
        controller.abort();
      }, timeout);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};

export const validateCatalog = (catalog: { tools?: CatalogTool[] }, runtimeEnabled = false): readonly BoundTool[] => {
  const expected: readonly string[] = runtimeEnabled ? [...GPTQUEUE_TOOLS, ...REQUIRED_RUNTIME_TOOLS] : GPTQUEUE_TOOLS;
  const allowed: readonly string[] = runtimeEnabled ? [...GPTQUEUE_TOOLS, ...SHELL_TOOLS] : GPTQUEUE_TOOLS;
  const tools = catalog.tools ?? [];
  const names = tools.map((tool) => tool.name);
  if (new Set(names).size !== names.length || names.some(name => !name || !allowed.includes(name)) || expected.some((name) => !names.includes(name))) {
    throw new Error("GPTQueue tool catalog mismatch");
  }
  return allowed.filter(name => names.includes(name)).map((name) => {
    const tool = tools.find((candidate) => candidate.name === name)!;
    const schema = tool.inputSchema;
    if (!schema || typeof schema !== "object" || Array.isArray(schema) ||
        !("type" in schema) || schema.type !== "object") throw new Error(`Invalid schema for ${name}`);
    const properties = "properties" in schema ? schema.properties : undefined;
    if (properties && typeof properties === "object" && "session_id" in properties) {
      throw new Error(`session_id is forbidden in ${name}`);
    }
    return Object.freeze({ name, description: tool.description, inputSchema: schema as Record<string, unknown> });
  });
};

/** Typed tool error while the extension is degraded; Pi keeps running. */
export type PiGptqueueUnavailableCode =
  "gptqueue_unavailable" | "runtime_binding_refused" | "runtime_binding_failed" | "runtime_unsupported";
export class PiGptqueueUnavailableError extends Error {
  constructor(readonly code: PiGptqueueUnavailableCode, readonly reason: string) {
    super(JSON.stringify({ status: "error", code, message: `GPTQueue is unavailable in this Pi session: ${reason}` }));
    this.name = "PiGptqueueUnavailableError";
  }
}
/** Pi's bind_runtime refusal: another live process owns this session's mailbox. Not transient by itself, but retried. */
class BindingRefusedError extends Error {}
/** Any other typed bind_runtime failure; the GPTQueue code is kept in the reason. */
class BindingFailedError extends Error {}
/** Missing host identity or API: retrying cannot help. */
class UnsupportedHostError extends Error {}

export type PiRetryOptions = Readonly<{ baseMs?: number; maxMs?: number }>;
const STATUS_KEY = "gptqueue";
const describe = (error: unknown): string => error instanceof Error ? error.message : String(error);
const classify = (error: unknown): PiGptqueueUnavailableCode =>
  error instanceof UnsupportedHostError ? "runtime_unsupported"
    : error instanceof BindingRefusedError ? "runtime_binding_refused"
      : error instanceof BindingFailedError ? "runtime_binding_failed" : "gptqueue_unavailable";
/** Exponential backoff, capped: the delay is bounded, the retry is not abandoned. */
export const retryDelay = (attempt: number, options: PiRetryOptions = {}): number =>
  Math.min((options.baseMs ?? 1_000) * 2 ** Math.min(attempt, 20), options.maxMs ?? 30_000);

/** The typed GPTQueue code of a failed bind_runtime (stableToolError's structuredContent.error.code). */
const bindErrorCode = (structured: unknown): string | undefined => {
  const error = structured && typeof structured === "object" ? (structured as Record<string, unknown>).error : undefined;
  const code = error && typeof error === "object" ? (error as Record<string, unknown>).code : undefined;
  return typeof code === "string" ? code : undefined;
};
const bindError = (code: string | undefined, text: string): Error => {
  const detail = text ? `: ${text}` : "";
  switch (code) {
    case "RUNTIME_BINDING_HELD": return new BindingRefusedError(`Pi inbox activation binding refused${detail}`);
    case "REDIS_UNAVAILABLE": return new Error(`GPTQueue Redis unavailable during Pi inbox activation binding${detail}`);
    default: return new BindingFailedError(`Pi inbox activation binding failed (${code ?? "UNKNOWN"})${detail}`);
  }
};

export const createPiExtension = (
  makeClient: (signal: AbortSignal, context?: PiContext) => Promise<SessionClient>, timeout = STARTUP_TIMEOUT_MS,
  options: { readonly runtimeEnabled?: boolean; readonly retry?: PiRetryOptions } = {}
) => async (pi: PiAPI): Promise<void> => {
  let client: SessionClient | undefined;
  let runtime: ReturnType<typeof createPiRuntime> | undefined;
  let binding: RuntimeBinding | undefined;
  let context: PiContext | undefined;
  let instructions = "";
  // Degraded state: why GPTQueue is unavailable, and the pending reconnect.
  let degraded: PiGptqueueUnavailableError | undefined;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let retryAttempt = 0;
  let generation = 0;
  // Aborted on every generation bump: cancels in-flight attempts of the old generation.
  let lifecycle = new AbortController();
  let connecting: { readonly life: AbortSignal; readonly promise: Promise<SessionClient> } | undefined;
  let starting: { readonly life: AbortSignal; readonly promise: Promise<void> } | undefined;
  // The latest bind_runtime call, settled-or-not. A new generation's bind
  // waits for it, so a superseded bind can never land after a newer one.
  let bindInFlight: Promise<unknown> | undefined;
  const supersede = () => { generation += 1; lifecycle.abort(); lifecycle = new AbortController(); };
  const superseded = () => new Error("GPTQueue connection attempt superseded");
  const registered = new Set<string>();
  const expected: readonly string[] = options.runtimeEnabled ? [...GPTQUEUE_TOOLS, ...REQUIRED_RUNTIME_TOOLS] : GPTQUEUE_TOOLS;
  const cancelRetry = () => { if (retryTimer) clearTimeout(retryTimer); retryTimer = undefined; };
  const close = async () => {
    runtime?.invalidate(); runtime = undefined; binding = undefined;
    const old = client; client = undefined;
    if (!old) return;
    try {
      await withTimeout(() => old.close(), 2_500);
    } catch (error) {
      // A wedged close must not leave the sidecar process running.
      old.destroy?.();
      throw error;
    }
  };
  const status = (text: string | undefined, level?: "info" | "warning" | "error") => {
    const ui = context?.ui;
    try {
      ui?.setStatus?.(STATUS_KEY, text);
      if (text && level && ui?.notify) ui.notify(text, level);
    } catch { /* UI is best effort; never let it take Pi down */ }
  };
  const recovered = () => {
    cancelRetry(); retryAttempt = 0;
    if (!degraded) return;
    degraded = undefined;
    status(undefined);
    try { context?.ui?.notify?.("GPTQueue reconnected", "info"); } catch { /* best effort */ }
  };
  /** Never exit Pi: close the sidecar, tell the user, and (unless hopeless) reconnect later. */
  const degrade = async (error: unknown): Promise<void> => {
    const code = classify(error);
    const reason = describe(error);
    degraded = new PiGptqueueUnavailableError(code, reason);
    console.error(`[gptqueue] Pi registration degraded (${code}): ${reason}`);
    await close().catch(() => undefined);
    const text = code === "runtime_binding_refused"
      ? `GPTQueue refused this Pi session's mailbox binding (another live process may own it); Pi continues without GPTQueue and retries: ${reason}`
      : `GPTQueue unavailable (${code}): ${reason}`;
    status(text, code === "runtime_binding_refused" ? "warning" : "error");
    if (code !== "runtime_unsupported") scheduleRetry();
  };
  const scheduleRetry = () => {
    cancelRetry();
    const mine = generation;
    const timer = setTimeout(() => {
      retryTimer = undefined;
      if (mine !== generation) return;
      retryAttempt += 1;
      void attempt().then(() => { if (mine === generation) recovered(); },
        (error: unknown) => mine === generation ? degrade(error) : undefined);
    }, retryDelay(retryAttempt, options.retry));
    timer.unref?.();
    retryTimer = timer;
  };
  /** One sidecar creation per generation; a stale one closes what it created. */
  const createClient = (life: AbortSignal): Promise<SessionClient> => {
    if (connecting?.life === life) return connecting.promise;
    const promise = withTimeout(async (timeoutSignal) => {
      const signal = AbortSignal.any([timeoutSignal, life]);
      const connected = await makeClient(signal, context);
      if (signal.aborted) {
        await withTimeout(() => connected.close(), 2_500).catch(() => connected.destroy?.());
        throw life.aborted ? superseded() : new Error("GPTQueue startup aborted");
      }
      return connected;
    }, timeout).finally(() => { if (connecting?.promise === promise) connecting = undefined; });
    connecting = { life, promise };
    return promise;
  };
  const connect = async (life: AbortSignal = lifecycle.signal) => {
    if (!client) {
      const created = await createClient(life);
      if (life.aborted) {
        await withTimeout(() => created.close(), 2_500).catch(() => created.destroy?.());
        throw superseded();
      }
      client ??= created;
    }
    const catalog = validateCatalog(await withTimeout((signal) => client!.listTools(undefined, { signal }), timeout), options.runtimeEnabled);
    instructions = client.getInstructions?.() ?? "";
    if (!instructions.trim()) throw new Error("GPTQueue server instructions are missing or empty");
    return catalog;
  };
  const registerCatalog = (catalog: readonly BoundTool[]) => {
    for (const definition of catalog) {
      if (registered.has(definition.name)) continue;
      registered.add(definition.name);
      pi.registerTool({
        name: definition.name, label: `GPTQueue ${definition.name}`,
        description: definition.description ?? `GPTQueue ${definition.name}`, parameters: definition.inputSchema,
        async execute(_id, params, signal) {
          if (degraded) throw degraded;
          if (!client) throw new PiGptqueueUnavailableError("gptqueue_unavailable", "Pi GPTQueue session is not connected");
          const result = await client.callTool({ name: definition.name, arguments: params }, undefined, { signal });
          const content = result.content.filter((item): item is { type: "text"; text: string } => item.type === "text");
          if (result.isError) throw new Error(content.map((item) => item.text).join("\n") || "GPTQueue tool failed");
          return { content, details: result };
        },
      });
    }
  };
  /** Serialised per generation: concurrent starts share one in-flight attempt. */
  const startRuntime = (current: PiContext): Promise<void> => {
    const life = lifecycle.signal;
    if (starting?.life === life) return starting.promise;
    const promise = startRuntimeOnce(current, life)
      .finally(() => { if (starting?.promise === promise) starting = undefined; });
    starting = { life, promise };
    return promise;
  };
  const startRuntimeOnce = async (current: PiContext, life: AbortSignal) => {
    context = current;
    if (!options.runtimeEnabled) return;
    if (!current.cwd || !isAbsolute(current.cwd) || !current.sessionManager.getSessionId() || !pi.sendMessage) throw new UnsupportedHostError("Pi session identity or native message API unavailable");
    if (binding?.runtime_id === current.sessionManager.getSessionId() && runtime && client) return;
    if (binding) await close();
    const catalog = await connect(life);
    if (life.aborted) throw superseded();
    registerCatalog(catalog);
    const session = client!;
    if (!session.setActivationHandler) throw new UnsupportedHostError("Pi activation request transport unavailable");
    binding = Object.freeze({ client: "pi", runtime_id: current.sessionManager.getSessionId(), epoch: randomUUID(), working_directory: current.cwd });
    const ownBinding = binding;
    const adapter = createPiRuntime(binding, {
      getContext: () => context!, getEpoch: () => binding?.epoch ?? "",
      sendMessage: (payload, delivery) => pi.sendMessage!(payload, delivery),
    });
    runtime = adapter;
    session.setActivationHandler(async (incoming, request, signal) => {
      if (incoming.runtime_id !== ownBinding.runtime_id || incoming.epoch !== ownBinding.epoch ||
          incoming.client !== "pi" || incoming.working_directory !== ownBinding.working_directory || runtime !== adapter) {
        return { status: "unavailable" };
      }
      return adapter.activate({ ...request, prompt: piActivationPrompt(request, pi.getActiveTools()) }, signal);
    });
    // Serialize binds across generations: the previous one was aborted (its
    // lifecycle signal) and its client closed by close(); wait until it settles.
    if (bindInFlight) await bindInFlight;
    if (life.aborted) throw superseded();
    // The lifecycle signal aborts this bind as soon as a newer generation starts.
    const call = withTimeout((signal) => session.callTool({ name: "bind_runtime", arguments: { ...binding } }, undefined, { signal: AbortSignal.any([signal, life]) }), timeout);
    const settled = call.then(() => undefined, () => undefined);
    bindInFlight = settled;
    const result = await call.finally(() => { if (bindInFlight === settled) bindInFlight = undefined; });
    if (life.aborted) throw superseded();
    if (result.isError) {
      const text = result.content.filter((item) => item.type === "text").map((item) => (item as { text: string }).text).join("\n");
      throw bindError(bindErrorCode(result.structuredContent), text);
    }
    if (result.structuredContent?.activation_ready !== true) throw new Error("Pi inbox activation binding failed");
  };
  /** One (re)connection attempt for the current mode. */
  const attempt = async () => {
    if (options.runtimeEnabled) {
      if (!context) throw new Error("Pi session_start has not supplied runtime identity");
      await startRuntime(context);
    } else {
      registerCatalog(await connect());
    }
  };
  const activateTools = () => {
    const active = pi.getActiveTools();
    const missing = expected.filter((name) => !active.includes(name));
    if (missing.length) pi.setActiveTools([...active, ...missing]);
    const now = pi.getActiveTools();
    if (expected.every((name) => now.includes(name))) return;
    // A host tool allowlist can withhold some GPTQueue tools; Pi then cannot
    // activate them. A bound session that can still consume mail stays
    // connected (degrading would close the sidecar and its session identity);
    // activations adapt their prompt to the tools that are active.
    if (options.runtimeEnabled && (hasAll(now, CLAIM_PATH) || hasAll(now, RECEIVE_PATH))) return;
    throw new Error("GPTQueue tools are not active");
  };
  const unavailableNote = () =>
    `GPTQueue is currently unavailable in this session (${degraded?.code}); GPTQueue tools will report errors until it reconnects. Continue the user's work normally.`;
  pi.on("before_agent_start", async (event) => {
    if (degraded) return { systemPrompt: `${event.systemPrompt}\n\n${unavailableNote()}` };
    try {
      if (options.runtimeEnabled) {
        if (!context) throw new Error("Pi session_start has not supplied runtime identity");
        if (!runtime) await startRuntime(context);
      }
      await connect();
      activateTools();
      recovered();
      return { systemPrompt: `${event.systemPrompt}\n\n${instructions}` };
    } catch (error) {
      await degrade(error);
      return { systemPrompt: `${event.systemPrompt}\n\n${unavailableNote()}` };
    }
  });
  if (options.runtimeEnabled) {
    pi.on("session_start", async (_event, current) => {
      supersede(); cancelRetry(); retryAttempt = 0; degraded = undefined;
      context = current;
      const mine = generation;
      // A superseded start must not degrade (and close) the newer session's client.
      try { await startRuntime(current); if (mine === generation) recovered(); }
      catch (error) { if (mine === generation) await degrade(error); }
    });
    pi.on("session_before_switch", async () => { supersede(); cancelRetry(); await close(); });
    pi.on("agent_end", (event) => {
      for (const item of event.messages ?? []) {
        if (!item || typeof item !== "object") continue;
        const message = item as Record<string, unknown>;
        const details = message.details as Record<string, unknown> | undefined;
        // The completion marker only says the turn ended; the dispatcher
        // re-reads pending work and re-arms a fresh attempt for anything unclaimed.
        if (message.role === "custom" && message.customType === "gptqueue-inbox-activation" &&
            details?.runtime_id === binding?.runtime_id && typeof details?.operation_id === "string") {
          pi.appendEntry?.("gptqueue-inbox-activation-completed", { operation_id: details.operation_id,
            runtime_id: binding!.runtime_id, turn_id: `pi:${details.operation_id}`, status: "completed" });
        }
      }
    });
  }
  pi.on("session_shutdown", async () => { supersede(); cancelRetry(); await close(); });
  if (!options.runtimeEnabled) {
    try { registerCatalog(await connect()); } catch (error) { await degrade(error); }
  }
};

export const createRegisteredPiExtension = (options: {
  readonly redisUrl: string; readonly nodePath?: string; readonly sidecarPath?: string;
}) => createPiExtension(async (signal, context) => {
  if (!options.redisUrl) throw new Error("An explicit GPTQueue Redis URL is required");
  if (!context || typeof context.cwd !== "string" || !isAbsolute(context.cwd)) throw new Error("An absolute Pi host cwd is required");
  const transport = new StdioClientTransport({
    command: options.nodePath ?? process.execPath,
    cwd: context.cwd,
    args: [options.sidecarPath ?? fileURLToPath(new URL("../../bin/gptqueue-session", import.meta.url)),
      "--client", "pi", "--redis-url", options.redisUrl],
  });
  const client = new Client({ name: "gptqueue-pi", version: VERSION });
  let activationHandler: ((binding: RuntimeBinding, request: ActivationRequest, signal: AbortSignal) => Promise<ActivationOutcome>) | undefined;
  client.setRequestHandler(z.object({
    method: z.literal("gptqueue/activate"),
    params: z.object({ binding: runtimeBindingSchema, request: z.object({ operation_id: z.string(), prompt: z.string(), recover_only: z.boolean().optional() }) }),
  }), async (request, extra) => activationHandler
    ? activationHandler(request.params.binding, request.params.request, extra.signal)
    : { status: "unavailable" });
  const abort = () => { void transport.close().catch(() => undefined); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    signal.throwIfAborted();
    await client.connect(transport, { signal });
    signal.throwIfAborted();
    return {
      listTools: (_params, opts) => client.listTools(undefined, opts),
      callTool: async (args, _schema, opts) => await client.callTool(args, undefined, opts) as CallToolResult,
      getInstructions: () => client.getInstructions(),
      setActivationHandler: (handler) => { activationHandler = handler; },
      close: () => client.close(),
      destroy: () => {
        const pid = transport.pid;
        if (pid) { try { process.kill(pid, "SIGKILL"); } catch { /* already gone */ } }
      },
    };
  } catch (error) {
    await transport.close();
    throw error;
  } finally { signal.removeEventListener("abort", abort); }
}, STARTUP_TIMEOUT_MS, { runtimeEnabled: true });
