import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { isAbsolute } from "node:path";
import { createPiRuntime, type PiRuntimeHost } from "./pi-runtime.js";
import type { ActivationRequest, ActivationOutcome, RuntimeBinding } from "./runtime.js";
import { runtimeBindingSchema } from "./runtime.js";
import { z } from "zod";
import { VERSION } from "../version.js";
import { RUNTIME_TOOL_NAMES, SHELL_TOOL_NAMES } from "./tool-names.js";

export const GPTQUEUE_TOOLS = ["send_message", "receive_message", "list_agents", "get_queue_status"] as const;
/** Every registered-shell tool; the runtime subset is required. */
export const SHELL_TOOLS = SHELL_TOOL_NAMES;
const REQUIRED_RUNTIME_TOOLS = RUNTIME_TOOL_NAMES;
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

export const createPiExtension = (
  makeClient: (signal: AbortSignal, context?: PiContext) => Promise<SessionClient>, timeout = STARTUP_TIMEOUT_MS,
  options: { readonly runtimeEnabled?: boolean } = {}
) => async (pi: PiAPI): Promise<void> => {
  let client: SessionClient | undefined;
  let runtime: ReturnType<typeof createPiRuntime> | undefined;
  let binding: RuntimeBinding | undefined;
  let context: PiContext | undefined;
  let instructions = "";
  const registered = new Set<string>();
  const expected: readonly string[] = options.runtimeEnabled ? [...GPTQUEUE_TOOLS, ...REQUIRED_RUNTIME_TOOLS] : GPTQUEUE_TOOLS;
  const close = async () => {
    runtime?.invalidate(); runtime = undefined; binding = undefined;
    const old = client; client = undefined;
    if (old) await withTimeout(() => old.close(), 2_500);
  };
  const fatal = async (error: unknown): Promise<never> => {
    console.error(`[gptqueue] Pi registration readiness failed: ${error instanceof Error ? error.message : String(error)}`);
    await close().catch(() => undefined);
    process.exit(1);
    throw error;
  };
  const connect = async () => {
    if (!client) client = await withTimeout(async (signal) => {
      const connected = await makeClient(signal, context);
      if (signal.aborted) { await connected.close(); throw new Error("GPTQueue startup aborted"); }
      return connected;
    }, timeout);
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
          if (!client) throw new Error("Pi GPTQueue session is not connected");
          const result = await client.callTool({ name: definition.name, arguments: params }, undefined, { signal });
          const content = result.content.filter((item): item is { type: "text"; text: string } => item.type === "text");
          if (result.isError) throw new Error(content.map((item) => item.text).join("\n") || "GPTQueue tool failed");
          return { content, details: result };
        },
      });
    }
  };
  const startRuntime = async (current: PiContext) => {
    context = current;
    if (!options.runtimeEnabled) return;
    if (!current.cwd || !isAbsolute(current.cwd) || !current.sessionManager.getSessionId() || !pi.sendMessage) throw new Error("Pi session identity or native message API unavailable");
    if (binding?.runtime_id === current.sessionManager.getSessionId() && runtime && client) return;
    if (binding) await close();
    const catalog = await connect();
    registerCatalog(catalog);
    const session = client!;
    if (!session.setActivationHandler) throw new Error("Pi activation request transport unavailable");
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
      return adapter.activate(request, signal);
    });
    const result = await withTimeout((signal) => session.callTool({ name: "bind_runtime", arguments: { ...binding } }, undefined, { signal }), timeout);
    if (result.isError || result.structuredContent?.activation_ready !== true) throw new Error("Pi inbox activation binding failed");
  };
  try {
    if (!options.runtimeEnabled) registerCatalog(await connect());
    pi.on("before_agent_start", async (event) => {
      try {
        if (options.runtimeEnabled) {
          if (!context) throw new Error("Pi session_start has not supplied runtime identity");
          if (!runtime) await startRuntime(context);
          await connect();
        } else {
          await connect();
        }
        const active = pi.getActiveTools();
        const missing = expected.filter((name) => !active.includes(name));
        if (missing.length) pi.setActiveTools([...active, ...missing]);
        if (!expected.every((name) => pi.getActiveTools().includes(name))) throw new Error("GPTQueue tools are not active");
        return { systemPrompt: `${event.systemPrompt}\n\n${instructions}` };
      } catch (error) { return fatal(error); }
    });
    if (options.runtimeEnabled) {
      pi.on("session_start", async (_event, current) => {
        try { await startRuntime(current); } catch (error) { await fatal(error); }
      });
      pi.on("session_before_switch", async () => { await close(); });
      pi.on("agent_end", (event) => {
        for (const item of event.messages ?? []) {
          if (!item || typeof item !== "object") continue;
          const message = item as Record<string, unknown>;
          const details = message.details as Record<string, unknown> | undefined;
          if (message.role === "custom" && message.customType === "gptqueue-inbox-activation" &&
              details?.runtime_id === binding?.runtime_id && typeof details?.operation_id === "string") {
            pi.appendEntry?.("gptqueue-inbox-activation-completed", { operation_id: details.operation_id,
              runtime_id: binding!.runtime_id, turn_id: `pi:${details.operation_id}`, status: "completed" });
          }
        }
      });
    }
    pi.on("session_shutdown", close);
  } catch (error) { return fatal(error); }
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
    };
  } catch (error) {
    await transport.close();
    throw error;
  } finally { signal.removeEventListener("abort", abort); }
}, STARTUP_TIMEOUT_MS, { runtimeEnabled: true });
