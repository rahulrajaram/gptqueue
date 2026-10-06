/**
 * End-to-end acceptance harness: real gptqueue components against the owned
 * Redis (REDIS_URL from scripts/with-owned-redis.mjs), with protocol-faithful
 * fake native hosts standing in for each runtime.
 *
 * - Senders use the real MCP tool layer (send_message, claim_tasks,
 *   acknowledge_tasks) on a registered RedisClient.
 * - Codex: real runtime controller, inbox dispatcher, bound MCP server and
 *   Codex adapter; the fake is a Codex app-server v2 JSON-RPC peer
 *   (thread/read, thread/turns/list, thread/items/list, turn/start,
 *   turn/completed notifications), shapes as in tests/codex-runtime.test.ts.
 * - Pi: the real registered-shell sidecar (startRegisteredShell, client "pi")
 *   over an in-memory MCP transport, driven by the real Pi extension
 *   (createPiExtension) against a fake Pi extension API (sendMessage
 *   followUp, isIdle, hasPendingMessages, agent_end, appendEntry).
 * - OpenCode: the real plugin (createOpenCodePlugin) and SDK port against a
 *   fake OpenCode SDK client (session.get/status/messages/promptAsync).
 * - Claude: no turn injection. The stand-in publishes a real session record
 *   for its own process (as the Claude MCP server does), so the real Stop hook
 *   (runClaudeHook) resolves a confirmed identity and blocks a finished turn
 *   when mail is pending; that block is the next turn. While idle, the real
 *   event-driven watcher (watchInbox, the body of `gptqueue-inbox-peek
 *   --watch`) exiting is the wake. Each injection records which path made it.
 *
 * Every fake "model" is cooperative: given a turn, it claims, answers each
 * task with a correlated result, and acknowledges, until its inbox is empty.
 */
import { randomUUID } from "node:crypto";
import { mkdtempSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Redis } from "ioredis";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { z } from "zod";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import { createBoundMcpServer } from "../../src/experimental-wrapper/bridge.js";
import { createRuntimeController } from "../../src/registered-shell/runtime-controller.js";
import { createCodexRuntime, type CodexNotification, type CodexRpcClient } from "../../src/registered-shell/codex-runtime.js";
import { startRegisteredShell } from "../../src/registered-shell/server.js";
import { createPiExtension, type PiAPI, type PiContext, type SessionClient } from "../../src/registered-shell/pi-extension.js";
import { createOpenCodePlugin, type OpenCodeNativeClient, type OpenCodePluginHooks } from "../../src/registered-shell/opencode-plugin-factory.js";
import { opencodeAgentName } from "../../src/registered-shell/opencode-backend.js";
import { runtimeBindingSchema, type ActivationOutcome, type ActivationRequest, type RuntimeBinding } from "../../src/registered-shell/runtime.js";
import { watchInbox } from "../../src/claude/peek.js";
import { runClaudeHook } from "../../src/claude/hook.js";
import { publishSessionRecord, readProcess } from "../../src/claude/session-record.js";
import { registerTools } from "../../src/transports/setup-tools.js";
import { SESSION_KEYS } from "../../src/core/keys.js";

export const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
// Keep registered-shell lifecycle logs out of the user's state directory.
process.env.GPTQ_LOG_DIR ??= mkdtempSync(join(tmpdir(), "gptq-e2e-logs-"));

export type Runtime = "codex" | "pi" | "opencode" | "claude";
export const ALL_RUNTIMES: readonly Runtime[] = Object.freeze(["codex", "pi", "opencode", "claude"]);
export const INJECTING_RUNTIMES: readonly Runtime[] = Object.freeze(["codex", "pi", "opencode"]);

/** Wall-clock ms with sub-ms resolution, comparable with trace ISO timestamps. */
export const now = (): number => performance.timeOrigin + performance.now();
export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
export const waitFor = async (check: () => boolean | Promise<boolean>, timeoutMs = 10_000, label = "condition"): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for ${label}`);
    await sleep(5);
  }
};
const tempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

// ---------------------------------------------------------------- the task

/** Task content and its independently computable answer. */
export const taskContent = (a: number, b: number): string => `compute:${a}+${b}`;
export const expectedAnswer = (a: number, b: number): string => `sum=${a + b}`;
const answer = (content: string): string => {
  const match = /^compute:(-?\d+)\+(-?\d+)$/u.exec(content);
  return match ? `sum=${Number(match[1]) + Number(match[2])}` : `unrecognised:${content}`;
};

// ---------------------------------------------------------------- tool calls

export type Payload = Record<string, unknown>;
/** One tool call by unprefixed gptqueue name; returns the parsed payload, throws on a tool error. */
export type ToolCall = (name: "claim_tasks" | "acknowledge_tasks" | "send_message", args: Payload) => Promise<Payload>;

type CallResult = Readonly<{ content?: unknown; structuredContent?: unknown; isError?: boolean }>;
export const parseToolResult = (result: CallResult | string): Payload => {
  if (typeof result === "string") {
    const parsed = JSON.parse(result) as Payload;
    // OpenCode tools return the stringified CallToolResult.
    return Array.isArray(parsed.content) ? parseToolResult(parsed as CallResult) : parsed;
  }
  const text = Array.isArray(result.content)
    ? (result.content as Array<{ type?: string; text?: string }>).filter((c) => c.type === "text").map((c) => c.text).join("")
    : "";
  const payload = (result.structuredContent as Payload | undefined) ?? (text ? JSON.parse(text) as Payload : {});
  if (result.isError || payload.status === "error") throw new Error(`tool error: ${JSON.stringify(payload)}`);
  return payload;
};

export type QueuedMessage = Readonly<{ id: string; from: string; to: string; type: string; payload: { content: string; in_reply_to?: string } }>;
const claimedTasks = (payload: Payload): Readonly<{ claim_id: string; tasks: readonly QueuedMessage[] }> | null => {
  const claim = payload.claim as { claim_id?: string; tasks?: unknown[] } | null | undefined;
  if (!claim?.claim_id) return null;
  const tasks = (claim.tasks ?? []).map((raw) => (typeof raw === "string" ? JSON.parse(raw) : raw) as QueuedMessage);
  return { claim_id: claim.claim_id, tasks };
};

/** What a model did with one claimed message. */
export type Handled = Readonly<{
  message: QueuedMessage; claim_id: string; acknowledged: boolean; reply_id?: string; turn: number; at: number;
}>;

/** Short lease so work claimed by a turn cut off by a restart is recovered within a test (DL-2). */
export const CLAIM_TTL_SECONDS = 5;

/** A cooperative agent turn: claim, answer each task with a correlated result, acknowledge; repeat until empty. */
export const cooperativeTurn = async (call: ToolCall, agent: () => string, turn: number, log: Handled[]): Promise<void> => {
  for (;;) {
    const claim = claimedTasks(await call("claim_tasks", { max_batch: 16, ttl_seconds: CLAIM_TTL_SECONDS }));
    if (!claim) return;
    const done: Array<Omit<Handled, "acknowledged">> = [];
    for (const message of claim.tasks) {
      if (message.to !== agent()) throw new Error(`cross-delivery: ${agent()} claimed a message for ${message.to}`);
      if (message.type === "task") {
        const sent = await call("send_message", { to: message.from, type: "result", content: answer(message.payload.content),
          in_reply_to: message.id, idempotency_key: `reply-${message.id}` });
        done.push({ message, claim_id: claim.claim_id, reply_id: String(sent.message_id), turn, at: now() });
      } else {
        // A result/error for outstanding work: continue it, never auto-reply to a reply.
        done.push({ message, claim_id: claim.claim_id, turn, at: now() });
      }
    }
    await call("acknowledge_tasks", { claim_id: claim.claim_id });
    log.push(...done.map((entry) => Object.freeze({ ...entry, acknowledged: true })));
  }
};

// ---------------------------------------------------------------- senders

export interface Sender {
  readonly name: string;
  send(to: string, content: string, options?: Readonly<{ type?: "task" | "result" | "error"; in_reply_to?: string; idempotency_key?: string }>): Promise<Readonly<{ message_id: string; sent_at: number; persisted_at: number }>>;
  /** Claim and acknowledge everything queued for this sender (replies). */
  drain(): Promise<readonly Readonly<{ message: QueuedMessage; claim_id: string }>[]>;
  readonly received: Array<Readonly<{ message: QueuedMessage; claim_id: string }>>;
  close(): Promise<void>;
}

/**
 * The generic gptqueue MCP tool layer (registerTools, as the stdio and HTTP
 * servers use) for a registered client, over an in-memory transport. It writes
 * the same inbox trace stages as production (task_claimed, task_acknowledged).
 */
const genericToolCall = async (client: RedisClient): Promise<Readonly<{ call: ToolCall; close: () => Promise<void> }>> => {
  const server = new McpServer({ name: "gptqueue-e2e", version: "0.0.0" });
  registerTools(server, client);
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const mcp = new Client({ name: "e2e-generic", version: "0.0.0" });
  await mcp.connect(clientTransport);
  const call: ToolCall = (name, args) => mcpToolCall(() => mcp)(name, { ...args, session_id: client.sessionId ?? undefined });
  return { call, close: async () => { await mcp.close().catch(() => undefined); await server.close().catch(() => undefined); } };
};

export const startSender = async (label = "sender"): Promise<Sender> => {
  const client = new RedisClient(null, REDIS_URL);
  const name = `e2e-${label}-${randomUUID()}`;
  await client.register("both", name);
  const { call, close: closeTools } = await genericToolCall(client);
  const received: Array<Readonly<{ message: QueuedMessage; claim_id: string }>> = [];
  return {
    name, received,
    send: async (to, content, options = {}) => {
      const sent_at = now();
      const result = await call("send_message", { to, content, type: options.type ?? "task", in_reply_to: options.in_reply_to,
        idempotency_key: options.idempotency_key });
      // send_message reports success only after the message is durably stored (DL-1).
      const persisted_at = now();
      if (result.status !== "sent") throw new Error(`send not accepted: ${JSON.stringify(result)}`);
      return { message_id: String(result.message_id), sent_at, persisted_at };
    },
    drain: async () => {
      const batch: Array<Readonly<{ message: QueuedMessage; claim_id: string }>> = [];
      for (;;) {
        const claim = claimedTasks(await call("claim_tasks", { max_batch: 16, ttl_seconds: 120 }));
        if (!claim) break;
        await call("acknowledge_tasks", { claim_id: claim.claim_id });
        batch.push(...claim.tasks.map((message) => Object.freeze({ message, claim_id: claim.claim_id })));
      }
      received.push(...batch);
      return batch;
    },
    close: async () => { await closeTools(); await client.closeCurrentSession().catch(() => undefined); await client.shutdown().catch(() => undefined); },
  };
};

// ---------------------------------------------------------------- recipients

export interface Recipient {
  readonly runtime: Runtime;
  /** The mailbox name currently held (may change only through explicit adoption on restart). */
  name(): string;
  /** Messages the model handled, in order. */
  readonly handled: Handled[];
  /** Native host receipt time of every injected turn (Claude: watcher exit or Stop-hook block). */
  readonly injections: number[];
  /** Claude only: which path made each injection, index-aligned with injections. */
  readonly injectionVia?: readonly ClaudeWake[];
  /** Start time of every model turn (Pi queues a mid-turn follow-up at receipt and runs it after the turn). */
  readonly turnStarts: number[];
  /** Turns that ended on a tool error. */
  readonly turnErrors: string[];
  /** True while a model turn is running. */
  turnRunning(): boolean;
  /** Simulate a user turn in progress. */
  hold(): void;
  /** End the simulated user turn; returns the turn-end time. */
  release(): Promise<number>;
  /** A send originated by this recipient's own tools (outstanding work for WK-3). */
  send(to: string, content: string): Promise<string>;
  /** Restart the dispatcher/sidecar mid-stream, preserving the native session. */
  restart(): Promise<void>;
  close(): Promise<void>;
}

/** Serialises model turns so a fake host never runs two at once. */
class TurnRunner {
  turns = 0;
  running = false;
  /** Start time of every model turn. */
  readonly starts: number[] = [];
  /** A turn whose tool calls failed (e.g. its sidecar restarted) ends; its claim is recovered after the lease. */
  readonly errors: string[] = [];
  constructor(private readonly call: () => ToolCall, private readonly agent: () => string, private readonly log: Handled[]) {}
  async run(): Promise<void> {
    this.running = true;
    this.turns += 1;
    this.starts.push(now());
    try { await cooperativeTurn(this.call(), this.agent, this.turns, this.log); }
    catch (error) { this.errors.push(String(error)); }
    finally { this.running = false; }
  }
}

const mcpToolCall = (client: () => Client): ToolCall => async (name, args) =>
  parseToolResult(await client().callTool({ name, arguments: stripUndefined(args) }) as CallResult);
const stripUndefined = (args: Payload): Payload => Object.fromEntries(Object.entries(args).filter(([, v]) => v !== undefined));

// ---- Codex: app-server v2 JSON-RPC fake

type CodexTurn = { id: string; status: "inProgress" | "completed"; items: Array<Record<string, unknown>> };
class FakeCodexAppServer {
  readonly turns: CodexTurn[] = [];
  readonly injections: number[] = [];
  private readonly listeners = new Set<(n: CodexNotification) => void>();
  private held: CodexTurn | undefined;
  /** Codex 0.160.1 materializes a thread at its first user message (turn/start or a user turn). */
  private materialized = false;
  onTurn: () => Promise<void> = async () => undefined;
  constructor(readonly threadId: string, readonly cwd: string) {}
  private active(): boolean { return this.turns.some((turn) => turn.status === "inProgress"); }
  private emit(method: string, params: Record<string, unknown>): void { for (const l of Array.from(this.listeners)) l({ method, params }); }
  private complete(turn: CodexTurn): void {
    turn.status = "completed";
    this.emit("turn/completed", { threadId: this.threadId, turn: { id: turn.id, status: "completed" } });
    this.emit("thread/status/changed", { threadId: this.threadId, status: { type: this.active() ? "active" : "idle" } });
  }
  hold(): void {
    this.materialized = true;
    this.held = { id: `turn-user-${randomUUID()}`, status: "inProgress", items: [{ type: "userMessage", id: randomUUID(), content: [{ type: "text", text: "user work" }] }] };
    this.turns.push(this.held);
  }
  release(): number { const turn = this.held; this.held = undefined; const at = now(); if (turn) this.complete(turn); return at; }
  /** One JSON-RPC connection (the adapter closes it on dispatcher stop). */
  connection(): CodexRpcClient {
    const own = new Set<(n: CodexNotification) => void>();
    return {
      onNotification: (listener) => { own.add(listener); this.listeners.add(listener); return () => { own.delete(listener); this.listeners.delete(listener); }; },
      close: async () => { for (const l of own) this.listeners.delete(l); own.clear(); },
      request: async (method, params) => this.request(method, params),
    };
  }
  private async request(method: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (params.threadId !== undefined && params.threadId !== this.threadId) throw new Error("Codex RPC rejected: thread not found");
    switch (method) {
      case "thread/read":
        return { thread: { id: this.threadId, cwd: this.cwd, status: this.active() ? { type: "active", activeFlags: [] } : { type: "idle" } } };
      case "thread/turns/list":
        // Real Codex 0.160.1 rejects this on a never-prompted thread (live probe 2026-10-06).
        if (!this.materialized) {
          throw new Error(`Codex RPC rejected: ${JSON.stringify({ code: -32600,
            message: `thread ${this.threadId} is not materialized yet; thread/turns/list is unavailable before first user message` })}`);
        }
        return { data: [...this.turns].reverse().slice(0, Number(params.limit ?? 16)).map(({ id, status }) => ({ id, status, items: [], itemsView: "notLoaded" })), nextCursor: null, backwardsCursor: null };
      case "thread/items/list": {
        const turn = this.turns.find((t) => t.id === params.turnId);
        return { data: (turn?.items ?? []).map((item) => ({ turnId: turn!.id, item })), nextCursor: null };
      }
      case "thread/resume": return { thread: { id: this.threadId } };
      case "turn/start": {
        if (this.active()) throw new Error("Codex RPC rejected: a turn is already in progress");
        this.injections.push(now());
        this.materialized = true;
        const input = params.input as Array<{ type: string; text: string }>;
        const turn: CodexTurn = { id: `turn-${randomUUID()}`, status: "inProgress",
          items: [{ type: "userMessage", id: randomUUID(), clientId: params.clientUserMessageId, content: input }] };
        this.turns.push(turn);
        setImmediate(() => { void this.onTurn().finally(() => this.complete(turn)); });
        return { turn: { id: turn.id, status: "inProgress", items: [] } };
      }
      default: throw new Error(`-32601 method not found: ${method}`);
    }
  }
}

export const startCodexRecipient = async (): Promise<Recipient> => {
  const cwd = tempDir("gptq-e2e-codex-");
  const host = new FakeCodexAppServer(`thr-${randomUUID()}`, cwd);
  const redis = new RedisClient(null, REDIS_URL);
  await redis.register("both", `e2e-codex-${randomUUID()}`);
  const handled: Handled[] = [];
  let client!: Client;
  let stopSidecar: () => Promise<void> = async () => undefined;
  const runner = new TurnRunner(() => mcpToolCall(() => client), () => redis.requireRegistered(), handled);
  host.onTurn = () => runner.run();
  /** One sidecar generation: controller + dispatcher + bound MCP server + model's MCP client. */
  const startSidecar = async () => {
    const shutdown = new AbortController();
    const controller = createRuntimeController(redis, { client: "codex", working_directory: cwd },
      async (binding) => createCodexRuntime(binding, { client: host.connection() }));
    const server = createBoundMcpServer({ agentName: redis.requireRegistered(), redisClient: redis, runtime: controller }, shutdown.signal);
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    client = new Client({ name: "fake-codex", version: "0.0.0" });
    await client.connect(clientTransport);
    const bound = parseToolResult(await client.callTool({ name: "bind_runtime", arguments: { client: "codex", runtime_id: host.threadId, epoch: randomUUID(), working_directory: cwd } }) as CallResult);
    if (bound.activation_ready !== true) throw new Error(`codex bind failed: ${JSON.stringify(bound)}`);
    const own = client;
    stopSidecar = async () => { shutdown.abort(); await controller.close(); await own.close().catch(() => undefined); await server.close().catch(() => undefined); };
  };
  await startSidecar();
  return {
    runtime: "codex", handled, turnStarts: runner.starts, turnErrors: runner.errors, turnRunning: () => runner.running, injections: host.injections,
    name: () => redis.requireRegistered(),
    hold: () => host.hold(),
    release: async () => host.release(),
    send: async (to, content) => String((await mcpToolCall(() => client)("send_message", { to, content, type: "task" })).message_id),
    restart: async () => { await stopSidecar(); await startSidecar(); },
    close: async () => { await stopSidecar(); await redis.closeCurrentSession().catch(() => undefined); await redis.shutdown().catch(() => undefined); },
  };
};

// ---- Pi: real sidecar + real extension, fake Pi extension API

type PiEntry = { type: string; customType?: string; data?: unknown; message?: { role?: string; customType?: unknown; details?: unknown } };
type PiTool = { name: string; execute: (id: string, params: unknown, signal?: AbortSignal) => Promise<{ content: unknown; details?: unknown }> };

export const startPiRecipient = async (): Promise<Recipient> => {
  const cwd = tempDir("gptq-e2e-pi-");
  const sessionId = `pi-session-${randomUUID()}`;
  const handled: Handled[] = [];
  const injections: number[] = [];
  const entries: PiEntry[] = [];
  const followUps: Array<{ customType: string; details: unknown }> = [];
  const tools = new Map<string, PiTool>();
  const handlers = new Map<string, (...args: any[]) => Promise<unknown> | unknown>();
  let active: string[] = ["read", "bash"];
  let held = false;
  let draining = false;
  const shells: Array<Awaited<ReturnType<typeof startRegisteredShell>>> = [];
  const context: PiContext = {
    cwd, hasUI: false,
    sessionManager: { getSessionId: () => sessionId, getEntries: () => entries },
    isIdle: () => !held && !runner.running && !draining,
    hasPendingMessages: () => followUps.length > 0,
  };
  const call: ToolCall = async (name, args) => {
    const tool = tools.get(name);
    if (!tool) throw new Error(`Pi tool ${name} not registered`);
    const result = await tool.execute(randomUUID(), stripUndefined(args));
    return parseToolResult(result.details as CallResult);
  };
  const runner = new TurnRunner(() => call, () => shells.at(-1)!.agentName, handled);
  /** Pi delivers queued follow-ups one turn each, after any running turn ends. */
  const drain = async () => {
    if (draining || held) return;
    draining = true;
    try {
      while (!held && followUps.length) {
        const next = followUps.shift()!;
        const message = { role: "custom", customType: next.customType, details: next.details };
        entries.push({ type: "message", message });
        await runner.run();
        await handlers.get("agent_end")?.({ messages: [message] }, context);
      }
    } finally { draining = false; }
  };
  const pi: PiAPI = {
    registerTool: (definition) => { tools.set(definition.name, definition as unknown as PiTool); },
    on: ((event: string, handler: (...args: any[]) => unknown) => { handlers.set(event, handler as never); }) as PiAPI["on"],
    appendEntry: (customType, data) => { entries.push({ type: "custom", customType, data }); },
    getActiveTools: () => [...active],
    setActiveTools: (names) => { active = [...names]; },
    sendMessage: (payload, options) => {
      if (options.deliverAs !== "followUp" || !options.triggerTurn) throw new Error("unexpected Pi delivery mode");
      injections.push(now());
      const p = payload as { customType: string; details: unknown };
      followUps.push({ customType: p.customType, details: p.details });
      setImmediate(() => { void drain(); });
    },
  };
  /** The registered Pi sidecar, connected in-process instead of over stdio. */
  const makeClient = async (): Promise<SessionClient> => {
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    const shell = await startRegisteredShell({ client: "pi", redisUrl: REDIS_URL, cwd }, new AbortController().signal, () => serverTransport);
    shells.push(shell);
    const client = new Client({ name: "gptqueue-pi", version: "0.0.0" });
    let activation: ((binding: RuntimeBinding, request: ActivationRequest, signal: AbortSignal) => Promise<ActivationOutcome>) | undefined;
    client.setRequestHandler(z.object({
      method: z.literal("gptqueue/activate"),
      params: z.object({ binding: runtimeBindingSchema, request: z.object({ operation_id: z.string(), prompt: z.string(), recover_only: z.boolean().optional() }) }),
    }), async (request, extra) => activation
      ? activation(request.params.binding, request.params.request, extra.signal)
      : { status: "unavailable" as const });
    await client.connect(clientTransport);
    return {
      listTools: (_params, options) => client.listTools(undefined, options),
      callTool: async (args, _schema, options) => await client.callTool(args, undefined, options) as never,
      getInstructions: () => client.getInstructions(),
      setActivationHandler: (handler) => { activation = handler; },
      close: async () => { await client.close().catch(() => undefined); await shell.closed; },
    };
  };
  await createPiExtension(makeClient, 5_000, { runtimeEnabled: true, retry: { baseMs: 50, maxMs: 200 } })(pi);
  const start = async () => {
    await handlers.get("session_start")!({}, context);
    await waitFor(() => tools.has("claim_tasks") && shells.length > 0, 5_000, "Pi binding");
  };
  await start();
  // Peers address the Pi session's bound mailbox, which survives restarts.
  // A restarted sidecar holds a per-startup provisional name until adoption
  // succeeds (adoption is refused while the cut-off turn's claim is
  // unexpired), and nothing real ever sends to that provisional name.
  const mailbox = shells.at(-1)!.agentName;
  return {
    runtime: "pi", handled, turnStarts: runner.starts, turnErrors: runner.errors, turnRunning: () => runner.running, injections,
    name: () => mailbox,
    hold: () => { held = true; },
    release: async () => { held = false; const at = now(); await handlers.get("agent_end")?.({ messages: [] }, context); setImmediate(() => { void drain(); }); return at; },
    send: async (to, content) => String((await call("send_message", { to, content, type: "task" })).message_id),
    // A sidecar restart: Pi tears the session's GPTQueue connection down and a new one re-binds the same Pi session.
    restart: async () => { await handlers.get("session_shutdown")!(); await start(); },
    close: async () => { await handlers.get("session_shutdown")?.(); for (const shell of shells) await shell.close().catch(() => undefined); },
  };
};

// ---- OpenCode: real plugin + SDK port, fake SDK client

export const startOpenCodeRecipient = async (): Promise<Recipient> => {
  const directory = tempDir("gptq-e2e-opencode-");
  const sessionID = `ses_${randomUUID().replace(/-/gu, "")}`;
  const handled: Handled[] = [];
  const injections: number[] = [];
  const history: Array<{ info: { id: string; role: string; sessionID: string }; parts: Array<{ type: string; text: string }> }> = [];
  let held = false;
  let plugin!: OpenCodePluginHooks;
  const call: ToolCall = async (name, args) => {
    const tool = plugin.tool[`gptqueue_${name}`];
    if (!tool) throw new Error(`OpenCode tool gptqueue_${name} missing`);
    const output = await tool.execute(stripUndefined(args), { sessionID, directory, abort: new AbortController().signal });
    return parseToolResult(typeof output === "string" ? output : output.output);
  };
  const runner = new TurnRunner(() => call, () => opencodeAgentName(sessionID), handled);
  const busy = () => held || runner.running;
  const idleEvent = () => plugin.event({ event: { type: "session.idle", properties: { sessionID } } });
  const sdk: OpenCodeNativeClient = {
    session: {
      get: async (options: { path: { id: string } }) => {
        if (options.path.id !== sessionID) throw Object.assign(new Error("not found"), { name: "NotFoundError" });
        return { id: sessionID, directory };
      },
      // OpenCode omits idle sessions from /session/status.
      status: async () => (busy() ? { [sessionID]: { type: "busy" } } : {}),
      messages: async () => history.map((entry) => ({ ...entry, parts: [...entry.parts] })),
      promptAsync: async (options: { path: { id: string }; body: { messageID: string; parts: Array<{ type: string; text: string }> } }) => {
        if (options.path.id !== sessionID) throw new Error("session not found");
        injections.push(now());
        history.push({ info: { id: options.body.messageID, role: "user", sessionID }, parts: options.body.parts });
        setImmediate(() => { void runner.run().then(idleEvent); });
      },
      list: async () => [],
    },
  };
  const startPlugin = async () => {
    plugin = createOpenCodePlugin({ client: sdk, directory }, { redisUrl: REDIS_URL, epoch: randomUUID(), bootstrapLimit: 0 });
    await plugin.event({ event: { type: "session.created", properties: { info: { id: sessionID, directory } } } });
    // Ready when the session's dispatcher owns activation (read-only status tool).
    await waitFor(async () => {
      const output = await plugin.tool.gptqueue_get_runtime_status!.execute({}, { sessionID, directory, abort: new AbortController().signal }).catch(() => undefined);
      if (output === undefined) return false;
      const status = parseToolResult(typeof output === "string" ? output : output.output);
      return status.activation_ready === true;
    }, 5_000, "OpenCode activation ready");
  };
  await startPlugin();
  return {
    runtime: "opencode", handled, turnStarts: runner.starts, turnErrors: runner.errors, turnRunning: () => runner.running, injections,
    name: () => opencodeAgentName(sessionID),
    hold: () => { held = true; },
    release: async () => { held = false; const at = now(); await idleEvent(); return at; },
    send: async (to, content) => String((await call("send_message", { to, content, type: "task" })).message_id),
    restart: async () => { await plugin.dispose(); await startPlugin(); },
    close: async () => { await plugin.dispose(); },
  };
};

// ---- Claude: watcher exit is the wake; the Stop hook decides continuation

/** stop_hook: the Stop hook blocked a finished turn; watcher: the idle watcher exited on mail. */
export type ClaudeWake = "stop_hook" | "watcher";

export const startClaudeRecipient = async (options: Readonly<{ name?: string; claudeSessionId?: string }> = {}): Promise<Recipient> => {
  const redis = new RedisClient(null, REDIS_URL);
  await redis.register("both", options.name ?? `e2e-claude-${randomUUID()}`);
  const runtimeDir = tempDir("gptq-e2e-claude-");
  mkdirSync(runtimeDir, { recursive: true, mode: 0o700 });
  const claudeSession = options.claudeSessionId ?? randomUUID();
  const handled: Handled[] = [];
  const injections: number[] = [];
  const injectionVia: ClaudeWake[] = [];
  const inject = (via: ClaudeWake) => { injections.push(now()); injectionVia.push(via); };
  const { call, close: closeTools } = await genericToolCall(redis);
  const runner = new TurnRunner(() => call, () => redis.requireRegistered(), handled);
  let held = false;
  let closed = false;
  let watcher: AbortController | undefined;
  let loop: Promise<void> = Promise.resolve();
  let generation = 0;
  // This process stands in for the Claude process: the hook resolves identity
  // from the record keyed by this pid, never from the environment or /proc scan.
  const env = (): NodeJS.ProcessEnv => ({ ...process.env, REDIS_URL, GPTQ_AGENT_NAME: "", GPTQ_CLAUDE_PID: String(process.pid),
    GPTQ_CLAUDE_LEGACY_SCAN: "off", GPTQ_CLAUDE_RUNTIME_DIR: runtimeDir, GPTQ_CLAUDE_WATCH: "off", CLAUDE_PROJECT_DIR: "" });
  const self = readProcess(process.pid);
  if (!self) throw new Error("cannot read /proc for the Claude stand-in");
  if (!redis.sessionId) throw new Error("Claude stand-in has no gptqueue session");
  publishSessionRecord(self, redis.requireRegistered(), redis.sessionId, env());
  /** The real Stop hook: "block" means Claude continues with another turn. */
  const stopHookBlocks = async (stopHookActive: boolean): Promise<boolean> => {
    const result = await runClaudeHook(["stop"], JSON.stringify({ session_id: claudeSession, cwd: runtimeDir, stop_hook_active: stopHookActive }), runtimeDir, env());
    return result.stdout !== null && (JSON.parse(result.stdout) as { decision?: string }).decision === "block";
  };
  /** Turns ended by the Stop hook: each block is an immediate next turn (stop_hook_active, as Claude sends it). */
  const continueWhileBlocked = async (stopHookActive: boolean) => {
    let active = stopHookActive;
    while (!held && await stopHookBlocks(active)) { inject("stop_hook"); await runner.run(); active = true; }
  };
  /** One model turn, then the Stop hook. */
  const turnAndStop = async () => { await runner.run(); await continueWhileBlocked(false); };
  /** Idle: run the event-driven watcher in the background; its exit wakes the session. */
  const idleLoop = async (mine: number) => {
    while (!closed && !held && mine === generation) {
      watcher = new AbortController();
      const outcome = await watchInbox(REDIS_URL, redis.requireRegistered(), { timeoutSec: 0, blockMs: 5_000, signal: watcher.signal, giveUpAfterMs: null, retryMs: 50 });
      if (outcome.kind !== "mail" || closed || held || mine !== generation) return;
      inject("watcher");
      await turnAndStop();
    }
  };
  const startWatcher = () => { generation += 1; loop = idleLoop(generation); };
  const stopWatcher = async () => { generation += 1; watcher?.abort(); await loop; };
  startWatcher();
  return {
    runtime: "claude", handled, turnStarts: runner.starts, turnErrors: runner.errors, turnRunning: () => runner.running, injections, injectionVia,
    name: () => redis.requireRegistered(),
    hold: () => { held = true; generation += 1; watcher?.abort(); },
    release: async () => {
      await loop; held = false; const at = now();
      // The user's turn ends: the Stop hook runs, and either continues with mail or the session idles with its watcher.
      await continueWhileBlocked(false);
      startWatcher();
      return at;
    },
    send: async (to, content) => String((await call("send_message", { to, content, type: "task" })).message_id),
    // A restarted watcher re-checks LLEN first, so mail that arrived while it was down still wakes the session.
    restart: async () => { await stopWatcher(); startWatcher(); },
    close: async () => {
      closed = true; watcher?.abort(); await loop.catch(() => undefined); await closeTools();
      publishSessionRecord(self, null, null, env());
      await redis.closeCurrentSession().catch(() => undefined); await redis.shutdown().catch(() => undefined); },
  };
};

export const startRecipient = (runtime: Runtime): Promise<Recipient> => {
  switch (runtime) {
    case "codex": return startCodexRecipient();
    case "pi": return startPiRecipient();
    case "opencode": return startOpenCodeRecipient();
    case "claude": return startClaudeRecipient();
    default: { const unreachable: never = runtime; throw new Error(String(unreachable)); }
  }
};

// ---------------------------------------------------------------- trace reading

export type TraceEntry = Readonly<Record<string, string>>;
type XRange = Pick<Redis, "xrange">;
const readTraceFrom = async (conn: XRange, agent: string): Promise<readonly TraceEntry[]> =>
  (await conn.xrange(SESSION_KEYS.inboxTrace(agent), "-", "+")).map(([, fields]) => {
    const out: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) out[fields[i]!] = fields[i + 1]!;
    return Object.freeze(out);
  });
export const readTrace = (redis: RedisClient, agent: string): Promise<readonly TraceEntry[]> => readTraceFrom(redis.adapterConnection, agent);

/** Server-side consumption of one message: its task_claimed row and the task_acknowledged row for that claim. */
export type TraceConsumption = Readonly<{ claim_id: string | undefined; acknowledged: boolean }>;
export const traceConsumption = async (agent: string, messageId: string, timeoutMs = 5_000): Promise<TraceConsumption> => {
  const conn = new Redis(REDIS_URL, { maxRetriesPerRequest: 3 });
  const look = async (): Promise<TraceConsumption> => {
    const rows = await readTraceFrom(conn, agent);
    const claim = rows.find((row) => row.stage === "task_claimed" && row.message_id === messageId);
    const acknowledged = claim !== undefined && rows.some((row) => row.stage === "task_acknowledged" && row.claim_id === claim.claim_id);
    return { claim_id: claim?.claim_id, acknowledged };
  };
  try {
    let found = await look();
    const deadline = Date.now() + timeoutMs;
    while (!found.acknowledged && Date.now() < deadline) { await sleep(10); found = await look(); }
    return found;
  } finally { conn.disconnect(); }
};

export const percentile = (values: readonly number[], p: number): number => {
  if (!values.length) return Number.NaN;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))]!;
};
