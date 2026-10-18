import { resolve } from "node:path";
import type { ActivationRequest, ActivationOutcome, RuntimeAdapter, RuntimeBinding } from "./runtime.js";
import { CodexSocketClient } from "./codex-socket.js";
import { CodexThreadReader, userMessageMatches } from "./codex-history.js";

export type CodexNotification = Readonly<{ method: string; params: Record<string, unknown> }>;

export interface CodexRpcClient {
  request(method: string, params: Record<string, unknown>, signal: AbortSignal): Promise<Record<string, unknown>>;
  close(): Promise<void>;
  /** Server notifications (JSON-RPC messages without an id). Optional for older fakes and clients. */
  onNotification?(listener: (notification: CodexNotification) => void): () => void;
  /**
   * Bumped each time the client opens a new app-server connection. Server-side
   * thread attachments (thread/resume) do not survive a reconnect.
   */
  readonly connectionGeneration?: number;
}

export interface CodexRuntimeOptions {
  readonly client?: CodexRpcClient;
  readonly timeoutMs?: number;
  readonly expectedAgent?: string;
}

/**
 * Bounded history window. App-server v2 (`codex app-server generate-json-schema`,
 * v2/ThreadTurnsListParams.json) pages turns newest-first; each page is fetched
 * with `itemsView: "notLoaded"` and only the leading items of each turn are read
 * through `thread/items/list` filtered by `turnId` (v2/ThreadItemsListParams.json).
 * A `turn/start` user message is the first item of its turn.
 */
export const CODEX_TURN_WINDOW = 16;
export const CODEX_TURN_ITEM_WINDOW = 8;

const TERMINAL_TURN = new Set(["completed", "succeeded", "failed", "interrupted"]);
const ACTIVE_TURN = new Set(["inprogress", "in_progress", "running", "started"]);

type Record_ = Record<string, unknown>;
const isRecord = (value: unknown): value is Record_ => !!value && typeof value === "object" && !Array.isArray(value);
const turnStatus = (turn: Record_): string => String(turn.status ?? "").toLowerCase();

const activeTurn = (thread: Record_): boolean => {
  const turns = thread.turns;
  if (!Array.isArray(turns)) return false;
  return turns.some((turn) => isRecord(turn) && ACTIVE_TURN.has(turnStatus(turn)));
};

/** v2 ThreadStatus: notLoaded | idle | systemError | active{activeFlags}. Absent on older daemons. */
const threadStatusType = (thread: Record_): string | undefined =>
  isRecord(thread.status) && typeof thread.status.type === "string" ? thread.status.type : undefined;

const outcomeForTurn = (turn: Record_): ActivationOutcome | undefined => {
  const id = String(turn.id ?? "");
  if (!id) return undefined;
  return TERMINAL_TURN.has(turnStatus(turn)) ? { status: "completed", turn_id: id } : { status: "started", turn_id: id };
};

const unsupportedMethod = (error: unknown): boolean =>
  /-32601|method not found|unknown variant|unknown method|not supported/i.test(String(error));

/** The JSON-RPC error a rejected Codex request carries ("Codex RPC rejected: {code, message}"). */
const rpcError = (error: unknown): Readonly<{ code?: unknown; message?: unknown }> | undefined => {
  const text = error instanceof Error ? error.message : String(error);
  const at = text.indexOf("Codex RPC rejected: ");
  if (at < 0) return undefined;
  try { return JSON.parse(text.slice(at + "Codex RPC rejected: ".length)) as Record_; } catch { return undefined; }
};

/**
 * Codex 0.160.1 before a thread's first user message (live probe 2026-10-06):
 * {"code":-32600,"message":"thread <id> is not materialized yet; thread/turns/list
 * is unavailable before first user message"}. Such a thread has no turns, so no
 * earlier submission can exist in it.
 */
const NOT_MATERIALIZED = /^thread \S+ is not materialized yet; thread\/turns\/list is unavailable before first user message$/;
const notMaterialized = (error: unknown, threadId: string): boolean => {
  const rpc = rpcError(error);
  return rpc?.code === -32600 && typeof rpc.message === "string" && NOT_MATERIALIZED.test(rpc.message) &&
    rpc.message.startsWith(`thread ${threadId} `);
};

type Lookup =
  | Readonly<{ kind: "match"; outcome: ActivationOutcome }>
  | Readonly<{ kind: "none"; busy: boolean; complete: boolean }>
  | Readonly<{ kind: "unsupported" }>;

/** Recent-turn lookup: at most one turns page plus one small items page per turn. */
const lookupRecent = async (
  client: CodexRpcClient, threadId: string, request: ActivationRequest, signal: AbortSignal,
): Promise<Lookup> => {
  let page: Record_;
  try {
    page = await client.request("thread/turns/list", {
      threadId, limit: CODEX_TURN_WINDOW, sortDirection: "desc", itemsView: "notLoaded",
    }, signal);
  } catch (error) {
    if (notMaterialized(error, threadId)) return { kind: "none", busy: false, complete: true };
    if (unsupportedMethod(error)) return { kind: "unsupported" };
    throw error;
  }
  if (!Array.isArray(page.data)) return { kind: "unsupported" };
  const turns = page.data.filter(isRecord);
  for (const turn of turns) {
    const turnId = String(turn.id ?? "");
    if (!turnId) continue;
    const items = await client.request("thread/items/list", {
      threadId, turnId, limit: CODEX_TURN_ITEM_WINDOW, sortDirection: "asc",
    }, signal);
    if (!Array.isArray(items.data)) return { kind: "unsupported" };
    const matched = items.data.some((entry) => isRecord(entry) && isRecord(entry.item) &&
      (entry.turnId === undefined || entry.turnId === turnId) &&
      userMessageMatches(entry.item, request.operation_id, request.prompt));
    if (matched) {
      const outcome = outcomeForTurn(turn);
      if (outcome) return { kind: "match", outcome };
    }
  }
  return {
    kind: "none",
    busy: turns.some((turn) => ACTIVE_TURN.has(turnStatus(turn))),
    // No further page means the whole history was consulted.
    complete: page.nextCursor === null || page.nextCursor === undefined,
  };
};

/** Pre-v2 path: whole-history read, kept only for daemons without turn paging. */
const lookupLegacy = (current: Record_, request: ActivationRequest): Lookup | "ambiguous" | "unavailable" => {
  if (!Array.isArray(current.turns)) return "unavailable";
  const turns = current.turns;
  for (const turn of turns) {
    const items = isRecord(turn) ? turn.items : undefined;
    if (Array.isArray(items)) for (const item of items) {
      if (isRecord(item) && userMessageMatches(item, request.operation_id, request.prompt)) {
        const outcome = outcomeForTurn(turn as Record_);
        if (outcome) return { kind: "match", outcome };
      }
    }
  }
  if (activeTurn(current)) return { kind: "none", busy: true, complete: true };
  if (turns.some((turn) => !isRecord(turn) || !Array.isArray(turn.items) || (turn.itemsView && turn.itemsView !== "full"))) return "ambiguous";
  return { kind: "none", busy: false, complete: true };
};

export const createCodexRuntime = async (
  binding: RuntimeBinding,
  options: CodexRuntimeOptions = {},
): Promise<RuntimeAdapter> => {
  if (binding.client !== "codex") throw new Error("Codex runtime requires a Codex binding");
  const client = options.client ?? new CodexSocketClient(undefined, options.timeoutMs);
  const signal = AbortSignal.timeout(options.timeoutMs ?? 10_000);
  const reader = new CodexThreadReader(client);
  const sameThread = (thread: Record_): boolean => String(thread.id ?? "") === binding.runtime_id &&
    typeof thread.cwd === "string" && resolve(thread.cwd) === resolve(binding.working_directory);
  const readMetadata = async (abortSignal: AbortSignal): Promise<Record_> => {
    const result = await client.request("thread/read", { threadId: binding.runtime_id, includeTurns: false }, abortSignal);
    return (result.thread ?? result) as Record_;
  };
  let raw: Record_;
  try { raw = await readMetadata(signal); } catch (error) { await client.close(); throw error; }
  if (!sameThread(raw)) { await client.close(); throw new Error("Codex thread identity does not match binding"); }
  if (options.expectedAgent) {
    try {
      const ownConnection = await client.request("mcpServer/tool/call", {
        threadId: binding.runtime_id, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {},
      }, signal);
      const status = ownConnection.structuredContent as Record_ | undefined;
      if (ownConnection.isError || status?.agent !== options.expectedAgent) {
        throw new Error("Codex thread does not own this GPTQueue connection");
      }
    } catch (error) { await client.close(); throw error; }
  }

  const idleListeners = new Set<() => void>();
  const staleListeners = new Set<() => void>();
  let stale = false;
  // Connection generation the thread/resume attachment was made on, if any.
  let subscribedOn: number | undefined;
  let hostState: string = threadStatusType(raw) ?? "unknown";
  const emit = (listeners: ReadonlySet<() => void>): void => {
    for (const listener of Array.from(listeners)) { try { listener(); } catch { /* listener faults never reach the socket */ } }
  };
  const markStale = (): void => { if (!stale) { stale = true; emit(staleListeners); } };
  // v2 ServerNotification.json: turn/completed {threadId, turn}, thread/status/changed {threadId, status},
  // thread/closed {threadId}. Notifications for other threads are ignored.
  const unsubscribe = client.onNotification?.((notification) => {
    if (notification.params.threadId !== binding.runtime_id) return;
    switch (notification.method) {
      case "turn/completed": emit(idleListeners); return;
      case "thread/status/changed": {
        const type = isRecord(notification.params.status) ? notification.params.status.type : undefined;
        if (type === "idle") emit(idleListeners);
        return;
      }
      case "thread/closed": markStale(); return;
      default: return;
    }
  });

  /** Attach this connection to an already loaded thread so turn notifications reach it. */
  const subscribe = async (abortSignal: AbortSignal): Promise<void> => {
    if (!idleListeners.size || !client.onNotification) return;
    const generation = client.connectionGeneration ?? 0;
    if (subscribedOn === generation) return;
    subscribedOn = generation;
    try {
      await client.request("thread/resume", { threadId: binding.runtime_id, excludeTurns: true }, abortSignal);
      // The request itself may have reconnected: record the connection it attached to.
      subscribedOn = client.connectionGeneration ?? 0;
    } catch { subscribedOn = undefined; }
  };

  const seen = new Map<string, string>();
  return {
    binding,
    async activate(request: ActivationRequest, abortSignal: AbortSignal): Promise<ActivationOutcome> {
      if (stale) return { status: "unavailable" };
      const current = await readMetadata(abortSignal);
      if (!sameThread(current)) throw new Error("Codex thread identity does not match binding");
      const statusType = threadStatusType(current);
      hostState = statusType ?? "unknown";
      // systemError is recoverable (the thread's next turn clears it), so only defer
      // delivery. Retiring here would close this sidecar and take the thread's
      // GPTQueue tools with it; only thread/closed retires the binding.
      if (statusType === "systemError") return { status: "unavailable" };
      let lookup = await lookupRecent(client, binding.runtime_id, request, abortSignal);
      if (lookup.kind === "unsupported") {
        const legacy = lookupLegacy(await reader.read(binding.runtime_id, abortSignal), request);
        if (legacy === "ambiguous" || legacy === "unavailable") return { status: legacy };
        lookup = legacy;
      }
      if (lookup.kind === "match") return lookup.outcome;
      // `notLoaded` (no attached TUI) still runs turns headless through the daemon;
      // delivery continues and presence reports it via describe().
      if (lookup.kind === "unsupported") return { status: "ambiguous" };
      if (lookup.busy || statusType === "active") { await subscribe(abortSignal); return { status: "busy" }; }
      if (seen.has(request.operation_id) || request.recover_only) return { status: "ambiguous" };
      seen.set(request.operation_id, "pending");
      if (seen.size > 1024) seen.delete(seen.keys().next().value!);
      try {
        const result = await client.request("turn/start", { threadId: binding.runtime_id, input: [{ type: "text", text: request.prompt }], clientUserMessageId: request.operation_id }, abortSignal);
        const id = String(((result.turn ?? result) as Record_).id ?? "");
        if (!id) return { status: "ambiguous" };
        seen.set(request.operation_id, id); return { status: "started", turn_id: id };
      } catch (error) {
        const message = String(error);
        if (abortSignal.aborted) return { status: "ambiguous" };
        if (/timeout|disconnect|closed|network|aborted/i.test(message)) return { status: "ambiguous" };
        if (/Codex RPC rejected:.*(?:busy|active|in progress)/i.test(message)) { seen.delete(request.operation_id); return { status: "busy" }; }
        return { status: "ambiguous" };
      }
    },
    describe: () => Object.freeze({
      host_state: hostState,
      attached: hostState === "idle" || hostState === "active",
      stale,
    }),
    onIdle(listener: () => void): () => void {
      idleListeners.add(listener);
      return () => { idleListeners.delete(listener); };
    },
    onStale(listener: () => void): () => void {
      staleListeners.add(listener);
      if (stale) queueMicrotask(listener);
      return () => { staleListeners.delete(listener); };
    },
    close: async () => { unsubscribe?.(); idleListeners.clear(); staleListeners.clear(); await client.close(); },
  };
};
