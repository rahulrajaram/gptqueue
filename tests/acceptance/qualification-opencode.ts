import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import {
  model,
  modelsPath,
  newConfigHome,
  newWorkDir,
  opencodeBin,
  opencodePrerequisites,
} from "./opencode-support.js";
import {
  repairConfig,
  repairPluginPath,
  startRepairOpenCodeServer,
  type RepairOpenCodeServer,
  type RepairSession,
} from "./opencode-repair-support.js";
import {
  routeIds,
  type Availability,
  type ModelParticipant,
  type ParticipantIdentity,
  type RouteAdapter,
  type RouteId,
  type RouteSpec,
  type RuntimeStatus,
} from "./qualification-types.js";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import type { QueueMessage } from "../../src/mcp-server/types.js";
import { opencodeAgentName } from "../../src/registered-shell/opencode-backend.js";
import { childReadinessEvidence } from "./opencode-qualification-oracles.js";

const openCodeRoutes = [
  "opencode-interactive", "opencode-run", "opencode-native-task", "opencode-fork",
  "opencode-resume", "opencode-serve-attach", "opencode-acp",
] as const satisfies readonly RouteId[];

const timeoutMs = 150_000;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("OpenCode qualification operation aborted");

const throwIfAborted = (signal: AbortSignal): void => {
  if (signal.aborted) throw abortError(signal);
};

const wait = async (ms: number, signal: AbortSignal): Promise<void> => {
  throwIfAborted(signal);
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const cleanup = (): void => signal.removeEventListener("abort", abort);
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    }, ms);
    const abort = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      cleanup();
      reject(abortError(signal));
    };
    signal.addEventListener("abort", abort, { once: true });
  });
};

const raceSignal = async <T>(work: Promise<T>, signal: AbortSignal): Promise<T> => {
  throwIfAborted(signal);
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(abortError(signal));
    signal.addEventListener("abort", abort, { once: true });
    work.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
};

const decode = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};

const record = (value: unknown): Record<string, unknown> | undefined => {
  const decoded = decode(value);
  return decoded !== null && typeof decoded === "object" && !Array.isArray(decoded)
    ? decoded as Record<string, unknown>
    : undefined;
};

const nativeEventSessionID = (value: unknown): string | undefined => {
  const event = record(value);
  if (!event) return undefined;
  const eventType = event.type;
  if (eventType !== "session.created" && eventType !== "step_start" && eventType !== "step_finish" && eventType !== "tool_use" && eventType !== "text") return undefined;
  if (typeof event.sessionID === "string") return event.sessionID;
  const part = record(event.part);
  return typeof part?.sessionID === "string" ? part.sessionID : undefined;
};

const eventIdentity = (value: unknown): Readonly<{ id: string; directory: string; parentID?: string }> | undefined => {
  const event = record(value);
  if (!event) return undefined;
  const nativeSessionID = nativeEventSessionID(event);
  if (nativeSessionID && event.type !== "session.created") return { id: nativeSessionID, directory: "" };
  if (event.type !== "session.created") return undefined;
  const properties = record(event.properties);
  const info = record(properties?.info);
  if (typeof info?.id !== "string" || typeof info.directory !== "string") return undefined;
  return Object.freeze({
    id: info.id,
    directory: info.directory,
    ...(typeof info.parentID === "string" ? { parentID: info.parentID } : {}),
  });
};

const eventHasSessionID = (value: unknown, expected: string): boolean =>
  Array.isArray(value) ? value.some((item) => nativeEventSessionID(item) === expected) : nativeEventSessionID(value) === expected;

type ProcessSnapshot = Readonly<{
  events: readonly unknown[];
  assistantText: string;
  command: readonly string[];
  stderr: string;
  processId?: number;
  identity?: Readonly<{ id: string; directory: string; parentID?: string }>;
  exited: boolean;
  exitCode: number | null;
  signal: NodeJS.Signals | null;
}>;

/** Native `run --format json` emits assistant text as `text` events. Keep this
 * parser deliberately narrow so a user prompt or arbitrary event cannot count
 * as a completed assistant answer. */
export const assistantTextFromNativeEvents = (events: readonly unknown[]): string => events
  .flatMap((event) => {
    const object = record(event);
    if (object?.type !== "text") return [];
    const part = record(object.part);
    const text = typeof part?.text === "string" ? part.text : object.text;
    return typeof text === "string" ? [text] : [];
  })
  .join("");

export const exactAssistantNonce = (
  events: readonly unknown[],
  nonce: string,
): string | undefined => assistantTextFromNativeEvents(events) === nonce ? nonce : undefined;

/** This OpenCode CLI expresses fork lineage through the child session title
 * ("… (fork #N)"); session records carry no session-level parentID for forks
 * (verified natively against the installed CLI). The check stays exact: same
 * directory, distinct id, and the seed's exact title with a fork suffix. */
export const forkSessionLineage = (seed: unknown, child: unknown): boolean => {
  const seedRecord = record(seed);
  const childRecord = record(child);
  if (!seedRecord || !childRecord) return false;
  if (childRecord.id === seedRecord.id) return false;
  if (childRecord.directory !== seedRecord.directory) return false;
  const seedTitle = seedRecord.title;
  const childTitle = childRecord.title;
  if (typeof seedTitle !== "string" || typeof childTitle !== "string" || seedTitle.length === 0) return false;
  return /^ \(fork #\d+\)$/u.test(childTitle.slice(seedTitle.length));
};

/** Authoritative fallback for `--attach` one-shot CLI turns whose stdout event
 * stream can end after the final `step_start` (observed exit 0 with the assistant
 * text persisted server-side only). Reads the session history and requires the
 * most recent assistant message's concatenated text parts to equal the nonce
 * exactly — the same assistant-only strictness as `exactAssistantNonce`. */
export const assistantNonceFromNativeHistory = (history: unknown, nonce: string): string | undefined => {
  const entries = Array.isArray(history) ? history : [];
  for (const entry of [...entries].reverse()) {
    const wrapper = record(entry);
    const info = record(wrapper?.info);
    if (info?.role !== "assistant") continue;
    const parts = Array.isArray(wrapper?.parts) ? wrapper.parts : [];
    const text = parts.flatMap((part) => {
      const value = record(part);
      return value?.type === "text" && typeof value.text === "string" ? [value.text] : [];
    }).join("");
    return text === nonce ? nonce : undefined;
  }
  return undefined;
};

type NativeRuntimeObservation = Readonly<{
  agent: string;
  runtimeId: string;
  epoch?: string;
  raw: Record<string, unknown>;
}>;

const nativeRuntimeObservation = (value: unknown): NativeRuntimeObservation | undefined => {
  const events = Array.isArray(value) ? value : [value];
  for (const eventValue of events) {
    const event = record(eventValue);
    const part = record(event?.part);
    if (event?.type !== "tool_use" || part?.type !== "tool" || part.tool !== "gptqueue_get_runtime_status") continue;
    const state = record(part.state);
    if (state?.status !== "completed") continue;
    const output = record(state.output);
    const structured = record(output?.structuredContent) ?? output;
    const runtime = record(structured?.runtime);
    if (typeof structured?.agent !== "string" || typeof runtime?.runtime_id !== "string") continue;
    return Object.freeze({
      agent: structured.agent,
      runtimeId: runtime.runtime_id,
      ...(typeof runtime.epoch === "string" ? { epoch: runtime.epoch } : {}),
      raw: event,
    });
  }
  return undefined;
};

export const nativeRuntimeObservationFromNativeEvents = nativeRuntimeObservation;

class JsonRun {
  private readonly events: unknown[] = [];
  private buffer = "";
  private stderr = "";
  private exited = false;
  private exitCode: number | null = null;
  private signal: NodeJS.Signals | null = null;
  private readonly completion: Promise<void>;
  public readonly processId: number | undefined;

  public constructor(
    private readonly child: ChildProcess,
    private readonly directory: string,
    public readonly command: readonly string[],
  ) {
    this.processId = child.pid;
    child.stdout?.on("data", (chunk: Buffer) => this.consume(chunk.toString()));
    child.stderr?.on("data", (chunk: Buffer) => { this.stderr = (this.stderr + chunk.toString()).slice(-20_000); });
    this.completion = new Promise<void>((resolve) => child.once("exit", (code, signal) => {
      this.consume(`${this.buffer}\n`);
      this.exited = true;
      this.exitCode = code;
      this.signal = signal;
      resolve();
    }));
    child.once("error", () => { this.exited = true; });
  }

  private consume(text: string): void {
    this.buffer += text;
    const lines = this.buffer.split("\n");
    this.buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      try { this.events.push(JSON.parse(line)); } catch { /* preserve raw process state, not malformed output */ }
    }
  }

  public snapshot(): ProcessSnapshot {
    const identity = this.events.map(eventIdentity).find((value): value is NonNullable<typeof value> => value !== undefined);
    return Object.freeze({ processId: this.processId, command: this.command, stderr: this.stderr, events: Object.freeze([...this.events]), assistantText: assistantTextFromNativeEvents(this.events), identity, exited: this.exited, exitCode: this.exitCode, signal: this.signal });
  }

  public async waitIdentity(signal: AbortSignal, expectedID?: string, expectedParentID?: string, allowKnownAfterExit = false): Promise<NonNullable<ProcessSnapshot["identity"]>> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      throwIfAborted(signal);
      const observedIdentity = expectedID && this.events.some((event) => eventHasSessionID(event, expectedID))
        ? { id: expectedID, directory: this.directory }
        : this.snapshot().identity;
      const identity = observedIdentity?.directory === ""
        ? { ...observedIdentity, directory: this.directory }
        : observedIdentity;
      if (identity && (expectedParentID === undefined || identity.parentID === expectedParentID)) {
        if (identity.directory !== this.directory) throw new Error("OpenCode route session directory does not match launch directory");
        return identity;
      }
      if (allowKnownAfterExit && this.exited && expectedID !== undefined) {
        return { id: expectedID, directory: this.directory };
      }
      if (this.exited) break;
      await wait(100, signal);
    }
    throw new Error("OpenCode route did not emit a native session.created identity");
  }

  public async waitExit(signal: AbortSignal): Promise<void> {
    await raceSignal(this.completion, signal);
    if (this.exitCode !== 0) throw new Error(`OpenCode run exited unsuccessfully: code=${this.exitCode ?? "none"} signal=${this.signal ?? "none"}`);
  }

  public history(signal: AbortSignal): Promise<readonly unknown[]> {
    throwIfAborted(signal);
    return Promise.resolve(Object.freeze([...this.events]));
  }

  public async close(): Promise<void> {
    if (!this.exited) {
      try { if (this.child.pid) process.kill(-this.child.pid, "SIGTERM"); else this.child.kill("SIGTERM"); }
      catch { this.child.kill("SIGTERM"); }
      await Promise.race([this.completion, new Promise<void>((resolve) => setTimeout(resolve, 5_000))]);
      if (!this.exited) {
        try { if (this.child.pid) process.kill(-this.child.pid, "SIGKILL"); else this.child.kill("SIGKILL"); }
        catch { this.child.kill("SIGKILL"); }
        await Promise.race([this.completion, new Promise<void>((resolve) => setTimeout(resolve, 1_000))]);
      }
    }
  }
}

export class OpenCodeLaunchError extends Error {
  public constructor(message: string, public readonly evidence: Readonly<Record<string, unknown>>) {
    super(message);
    this.name = "OpenCodeLaunchError";
  }
}

type RpcMessage = Readonly<{
  jsonrpc?: string;
  id?: number;
  method?: string;
  params?: Record<string, unknown>;
  result?: unknown;
  error?: unknown;
}>;

class AcpRun {
  private readonly notifications: RpcMessage[] = [];
  private readonly historyRows: RpcMessage[] = [];
  private readonly responses = new Map<number, (message: RpcMessage) => void>();
  private buffer = "";
  private stderr = "";
  private nextID = 1;
  private busy = false;
  private exited = false;
  private readonly completion: Promise<void>;
  public readonly processId: number | undefined;

  public constructor(
    private readonly child: ChildProcess,
    private readonly directory: string,
  ) {
    this.processId = child.pid;
    child.stdout?.on("data", (chunk: Buffer) => this.consume(chunk.toString()));
    child.stderr?.on("data", (chunk: Buffer) => { this.stderr = (this.stderr + chunk.toString()).slice(-20_000); });
    this.completion = new Promise<void>((resolve) => {
      child.once("exit", () => { this.exited = true; resolve(); });
      child.once("error", () => { this.exited = true; resolve(); });
    });
  }

  private consume(text: string): void {
    this.buffer += text;
    const lines = this.buffer.split("\n");
    this.buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      let message: RpcMessage;
      try { message = JSON.parse(line) as RpcMessage; } catch { continue; }
      this.historyRows.push(message);
      if (typeof message.id === "number") this.responses.get(message.id)?.(message);
      else if (message.method) this.notifications.push(message);
    }
  }

  public call(method: string, params: Record<string, unknown>, signal: AbortSignal): Promise<RpcMessage> {
    throwIfAborted(signal);
    const id = this.nextID++;
    return new Promise<RpcMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        signal.removeEventListener("abort", abort);
        this.responses.delete(id);
        reject(new Error(`ACP timeout waiting for ${method}`));
      }, timeoutMs);
      const abort = () => { clearTimeout(timer); this.responses.delete(id); reject(abortError(signal)); };
      this.responses.set(id, (message) => {
        clearTimeout(timer); signal.removeEventListener("abort", abort); this.responses.delete(id);
        if (message.error) reject(new Error(`ACP ${method} failed: ${JSON.stringify(message.error)}`)); else resolve(message);
      });
      signal.addEventListener("abort", abort, { once: true });
      this.child.stdin?.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
    });
  }

  public async start(signal: AbortSignal, prompt: string): Promise<Readonly<{ sessionID: string; prompt: Readonly<{ sessionID: string; stopReason: string | undefined; assistantText: string }> }>> {
    await this.call("initialize", { protocolVersion: 1, clientInfo: { name: "gptqueue-qualification", version: "1" }, clientCapabilities: {} }, signal);
    const created = await this.call("session/new", { cwd: this.directory, mcpServers: [] }, signal);
    const session = record(record(created.result));
    if (typeof session?.sessionId !== "string") throw new Error("ACP session/new returned no native session ID");
    return Object.freeze({ sessionID: session.sessionId, prompt: await this.prompt(session.sessionId, prompt, signal) });
  }

  private assistantText(sessionID: string, rows: readonly RpcMessage[]): string {
    return rows.flatMap((message) => {
      const params = record(message.params);
      if (message.method !== "session/update" || params?.sessionId !== sessionID) return [];
      const update = record(params.update);
      const content = record(update?.content);
      return update?.sessionUpdate === "agent_message_chunk" && typeof content?.text === "string" ? [content.text] : [];
    }).join("");
  }

  public async prompt(sessionID: string, text: string, signal: AbortSignal): Promise<Readonly<{ sessionID: string; stopReason: string | undefined; assistantText: string }>> {
    const historyStart = this.historyRows.length;
    this.busy = true;
    try {
      const response = await this.call("session/prompt", { sessionId: sessionID, prompt: [{ type: "text", text }] }, signal);
      const result = record(response.result);
      return Object.freeze({
        sessionID,
        stopReason: typeof result?.stopReason === "string" ? result.stopReason : undefined,
        assistantText: this.assistantText(sessionID, this.historyRows.slice(historyStart)),
      });
    }
    finally { this.busy = false; }
  }

  public status(sessionID: string, signal: AbortSignal): RuntimeStatus {
    throwIfAborted(signal);
    if (this.exited) return { kind: "terminated", runtimeId: sessionID };
    return this.busy
      ? { kind: "busy", runtimeId: sessionID }
      : { kind: "unknown", detail: "ACP does not expose autonomous idle state" };
  }

  public history(signal: AbortSignal): Promise<readonly unknown[]> {
    throwIfAborted(signal);
    return Promise.resolve(Object.freeze([...this.historyRows]));
  }

  public async diagnostic(): Promise<Readonly<Record<string, unknown>>> {
    return Object.freeze({
      process_id: this.processId,
      exited: this.exited,
      stderr: this.stderr,
      history: [...this.historyRows],
    });
  }

  public async close(): Promise<void> {
    if (!this.exited) {
      try { if (this.child.pid) process.kill(-this.child.pid, "SIGTERM"); else this.child.kill("SIGTERM"); }
      catch { this.child.kill("SIGTERM"); }
      await Promise.race([this.completion, new Promise<void>((resolve) => setTimeout(resolve, 5_000))]);
      if (!this.exited) {
        try { if (this.child.pid) process.kill(-this.child.pid, "SIGKILL"); else this.child.kill("SIGKILL"); }
        catch { this.child.kill("SIGKILL"); }
        await Promise.race([this.completion, new Promise<void>((resolve) => setTimeout(resolve, 1_000))]);
      }
    }
  }
}

type OpenCodeProfile = Readonly<{
  env: NodeJS.ProcessEnv;
  paths: readonly string[];
  profileHash: string;
}>;

const privateProfile = (redisUrl: string): OpenCodeProfile => {
  const config = newConfigHome();
  const home = newConfigHome();
  const data = newConfigHome();
  const cache = newConfigHome();
  const state = newConfigHome();
  const configContent = JSON.stringify(repairConfig());
  const effectiveConfig = JSON.stringify({
    configContent,
    HOME: home,
    XDG_CONFIG_HOME: config,
    XDG_DATA_HOME: data,
    XDG_CACHE_HOME: cache,
    XDG_STATE_HOME: state,
    REDIS_URL: redisUrl,
    OPENCODE_PURE: undefined,
    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
    OPENCODE_MODELS_PATH: modelsPath,
  });
  return Object.freeze({
    paths: Object.freeze([config, home, data, cache, state]),
    env: {
      ...process.env,
      HOME: home,
      XDG_CONFIG_HOME: config,
      XDG_DATA_HOME: data,
      XDG_CACHE_HOME: cache,
      XDG_STATE_HOME: state,
      OPENCODE_CONFIG: undefined,
      OPENCODE_CONFIG_DIR: undefined,
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_CONFIG_CONTENT: configContent,
      REDIS_URL: redisUrl,
      OPENCODE_PURE: undefined,
      OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
      OPENCODE_MODELS_PATH: modelsPath,
    },
    profileHash: hash(effectiveConfig),
  });
};

const cleanupPaths = (paths: readonly string[]): void => {
  for (const path of paths) rmSync(path, { recursive: true, force: true });
};

const validateRedisUrl = (redisUrl: string): void => {
  let parsed: URL;
  try { parsed = new URL(redisUrl); } catch { throw new Error("OpenCode qualification requires a valid Redis URL"); }
  if (parsed.protocol !== "redis:" || parsed.hostname !== "127.0.0.1" || parsed.username || parsed.password) {
    throw new Error("OpenCode qualification requires unauthenticated local Redis at 127.0.0.1");
  }
  if (parsed.pathname !== "/15") throw new Error("OpenCode qualification requires Redis database 15");
};

type RegistryBindingEvidence = Readonly<{
  agent: string;
  native: Readonly<{ id: string; directory: string }>;
  registration: Record<string, unknown> | undefined;
  binding: Record<string, unknown> | undefined;
  registrationPresent: boolean;
  bindingPresent: boolean;
}>;

const observeRegistryBinding = async (
  redisUrl: string,
  native: Readonly<{ id: string; directory: string }>,
): Promise<RegistryBindingEvidence> => {
  validateRedisUrl(redisUrl);
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
  const agent = opencodeAgentName(native.id);
  try {
    const registered = await redis.hget(SESSION_KEYS.registry, agent);
    const registration = registered ? record(JSON.parse(registered)) : undefined;
    const metadata = record(registration?.metadata);
    const rawBinding = await redis.get(`gptq:runtime-binding:${agent}`);
    const binding = rawBinding ? record(JSON.parse(rawBinding)) : undefined;
    return Object.freeze({
      agent,
      native,
      registration,
      binding,
      registrationPresent: registration !== undefined && metadata?.working_directory === native.directory,
      bindingPresent: binding !== undefined && binding.client === "opencode" && binding.runtime_id === native.id &&
        typeof binding.epoch === "string" && binding.epoch.length > 0 && binding.working_directory === native.directory,
    });
  } finally {
    await redis.quit();
  }
};

const verifyRegistryBinding = async (
  redisUrl: string,
  native: Readonly<{ id: string; directory: string }>,
): Promise<RegistryBindingEvidence> => {
  const evidence = await observeRegistryBinding(redisUrl, native);
  if (!evidence.registrationPresent) throw new Error("OpenCode registry directory does not match native session");
  if (!evidence.bindingPresent) throw new Error("OpenCode runtime binding does not match native session");
  return evidence;
};

const launchPrompt = (route: RouteId, nonce: string): string => [
  `OpenCode qualification route ${route}; nonce ${nonce}.`,
  "Use only the bound gptqueue_* tools exposed by the local plugin.",
  "Call gptqueue_get_runtime_status and report the structured result exactly; do not infer identity from prose.",
].join(" ");

const spawnRun = (
  directory: string,
  profile: OpenCodeProfile,
  routeArgs: readonly string[],
  prompt: string,
): JsonRun => {
  const command = ["run", ...routeArgs, "--format", "json", "--model", model, "--agent", "build", "--dir", directory, "--auto", prompt];
  const child = spawn(opencodeBin, command, {
    cwd: directory,
    env: profile.env,
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
  });
  return new JsonRun(child, directory, Object.freeze([...command]));
};

const identityOf = (
  route: RouteId,
  native: Readonly<{ id: string; directory: string; parentID?: string }>,
  profile: Readonly<{ profileHash: string }>,
  epoch = native.id,
): ParticipantIdentity => Object.freeze({
  participantId: `${route}:${native.id}`,
  route,
  hostRuntimeId: native.id,
  agent: `gptqueue-opencode-${native.id}`,
  cwdHash: hash(native.directory),
  profileHash: profile.profileHash,
  epochHash: hash(epoch),
});

const launchHeadlessRunParticipant = async (
  input: Readonly<{ nonce: string; redisUrl: string; pairId: string; role: "sender" | "receiver" }>,
  signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  validateRedisUrl(input.redisUrl);
  const prompt = [
    "You are a long-lived OpenCode GPTQueue qualification actor.",
    "Use only the bound gptqueue_* tools exposed by the local plugin; do not use shell, files, web, direct MCP, or another process.",
    "Call gptqueue_get_runtime_status exactly once and retain its structured result.",
    "Then repeatedly call gptqueue_receive_message with timeout 60.",
    "Only messages whose metadata has qualification_control=true are controller instructions.",
    "For each such task, perform the requested work, send exactly one result to its sender with in_reply_to set to the task id, then call gptqueue_receive_message again.",
    "Do not exit while waiting. Control traffic is excluded from peer qualification evidence.",
  ].join(" ");
  let profile: OpenCodeProfile | undefined;
  let directory: string | undefined;
  let control: RedisClient | undefined;
  let controllerName = "";
  let run: JsonRun | undefined;
  let registered = false;
  let closed = false;
  const cleanupFailures: string[] = [];
  const controlExchanges: Array<Readonly<Record<string, unknown>>> = [];
  try {
    profile = privateProfile(input.redisUrl);
    directory = newWorkDir();
    control = new RedisClient(null, input.redisUrl);
    const actorProfile = profile;
    const actorDirectory = directory;
    const actorControl = control;
    controllerName = `opencode-run-control-${randomUUID()}`;
    const actor = spawnRun(actorDirectory, actorProfile, [], prompt);
    run = actor;
    await actorControl.register("both", controllerName, "OpenCode run qualification control channel", {
      label: "qualification-control", uuid: null, client: null, working_directory: actorDirectory,
    });
    registered = true;
    const native = await actor.waitIdentity(signal);
    const runtimeDeadline = Date.now() + timeoutMs;
    let runtime: NativeRuntimeObservation | undefined;
    let binding: RegistryBindingEvidence | undefined;
    while (Date.now() < runtimeDeadline) {
      throwIfAborted(signal);
      runtime = nativeRuntimeObservation(actor.snapshot().events);
      if (runtime && runtime.runtimeId === native.id && runtime.agent === opencodeAgentName(native.id)) {
        try {
          binding = await verifyRegistryBinding(input.redisUrl, native);
          break;
        } catch {
          // Registration can lag the first native runtime probe.
        }
      }
      if (actor.snapshot().exited) throw new Error("OpenCode run actor exited before exact runtime binding");
      await wait(100, signal);
    }
    if (!runtime || !binding) throw new Error("OpenCode run actor did not expose exact runtime status and registry binding");
    const runtimeEpoch = `${actor.processId ?? "unknown"}:${String(binding.binding?.epoch ?? runtime.epoch ?? "missing")}`;
    const identity = identityOf("opencode-run", native, profile, runtimeEpoch);
    const receiveControlReply = async (messageID: string, promptSignal: AbortSignal): Promise<QueueMessage> => {
      const deadline = Date.now() + 180_000;
      while (Date.now() < deadline) {
        throwIfAborted(promptSignal);
        const reply = await actorControl.receiveMessage(30, promptSignal);
        if (reply?.from === identity.agent && reply.to === controllerName && reply.payload.in_reply_to === messageID) return reply;
      }
      throw new Error(`OpenCode run actor did not reply to control task ${messageID}`);
    };
    const close = async (): Promise<void> => {
      if (closed) return;
      const failures: unknown[] = [];
      try { await actor.close(); } catch (error) { failures.push(error); cleanupFailures.push(`run:${actor.processId ?? "unknown"}: ${String(error)}`); }
      try { if (registered) await actorControl.unregister(); } catch (error) { failures.push(error); cleanupFailures.push(`controller_unregister: ${String(error)}`); }
      try { await actorControl.shutdown(); } catch (error) { failures.push(error); cleanupFailures.push(`controller_shutdown: ${String(error)}`); }
      try { cleanupPaths(actorProfile.paths); } catch (error) { failures.push(error); cleanupFailures.push(`profile: ${String(error)}`); }
      try { rmSync(actorDirectory, { recursive: true, force: true }); } catch (error) { failures.push(error); cleanupFailures.push(`directory: ${String(error)}`); }
      if (failures.length > 0) throw new AggregateError(failures, "OpenCode run actor cleanup failed");
      closed = true;
    };
    return Object.freeze({
      kind: "model" as const,
      identity,
      prompt: async (text: string, promptSignal: AbortSignal): Promise<unknown> => {
        throwIfAborted(promptSignal);
        if (closed) throw new Error("OpenCode run actor is closed");
        const task: QueueMessage = {
          id: randomUUID(), from: controllerName, to: identity.agent, timestamp: new Date().toISOString(), type: "task",
          payload: { content: text, metadata: { qualification_control: true, nonce: input.nonce } },
        };
        const sent = await actorControl.sendMessageIdempotent(task, `${input.pairId}:${input.role}:${input.nonce}:${task.id}`);
        if (sent.status === "full") throw new Error(`OpenCode run control mailbox was full for ${identity.agent}`);
        const reply = await receiveControlReply(sent.messageId, promptSignal);
        controlExchanges.push(Object.freeze({ task, reply }));
        return reply;
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        throwIfAborted(statusSignal);
        if (closed || actor.snapshot().exited) return { kind: "terminated", runtimeId: native.id };
        return { kind: "unknown", detail: "OpenCode run actor is waiting through GPTQueue control tasks; idle is not observable" };
      },
      history: (historySignal: AbortSignal) => actor.history(historySignal),
      close,
      evidence: async () => Object.freeze({
        route: "opencode-run", native_session_id: native.id, directory: actorDirectory, process_id: actor.processId,
        native_cli_args: actor.command,
        process_exit: actor.snapshot().exited ? { code: actor.snapshot().exitCode, signal: actor.snapshot().signal } : { state: "running" },
        runtime_epoch: runtimeEpoch, native_runtime_observation: runtime, registry_binding: binding,
        control_identity: controllerName, control_traffic_excluded: true, native_history: actor.snapshot(),
        control_exchanges: Object.freeze([...controlExchanges]),
        cleanup_failure_history: Object.freeze([...cleanupFailures]),
      }),
      get ownedProcessIds(): readonly number[] { return Object.freeze(actor.processId === undefined ? [] : [actor.processId]); },
      get cleanupFailureHistory(): readonly string[] { return Object.freeze([...cleanupFailures]); },
    });
  } catch (error) {
    const failures: unknown[] = [error];
    const preCleanupSnapshot = run?.snapshot();
    const preCleanupOwnedProcessIds = run?.processId === undefined ? [] : [run.processId];
    await run?.close().catch((cleanupError) => failures.push(cleanupError));
    if (registered && control) await control.unregister().catch((cleanupError) => failures.push(cleanupError));
    await control?.shutdown().catch((cleanupError) => failures.push(cleanupError));
    try { if (profile) cleanupPaths(profile.paths); }
    catch (cleanupError) { failures.push(cleanupError); }
    try { if (directory) rmSync(directory, { recursive: true, force: true }); }
    catch (cleanupError) { failures.push(cleanupError); }
    const evidence = Object.freeze({
      route: "opencode-run",
      phase: "launch_failure_before_cleanup",
      native_snapshot: preCleanupSnapshot,
      owned_process_ids: Object.freeze(preCleanupOwnedProcessIds),
      cleanup_failure_history: Object.freeze([...cleanupFailures]),
      cleanup_errors: Object.freeze(failures.slice(1).map(String)),
    });
    if (failures.length > 1) throw new OpenCodeLaunchError(`OpenCode run actor launch failed: ${String(error)}`, evidence);
    throw new OpenCodeLaunchError(String(error), evidence);
  }
};

const launchResumeParticipant = async (
  input: Readonly<{ nonce: string; redisUrl: string }>,
  signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  return launchPersistentCliParticipant("opencode-resume", input, signal);
};

const launchForkParticipant = async (
  input: Readonly<{ nonce: string; redisUrl: string }>,
  signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  return launchPersistentCliParticipant("opencode-fork", input, signal);
};

type EvidenceParticipant = ModelParticipant & Readonly<{
  evidence: () => Promise<Readonly<Record<string, unknown>>>;
  ownedProcessIds: readonly number[];
  cleanupFailureHistory?: readonly string[];
}>;

const serverParticipant = (
  route: RouteId,
  server: RepairOpenCodeServer,
  session: RepairSession,
  profileHash: string,
  ownedProcessIds: readonly number[],
  hostClose: () => Promise<void>,
  evidence: () => Promise<Readonly<Record<string, unknown>>>,
  epoch = server.processId === undefined ? `${session.id}:unknown` : `${server.processId}`,
): EvidenceParticipant => {
  const identity = identityOf(route, { id: session.id, directory: server.directory }, { profileHash }, epoch);
  return Object.freeze({
    kind: "model" as const,
    identity,
    prompt: async (text: string, signal: AbortSignal) => { await raceSignal(session.prompt(text), signal); return text; },
    status: async (signal: AbortSignal): Promise<RuntimeStatus> => {
      const value = record(await raceSignal(session.status(), signal));
      const data = record(value?.data) ?? value;
      const entry = record(data?.[session.id]);
      if (!entry) return { kind: "idle", runtimeId: session.id };
      const type = entry.type;
      if (type === "busy" || type === "idle") return { kind: type, runtimeId: session.id };
      return { kind: "unknown", detail: "OpenCode returned an unknown serve status" };
    },
    history: (signal: AbortSignal) => raceSignal(session.history(), signal),
    close: async () => { await hostClose(); await session.close().catch(() => undefined); await server.close(); },
    evidence,
    ownedProcessIds: Object.freeze([...ownedProcessIds]),
  });
};

/**
 * Resume and fork use the native CLI only to establish route provenance. The
 * owned serve process remains the participant owner after that CLI turn exits;
 * subsequent turns are attached CLI invocations while the server keeps the
 * exact session registration addressable between prompts.
 */
const launchPersistentCliParticipant = async (
  route: "opencode-resume" | "opencode-fork",
  input: Readonly<{ nonce: string; redisUrl: string }>,
  signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  validateRedisUrl(input.redisUrl);
  const profile = privateProfile(input.redisUrl);
  let server: RepairOpenCodeServer | undefined;
  let seed: RepairSession | undefined;
  let session: RepairSession | undefined;
  const runs: JsonRun[] = [];
  const cleanupFailures: string[] = [];
  try {
    server = await startRepairOpenCodeServer(input.redisUrl);
    if (server.processId === undefined) throw new Error("OpenCode persistent server did not expose an owned process ID");
    seed = await server.session();
    const seedRecord = record(await seed.record());
    if (seedRecord?.id !== seed.id || seedRecord.directory !== server.directory) {
      throw new Error("OpenCode persistent lifecycle seed identity mismatch");
    }
    const initialArgs = ["--attach", server.baseUrl, "--session", seed.id, ...(route === "opencode-fork" ? ["--fork"] : [])];
    const initialRun = spawnRun(server.directory, profile, initialArgs, launchPrompt(route, input.nonce));
    runs.push(initialRun);
    const native = await initialRun.waitIdentity(signal, route === "opencode-resume" ? seed.id : undefined);
    if (native.directory !== server.directory || (route === "opencode-resume" && native.id !== seed.id)) {
      throw new Error(`OpenCode ${route} CLI invocation did not preserve exact native lineage`);
    }
    await initialRun.waitExit(signal);
    const initialSnapshot = initialRun.snapshot();
    session = await server.session(native.id);
    const childRecord = record(await session.record());
    if (childRecord?.id !== native.id || childRecord.directory !== server.directory ||
        (route === "opencode-fork" && !forkSessionLineage(seedRecord, childRecord))) {
      throw new Error(`OpenCode ${route} persistent session record failed exact lineage verification`);
    }
    const bindingAfterCliExit = await verifyRegistryBinding(input.redisUrl, native);
    const parentBindingAfterCliExit = await verifyRegistryBinding(input.redisUrl, { id: seed.id, directory: server.directory });
    const runtimeEpoch = `${server.processId}:${String(bindingAfterCliExit.binding?.epoch ?? "missing")}`;
    let closed = false;
    let active: JsonRun | undefined;
    const identity = identityOf(route, { id: native.id, directory: server.directory, ...(native.parentID ? { parentID: native.parentID } : {}) }, { profileHash: server.profileHash }, runtimeEpoch);
    const cleanup = async (): Promise<void> => {
      if (closed) return;
      const failures: unknown[] = [];
      for (const run of [...runs].reverse()) {
        try { await run.close(); }
        catch (error) { failures.push(error); cleanupFailures.push(`run:${run.processId ?? "unknown"}: ${String(error)}`); }
      }
      if (session) {
        try { await session.close(); }
        catch (error) { failures.push(error); cleanupFailures.push(`session:${session.id}: ${String(error)}`); }
      }
      if (server) {
        try { await server.close(); }
        catch (error) { failures.push(error); cleanupFailures.push(`server:${server.processId ?? "unknown"}: ${String(error)}`); }
      }
      try { cleanupPaths(profile.paths); }
      catch (error) { failures.push(error); cleanupFailures.push(`profile: ${String(error)}`); }
      if (failures.length > 0) throw new AggregateError(failures, `OpenCode ${route} participant cleanup failed`);
      closed = true;
    };
    const participant = {
      kind: "model" as const,
      identity,
      prompt: async (text: string, promptSignal: AbortSignal): Promise<unknown> => {
        throwIfAborted(promptSignal);
        if (closed) throw new Error(`OpenCode ${route} participant is closed`);
        if (active && !active.snapshot().exited) throw new Error(`OpenCode ${route} participant is already busy`);
        const run = spawnRun(server!.directory, profile, ["--attach", server!.baseUrl, "--session", session!.id], text);
        active = run;
        runs.push(run);
        try {
          const continued = await run.waitIdentity(promptSignal, session!.id, undefined, true);
          if (continued.id !== session!.id || continued.directory !== server!.directory) {
            throw new Error(`OpenCode ${route} continuation changed the native session identity`);
          }
          await run.waitExit(promptSignal);
          const snapshot = run.snapshot();
          const expectedNonce = /^Reply with this exact nonce and no other text:\s*(.+)$/u.exec(text)?.[1];
          let assistantText = snapshot.assistantText;
          let assistantNonceSource: "cli_stdout" | "native_session_history" | undefined;
          if (expectedNonce !== undefined) {
            if (exactAssistantNonce(snapshot.events, expectedNonce) !== undefined) {
              assistantNonceSource = "cli_stdout";
            } else {
              // `run --attach` CLI turns can exit (code 0) after the final
              // step_start, losing the trailing text events on stdout while the
              // completed assistant message is already persisted server-side.
              // The exact assistant-only requirement is unchanged; only the
              // observation source falls back to the authoritative native
              // session history, and the source is recorded for provenance.
              const nativeNonce = assistantNonceFromNativeHistory(await session!.history(), expectedNonce);
              if (nativeNonce === undefined) throw new Error(`OpenCode ${route} completed CLI turn did not produce the exact assistant-only nonce`);
              assistantText = nativeNonce;
              assistantNonceSource = "native_session_history";
            }
          }
          const binding = await verifyRegistryBinding(input.redisUrl, { id: session!.id, directory: server!.directory });
          return Object.freeze({ route, native: snapshot, assistantText, assistant_nonce_source: assistantNonceSource, assistantOnlyNonce: expectedNonce, sessionID: session!.id, binding_after_cli_exit: binding });
        } finally {
          active = undefined;
        }
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        throwIfAborted(statusSignal);
        if (closed) return { kind: "terminated", runtimeId: session!.id };
        if (active && !active.snapshot().exited) return { kind: "busy", runtimeId: session!.id };
        const value = record(await raceSignal(session!.status(), statusSignal));
        const data = record(value?.data) ?? value;
        const entry = record(data?.[session!.id]);
        if (!entry) return { kind: "idle", runtimeId: session!.id };
        if (entry.type === "busy" || entry.type === "idle") return { kind: entry.type, runtimeId: session!.id };
        return { kind: "unknown", detail: "OpenCode returned an unknown persistent session status" };
      },
      history: (historySignal: AbortSignal) => raceSignal(session!.history(), historySignal),
      close: cleanup,
      evidence: async () => {
        const nativeHistory = await session!.history();
        return Object.freeze({
          route,
          server_process_id: server!.processId,
          runtime_epoch: runtimeEpoch,
          native_tools: server!.toolIds,
          seed: { record: await seed!.record(), history: await seed!.history() },
          native_session: {
            record: await session!.record(),
            history: nativeHistory,
            status_while_cli_exited: await session!.status(),
          },
          runtime_status_probe_observed: hasRuntimeStatus(nativeHistory, opencodeAgentName(session!.id), session!.id),
          cli_invocations: runs.map((run) => run.snapshot()),
          initial_cli_args: initialArgs,
          initial_cli_snapshot: initialSnapshot,
          binding_after_initial_cli_exit: bindingAfterCliExit,
          parent_binding_after_initial_cli_exit: parentBindingAfterCliExit,
          peer_reachable_while_cli_exited: bindingAfterCliExit.bindingPresent && parentBindingAfterCliExit.bindingPresent,
          cleanup_failure_history: Object.freeze([...cleanupFailures]),
        });
      },
      get ownedProcessIds(): readonly number[] {
        return Object.freeze([server!.processId!, ...runs.flatMap((run) => run.processId === undefined ? [] : [run.processId])]);
      },
      get cleanupFailureHistory(): readonly string[] {
        return Object.freeze([...cleanupFailures]);
      },
    } satisfies EvidenceParticipant;
    return Object.freeze(participant);
  } catch (error) {
    const failures: unknown[] = [error];
    for (const run of [...runs].reverse()) await run.close().catch((cleanupError) => failures.push(cleanupError));
    if (session) await session.close().catch((cleanupError) => failures.push(cleanupError));
    if (server) await server.close().catch((cleanupError) => failures.push(cleanupError));
    cleanupPaths(profile.paths);
    if (failures.length > 1) throw new AggregateError(failures, `OpenCode ${route} persistent lifecycle launch failed`);
    throw error;
  }
};

class AcpQualificationError extends Error {
  public constructor(message: string, public readonly evidence: Readonly<Record<string, unknown>>) {
    super(message);
    this.name = "AcpQualificationError";
  }
}

const launchAcp = async (
  input: Readonly<{ nonce: string; redisUrl: string }>,
  signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  const profile = privateProfile(input.redisUrl);
  const directory = newWorkDir();
  const child = spawn(opencodeBin, ["acp", "--cwd", directory], {
    cwd: directory,
    env: profile.env,
    stdio: ["pipe", "pipe", "pipe"],
    detached: true,
  });
  const acp = new AcpRun(child, directory);
  try {
    const started = await acp.start(signal, launchPrompt("opencode-acp", input.nonce));
    const sessionID = started.sessionID;
    await verifyRegistryBinding(input.redisUrl, { id: sessionID, directory });
    const identity = identityOf("opencode-acp", { id: sessionID, directory }, profile);
    const initialPrompt = started.prompt;
    return Object.freeze({
      kind: "model" as const,
      identity,
      prompt: async (text: string, promptSignal: AbortSignal) => acp.prompt(sessionID, text, promptSignal),
      status: async (statusSignal: AbortSignal) => acp.status(sessionID, statusSignal),
      history: (historySignal: AbortSignal) => acp.history(historySignal),
      close: async () => { await acp.close(); cleanupPaths(profile.paths); rmSync(directory, { recursive: true, force: true }); },
      evidence: async () => Object.freeze({
        route: "opencode-acp",
        native_session_id: sessionID,
        directory,
        initial_prompt: initialPrompt,
        history: await acp.history(new AbortController().signal),
      }),
      ownedProcessIds: Object.freeze(acp.processId === undefined ? [] : [acp.processId]),
    });
  } catch (error) {
    const evidence = await acp.diagnostic();
    await acp.close(); cleanupPaths(profile.paths); rmSync(directory, { recursive: true, force: true });
    throw new AcpQualificationError(String(error), evidence);
  }
};

const arrayRecords = (value: unknown): readonly Record<string, unknown>[] => {
  if (Array.isArray(value)) return value.map(record).filter((item): item is Record<string, unknown> => item !== undefined);
  const object = record(value);
  for (const key of ["sessions", "data", "items"]) {
    if (object?.[key] !== undefined) return arrayRecords(object[key]);
  }
  return [];
};

const hasCompletedTask = (value: unknown): boolean => {
  if (Array.isArray(value)) return value.some(hasCompletedTask);
  const object = record(value);
  if (!object) return false;
  const state = record(object.state);
  if (object.tool === "task" && state?.status === "completed" && state.error === undefined) return true;
  return Object.values(object).some(hasCompletedTask);
};

const hasRuntimeStatus = (value: unknown, agent: string, runtimeID: string): boolean => {
  const completedPart = (partValue: unknown): boolean => {
    const part = record(partValue);
    if (part?.type !== "tool" || part.tool !== "gptqueue_get_runtime_status") return false;
    const state = record(part.state);
    const output = record(state?.output);
    const structured = record(output?.structuredContent) ?? output;
    const runtime = record(structured?.runtime);
    return state?.status === "completed" && structured?.agent === agent && runtime?.runtime_id === runtimeID;
  };
  if (Array.isArray(value)) return value.some((item) => hasRuntimeStatus(item, agent, runtimeID));
  const object = record(value);
  if (!object) return false;
  if (object.type === "tool_use" && completedPart(object.part)) return true;
  if (Array.isArray(object.parts)) return object.parts.some((part) => completedPart(part));
  return false;
};

class NativeTaskQualificationError extends Error {
  public constructor(message: string, public readonly evidence: Readonly<Record<string, unknown>>) {
    super(message);
    this.name = "NativeTaskQualificationError";
  }
}

const waitForNativeChild = async (
  server: RepairOpenCodeServer,
  parentID: string,
  signal: AbortSignal,
): Promise<Record<string, unknown>> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    throwIfAborted(signal);
    const child = arrayRecords(await server.sessions()).find((item) =>
      item.id !== parentID && item.parentID === parentID && typeof item.directory === "string");
    if (child) return child;
    await wait(250, signal);
  }
  throw new Error("OpenCode native Task did not expose a child session with the exact parentID");
};

const launchNativeTask = async (
  input: Readonly<{ nonce: string; redisUrl: string }>,
  signal: AbortSignal,
): Promise<ModelParticipant> => {
  validateRedisUrl(input.redisUrl);
  let server: RepairOpenCodeServer | undefined;
  let parent: RepairSession | undefined;
  let child: RepairSession | undefined;
  const observe = async (read: () => Promise<unknown>): Promise<unknown> => read().catch((error) => ({ read_error: String(error) }));
  const diagnostics = async (phase: string, extra: Readonly<Record<string, unknown>> = {}): Promise<Readonly<Record<string, unknown>>> => {
    const parentSession = parent;
    const childSession = child;
    const serverInstance = server;
    const parentID = parentSession?.id;
    const childID = childSession?.id;
    const [parentRecord, parentHistory, parentStatus, childRecord, childHistory, childStatus, sessions] = await Promise.all([
      parentSession ? observe(() => parentSession.record()) : Promise.resolve(undefined),
      parentSession ? observe(() => parentSession.history()) : Promise.resolve(undefined),
      parentSession ? observe(() => parentSession.status()) : Promise.resolve(undefined),
      childSession ? observe(() => childSession.record()) : Promise.resolve(undefined),
      childSession ? observe(() => childSession.history()) : Promise.resolve(undefined),
      childSession ? observe(() => childSession.status()) : Promise.resolve(undefined),
      serverInstance ? observe(() => serverInstance.sessions()) : Promise.resolve(undefined),
    ]);
    return Object.freeze({
      phase,
      ...extra,
      parent: { id: parentID, record: parentRecord, history: parentHistory, status: parentStatus },
      child: { id: childID, record: childRecord, history: childHistory, status: childStatus },
      sessions,
      owned_process_id: serverInstance?.processId,
      task_result_observed: hasCompletedTask(parentHistory),
      ...(childID ? { child_runtime_status_observed: hasRuntimeStatus(childHistory, opencodeAgentName(childID), childID) } : {}),
    });
  };
  const fail = async (message: string, phase: string, extra: Readonly<Record<string, unknown>> = {}): Promise<never> => {
    throw new NativeTaskQualificationError(message, await diagnostics(phase, extra));
  };
  try {
    server = await startRepairOpenCodeServer(input.redisUrl);
    const serverInstance = server;
    const parentSession = await serverInstance.session();
    parent = parentSession;
    const parentRecord = record(await parentSession.record());
    if (parentRecord?.id !== parentSession.id || parentRecord.directory !== serverInstance.directory) {
      await fail("OpenCode native Task parent identity mismatch", "parent_identity", { expected_directory: serverInstance.directory });
    }
    await parentSession.prompt([
      "Use only the locally installed GPTQueue plugin tools and the native Task tool. Do not use shell, files, direct MCP servers, web, or another OpenCode process.",
      "GPTQueue is already bound to this exact OpenCode session; never register an agent or provide session credentials.",
      "Invoke native Task exactly once with a general-purpose child. The child prompt must call gptqueue_get_runtime_status and gptqueue_list_agents once, report CHILD_READY and its exact bound gptqueue-opencode name, then finish. This first turn creates the child only; do not send or claim a GPTQueue task.",
    ].join(" "));
    const parentHistory = await parentSession.history();
    if (!hasCompletedTask(parentHistory)) {
      await fail("OpenCode native Task did not produce a completed native Task tool result", "parent_task_result");
    }
    const childRecord = await waitForNativeChild(serverInstance, parentSession.id, signal);
    const childID = typeof childRecord.id === "string" ? childRecord.id : "";
    const childDirectory = typeof childRecord.directory === "string" ? childRecord.directory : "";
    if (!childID || !childDirectory || childRecord.parentID !== parentSession.id) {
      await fail("OpenCode native Task child identity is not bound to the parent", "child_lineage", { child_record: childRecord });
    }
    const childSession = await serverInstance.session(childID);
    child = childSession;
    const verifiedChild = record(await childSession.record());
    if (verifiedChild?.id !== childID || verifiedChild.directory !== serverInstance.directory || verifiedChild.parentID !== parentSession.id) {
      await fail("OpenCode native Task child record failed exact parent/directory verification", "child_identity", { child_record: verifiedChild });
    }
    const childHistory = await childSession.history();
    const childAgent = opencodeAgentName(childID);
    if (!hasRuntimeStatus(childHistory, childAgent, childID)) {
      await fail("OpenCode native Task child has no exact runtime status proof", "child_runtime_status");
    }
    if (!childReadinessEvidence(childHistory, childAgent, childID)) {
      await fail("OpenCode native Task child has no exact assistant readiness marker", "child_readiness");
    }
    try {
      await verifyRegistryBinding(input.redisUrl, { id: parentSession.id, directory: serverInstance.directory });
      await verifyRegistryBinding(input.redisUrl, { id: childID, directory: serverInstance.directory });
    } catch (error) {
      await fail("OpenCode native Task registry binding verification failed", "registry_binding", { binding_error: String(error) });
    }
    const parentAfter = record(await parentSession.record());
    if (parentAfter?.id !== parentSession.id || parentAfter.directory !== serverInstance.directory) {
      await fail("OpenCode native Task parent identity changed", "parent_preservation", { parent_after: parentAfter });
    }
    return serverParticipant("opencode-native-task", serverInstance, childSession, serverInstance.profileHash, serverInstance.processId ? [serverInstance.processId] : [], async () => undefined, async () => {
      const [parentRecordAfter, parentHistoryAfter, childRecordAfter, childHistoryAfter] = await Promise.all([
        parentSession.record(), parentSession.history(), childSession.record(), childSession.history(),
      ]);
      return Object.freeze({
        route: "opencode-native-task",
        parent: { record: parentRecordAfter, history: parentHistoryAfter },
        child: { record: childRecordAfter, history: childHistoryAfter },
        task_result_observed: hasCompletedTask(parentHistoryAfter),
        child_runtime_status_observed: hasRuntimeStatus(childHistoryAfter, childAgent, childID),
      });
    });
  } catch (error) {
    if (!(error instanceof NativeTaskQualificationError)) {
      error = new NativeTaskQualificationError(String(error), await diagnostics("exception"));
    }
    await server?.close().catch(() => undefined);
    throw error;
  }
};

const preflight = async (route: RouteId, signal: AbortSignal): Promise<Availability> => {
  throwIfAborted(signal);
  const prerequisites = opencodePrerequisites(existsSync, { binary: opencodeBin, models: modelsPath, plugin: repairPluginPath });
  if (prerequisites.kind === "unavailable") return { kind: "blocked_prerequisite", detail: prerequisites.detail };
  // A missing build is a failure of this checkout, never a skippable prerequisite.
  if (prerequisites.kind === "build_missing") throw new Error(prerequisites.detail);
  if (route === "opencode-interactive") return { kind: "setup_gap", detail: "PTY participant control and native history correlation are not implemented" };
  return { kind: "available" };
};

const spec = (id: RouteId): RouteSpec => Object.freeze({ id, host: "opencode", modelBacked: true, availability: { kind: "setup_gap" as const, detail: "preflight not run" } });

const adapter = (id: RouteId): RouteAdapter => Object.freeze({
  spec: spec(id),
  preflight: (signal) => preflight(id, signal),
  launch: async (input, signal) => {
    throwIfAborted(signal);
    if (id === "opencode-interactive") throw new Error("OpenCode interactive route requires a supported native identity/history observer");
    if (id === "opencode-run") return launchHeadlessRunParticipant(input, signal);
    if (id === "opencode-fork") return launchForkParticipant(input, signal);
    if (id === "opencode-resume") return launchResumeParticipant(input, signal);
    if (id === "opencode-native-task") return launchNativeTask(input, signal);
    if (id === "opencode-acp") return launchAcp(input, signal);
    if (id === "opencode-serve-attach") {
      validateRedisUrl(input.redisUrl);
      const profile = privateProfile(input.redisUrl);
      let server: RepairOpenCodeServer | undefined;
      let run: JsonRun | undefined;
      try {
        server = await startRepairOpenCodeServer(input.redisUrl);
        const session = await server.session();
        const knownRecord = record(await session.record());
        if (knownRecord?.id !== session.id || knownRecord.directory !== server.directory) throw new Error("OpenCode serve session identity mismatch");
        run = spawnRun(server.directory, profile, ["--attach", server.baseUrl, "--session", session.id], launchPrompt(id, input.nonce));
        const native = await run.waitIdentity(signal, session.id);
        await run.waitExit(signal);
        const sessionRecord = record(await session.record());
        if (native.id !== session.id || sessionRecord?.id !== session.id || sessionRecord.directory !== server.directory) throw new Error("OpenCode serve attach changed the native session identity");
        await verifyRegistryBinding(input.redisUrl, native);
        const participant = serverParticipant(id, server, session, server.profileHash, server.processId ? [server.processId] : [], async () => { await run?.close(); cleanupPaths(profile.paths); }, async () => Object.freeze({
          route: id,
          native_run: run?.snapshot(),
          session: { record: await session.record(), history: await session.history() },
        }));
        return participant;
      } catch (error) {
        await run?.close().catch(() => undefined);
        await server?.close().catch(() => undefined); cleanupPaths(profile.paths); throw error;
      }
    }
    throw new Error(`OpenCode route ${id} is not enabled in this bounded adapter slice`);
  },
});

export const opencodeRouteAdapters: readonly RouteAdapter[] = Object.freeze(openCodeRoutes.map(adapter));
export const opencodeRouteAdapter = (id: RouteId): RouteAdapter | undefined => opencodeRouteAdapters.find((item) => item.spec.id === id);
export const opencodeRouteIds: readonly RouteId[] = Object.freeze([...openCodeRoutes]);
export const opencodeRouteSourceRevision = "3f2e8f4d12c719117ba9e0ffa19aaffe7a460af9";
