import { access, mkdir, mkdtemp, rm } from "node:fs/promises";
import { constants, existsSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { homedir, tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import * as pty from "node-pty";
import { appConfig, repo, type Json } from "./codex-support.js";
import { CodexSocketClient } from "../../src/registered-shell/codex-socket.js";
import { readCodexAppserverHistory } from "./codex-appserver-history.js";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import type { QueueMessage } from "../../src/mcp-server/types.js";
import type { Availability, ModelParticipant, Participant, RouteAdapter, RouteId, RouteSpec, RuntimeStatus } from "./qualification-types.js";

export const codexRouteIds = Object.freeze([
  "codex-appserver", "codex-interactive", "codex-headless", "codex-native-child", "codex-fork", "codex-resume",
] as const);
export type CodexRouteId = (typeof codexRouteIds)[number];
export type CodexParticipant = ModelParticipant & Readonly<{ provenance: Readonly<{ model: string; socketHash: string; configHash: string; runtimeId: string }> }>;
type LaunchInput = Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>;
type JsonObject = Record<string, unknown>;
type ProcessResult = Readonly<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }>;
type RuntimeObservation = Readonly<{ agent: string; runtimeId: string; epoch: string; raw: JsonObject }>;
type CodexHost = Readonly<{ ensure: (redisUrl: string, signal: AbortSignal) => Promise<HostSession>; close: () => Promise<void> }>;
type HostSession = Readonly<{ rpc: CodexSocketClient; socket: string; config: Json; configHash: string; model: string; close: () => Promise<void> }>;
type CodexOptions = Readonly<{
  codexBin?: string;
  model?: string;
  workspaceRoot?: string;
  host?: CodexHost;
  config?: Json;
  tuiTrustRoot?: string;
}>;

const DEFAULT_CODEX = "/home/rahul/.local/bin/codex";
const model = (options: CodexOptions): string => options.model ?? process.env.GPTQUEUE_CODEX_MODEL ?? "gpt-5.6-luna";
const object = (value: unknown): JsonObject | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const hashPath = (value: string): string => hash(resolve(value));
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("Codex qualification operation aborted");
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw abortError(signal); };
const delay = (ms: number, signal: AbortSignal): Promise<void> => new Promise((resolveDelay, reject) => {
  checkAbort(signal);
  const timer = setTimeout(resolveDelay, ms);
  const abort = () => { clearTimeout(timer); reject(abortError(signal)); };
  signal.addEventListener("abort", abort, { once: true });
  setTimeout(() => signal.removeEventListener("abort", abort), ms + 1);
});

const withAbort = <T>(promise: Promise<T>, signal: AbortSignal): Promise<T> => new Promise<T>((resolvePromise, reject) => {
  if (signal.aborted) { reject(abortError(signal)); return; }
  const abort = () => reject(abortError(signal));
  signal.addEventListener("abort", abort, { once: true });
  promise.then(value => { signal.removeEventListener("abort", abort); resolvePromise(value); }, error => { signal.removeEventListener("abort", abort); reject(error); });
});

const processStatus = (child: ChildProcess): string => child.exitCode === null && child.signalCode === null ? "running" : "exited";
const waitForExit = async (child: ChildProcess, timeoutMs: number): Promise<boolean> => {
  if (processStatus(child) !== "running") return true;
  return new Promise<boolean>(resolveExit => {
    const timer = setTimeout(() => { child.removeListener("close", onClose); resolveExit(processStatus(child) !== "running"); }, timeoutMs);
    const onClose = () => { clearTimeout(timer); resolveExit(true); };
    child.once("close", onClose);
  });
};
const signalProcessGroup = (child: ChildProcess, signal: NodeJS.Signals): void => {
  try { if (child.pid) process.kill(-child.pid, signal); else child.kill(signal); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
};
const stopProcess = async (child: ChildProcess | undefined): Promise<void> => {
  if (!child || processStatus(child) !== "running") return;
  signalProcessGroup(child, "SIGTERM");
  if (await waitForExit(child, 2_000)) return;
  signalProcessGroup(child, "SIGKILL");
  if (!await waitForExit(child, 2_000)) throw new Error(`owned Codex process ${child.pid ?? "unknown"} did not terminate`);
};

const runProcess = (command: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv, signal: AbortSignal): Promise<ProcessResult> => {
  checkAbort(signal);
  const child = spawn(command, [...args], { cwd, env, stdio: ["ignore", "pipe", "pipe"], detached: true });
  let stdout = "", stderr = "";
  child.stdout?.on("data", chunk => { stdout = (stdout + String(chunk)).slice(-2_000_000); });
  child.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-100_000); });
  return new Promise<ProcessResult>((resolveResult, reject) => {
    let settled = false;
    const finish = (result: ProcessResult): void => { if (settled) return; settled = true; signal.removeEventListener("abort", abort); resolveResult({ ...result, stdout, stderr }); };
    const abort = (): void => { void (async () => { signalProcessGroup(child, "SIGTERM"); if (!await waitForExit(child, 2_000) && processStatus(child) === "running") signalProcessGroup(child, "SIGKILL"); await waitForExit(child, 2_000); finish({ code: null, signal: "SIGTERM", stdout, stderr }); })(); };
    signal.addEventListener("abort", abort, { once: true });
    child.once("error", error => { if (!settled) { settled = true; signal.removeEventListener("abort", abort); reject(error); } });
    child.once("close", (code, childSignal) => finish({ code, signal: childSignal, stdout, stderr }));
  });
};

export type LiveProcessSnapshot = Readonly<{
  pid: number | undefined; exit_code: number | null; signal_code: NodeJS.Signals | null;
  closed_at: string | null; spawn_error: string | null; caller_requested_close: boolean; stderr: string;
}>;
type LiveProcess = Readonly<{ child: ChildProcess; events: JsonObject[]; stderr: () => string; snapshot: () => LiveProcessSnapshot; requestClose: () => void }>;
export const startLiveProcess = (command: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv): LiveProcess => {
  const child = spawn(command, [...args], { cwd, env, stdio: ["ignore", "pipe", "pipe"], detached: true });
  const events: JsonObject[] = [];
  let stdout = "", stderr = "", spawnError: string | null = null;
  let closedAt: string | null = null, callerRequestedClose = false;
  const ingest = (chunk: unknown): void => {
    stdout += String(chunk);
    const lines = stdout.split("\n");
    stdout = lines.pop() ?? "";
    events.push(...parseLines(lines.join("\n")));
  };
  child.stdout?.on("data", ingest);
  child.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-100_000); });
  child.once("error", error => { spawnError = String(error); });
  child.once("close", () => { closedAt ??= new Date().toISOString(); });
  return Object.freeze({ child, events, stderr: () => stderr,
    snapshot: () => Object.freeze({ pid: child.pid, exit_code: child.exitCode, signal_code: child.signalCode, closed_at: closedAt, spawn_error: spawnError, caller_requested_close: callerRequestedClose, stderr }),
    requestClose: () => { callerRequestedClose = true; },
  });
};

const waitForLiveRuntime = async (process: LiveProcess, signal: AbortSignal): Promise<RuntimeObservation> => {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const threadId = codexHeadlessThreadStartedId(process.events);
    const runtimeStatus = headlessRuntimeStatus(process.events);
    if (threadId && runtimeStatus) return Object.freeze({ agent: runtimeStatus.agent, runtimeId: threadId, epoch: threadId, raw: runtimeStatus.raw });
    if (processStatus(process.child) !== "running") throw new Error(`Codex headless actor exited before runtime binding: ${process.stderr()}`);
    await delay(250, signal);
  }
  throw new Error("Codex headless actor did not expose a runtime binding");
};

const configOverrides = (config: Json): string[] => Object.entries(config).flatMap(([key, value]) => ["-c", `${key}=${JSON.stringify(value)}`]);
const redactedConfig = (config: Json): Json => Object.fromEntries(Object.entries(config).sort(([left], [right]) => left.localeCompare(right)).map(([key, value]) => {
  if (/(?:token|password|secret|credential|api[_-]?key)/iu.test(key)) return [key, "<redacted>"];
  const text = typeof value === "string" ? value.replace(/redis:\/\/[^\s"']+/gu, "redis://<owned-db15>") : value;
  return [key, text];
}));
const configHash = (config: Json): string => hash(JSON.stringify(redactedConfig(config)));
export const codexEffectiveConfig = (base: Json, redisUrl: string, socket?: string): Json => {
  const config = { ...base };
  Object.assign(config, {
    "mcp_servers.gptqueue-shared.enabled": true,
    "mcp_servers.gptqueue-shared.required": true,
    "mcp_servers.gptqueue-shared.command": process.execPath,
    "mcp_servers.gptqueue-shared.args": [join(repo, "bin/gptqueue-session"), "--client", "codex", "--redis-url", redisUrl],
    "mcp_servers.gptqueue-shared.env.REDIS_URL": redisUrl,
    ...(socket ? { "mcp_servers.gptqueue-shared.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET": socket } : {}),
    model_reasoning_effort: "low",
  });
  return config;
};
export const codexHeadlessEffectiveConfig = (base: Json, redisUrl: string): Json => Object.fromEntries(
  Object.entries(codexEffectiveConfig(base, redisUrl)).filter(([key]) => !key.startsWith("mcp_servers.") || key.startsWith("mcp_servers.gptqueue-shared.")),
);
const privateConfig = async (redisUrl: string, socket?: string, configOverride?: Json): Promise<Json> => {
  return codexEffectiveConfig(configOverride ?? await appConfig(), redisUrl, socket);
};

const waitForPath = async (path: string, child: ChildProcess, signal: AbortSignal): Promise<void> => {
  const deadline = Date.now() + 20_000;
  while (!existsSync(path)) {
    checkAbort(signal);
    if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Codex app-server exited before creating ${path}`);
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for Codex app-server socket ${path}`);
    await delay(100, signal);
  }
};

export const validateCodexRedisUrl = (value: string): void => {
  let parsed: URL;
  try { parsed = new URL(value); } catch { throw new Error("Codex qualification requires a valid Redis URL"); }
  if (parsed.protocol !== "redis:" || !["127.0.0.1", "localhost", "::1", "[::1]"].includes(parsed.hostname) || parsed.pathname !== "/15" || parsed.username || parsed.password) {
    throw new Error("Codex qualification requires an unauthenticated loopback Redis db15 URL");
  }
};

const createHost = (options: CodexOptions): CodexHost => {
  let session: HostSession | undefined;
  let opening: Promise<HostSession> | undefined;
  let sessionRedisUrl: string | undefined;
  let openingRedisUrl: string | undefined;
  let closed = false;
  const ensure = async (redisUrl: string, signal: AbortSignal): Promise<HostSession> => {
    checkAbort(signal);
    validateCodexRedisUrl(redisUrl);
    if (closed) throw new Error("Codex qualification host is closed");
    if (session) {
      if (sessionRedisUrl !== redisUrl) throw new Error("Codex qualification host cannot mix Redis URLs in one cohort");
      return session;
    }
    if (opening) {
      if (openingRedisUrl !== redisUrl) throw new Error("Codex qualification host cannot mix Redis URLs in one cohort");
      return withAbort(opening, signal);
    }
    openingRedisUrl = redisUrl;
    const pending = (async () => {
      const directory = await mkdtemp(join(tmpdir(), "gptqueue-qualification-codex-"));
      const socket = join(directory, "control.sock");
      const config = await privateConfig(redisUrl, socket, options.config);
      const child = spawn(options.codexBin ?? process.env.CODEX_BIN ?? DEFAULT_CODEX,
        ["app-server", "--listen", `unix://${socket}`, ...configOverrides(config)],
        { cwd: options.workspaceRoot ?? repo, env: { ...process.env, REDIS_URL: redisUrl, GPTQUEUE_CODEX_APP_SERVER_SOCKET: socket }, stdio: ["ignore", "pipe", "pipe"], detached: true });
      let stdout = "", stderr = "";
      child.stdout?.on("data", chunk => { stdout = (stdout + String(chunk)).slice(-20_000); });
      child.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-20_000); });
      try {
        await waitForPath(socket, child, signal);
        const rpc = new CodexSocketClient(socket, 20_000);
        const hostSession: HostSession = Object.freeze({
          rpc, socket, config, configHash: configHash(config), model: model(options),
          close: async () => { await rpc.close(); await stopProcess(child); await rm(directory, { recursive: true, force: true }); },
        });
        session = hostSession;
        sessionRedisUrl = redisUrl;
        return hostSession;
      } catch (error) { await stopProcess(child); await rm(directory, { recursive: true, force: true }); throw error; }
    })();
    opening = pending;
    void pending.then(() => { if (opening === pending) { opening = undefined; openingRedisUrl = undefined; } }, () => { if (opening === pending) { opening = undefined; openingRedisUrl = undefined; } });
    return withAbort(pending, signal);
  };
  return Object.freeze({ ensure, close: async () => { closed = true; if (opening) await opening.catch(() => undefined); if (session) { const current = session; session = undefined; sessionRedisUrl = undefined; await current.close(); } } });
};

export const codexTurnCompleted = (thread: JsonObject, turnId: string): boolean => {
  const turns = Array.isArray(thread.turns) ? thread.turns : [];
  const turn = turns.map(object).find(value => value?.id === turnId);
  return ["completed", "succeeded", "failed", "interrupted"].includes(String(turn?.status ?? "").toLowerCase());
};
const acceptedTurnId = (result: JsonObject): string => {
  const id = object(result.turn)?.id;
  if (!nonEmpty(id)) throw new Error("Codex turn/start did not return an exact turn ID");
  return id;
};
const readThreadBounded = async (rpc: CodexSocketClient, threadId: string, signal: AbortSignal): Promise<Json> => {
  for (let attempt = 0; ; attempt += 1) {
    try { return await readCodexAppserverHistory(rpc, threadId, signal); }
    catch (error) {
      if (attempt >= 4 || signal.aborted || !/rollout .+ is empty|failed to read thread/.test(String(error))) throw error;
      await delay(1_000, signal);
    }
  }
};

const waitForThread = async (rpc: CodexSocketClient, threadId: string, signal: AbortSignal, turnId?: string): Promise<JsonObject> => {
  const deadline = Date.now() + 180_000;
  let thread: JsonObject = {};
  while (Date.now() < deadline) {
    checkAbort(signal);
    thread = await readThreadBounded(rpc, threadId, signal);
    const turns = Array.isArray(thread.turns) ? thread.turns : [];
    const current = object(turns.at(-1));
    const status = String(current?.status ?? "").toLowerCase();
    if (turnId ? codexTurnCompleted(thread, turnId) : ["completed", "succeeded", "failed", "interrupted"].includes(status)) return thread;
    await delay(500, signal);
  }
  throw new Error(`Codex thread ${threadId} did not reach a terminal turn`);
};

const runtimeFromToolResult = (value: unknown): RuntimeObservation => {
  const direct = object(value);
  const status = object(direct?.structuredContent ?? direct?.structured_content) ?? direct;
  const runtime = object(status?.runtime);
  if (!nonEmpty(status?.agent) || !nonEmpty(runtime?.runtime_id)) throw new Error("Codex runtime status did not report an exact agent/runtime_id");
  if (!nonEmpty(runtime?.working_directory) || !isAbsolute(runtime.working_directory)) throw new Error("Codex runtime status did not report an absolute working directory");
  return Object.freeze({ agent: status.agent, runtimeId: runtime.runtime_id, epoch: nonEmpty(runtime.epoch) ? runtime.epoch : runtime.runtime_id, raw: status });
};

const runtimeStatus = async (rpc: CodexSocketClient, threadId: string, cwd: string, signal: AbortSignal): Promise<RuntimeObservation> => {
  const result = await rpc.request("mcpServer/tool/call", { threadId, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {} }, signal);
  const observation = runtimeFromToolResult(result);
  const runtime = object(observation.raw.runtime);
  if (observation.runtimeId !== threadId || resolve(String(runtime?.working_directory)) !== resolve(cwd)) throw new Error(`Codex runtime binding does not match thread ${threadId} and cwd ${cwd}`);
  return observation;
};

const failureEvidence = async (rpc: CodexSocketClient, threadId: string): Promise<Json> => ({
  thread_id: threadId,
  history: await readCodexAppserverHistory(rpc, threadId, AbortSignal.timeout(10_000)).catch(error => ({ read_error: String(error) })),
  runtime_status: await rpc.request("mcpServer/tool/call", { threadId, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {} }, AbortSignal.timeout(10_000)).catch(error => ({ call_error: String(error) })),
});

const statusFor = async (rpc: CodexSocketClient, threadId: string, cwd: string, terminated: () => boolean, signal: AbortSignal): Promise<RuntimeStatus> => {
  if (terminated()) return { kind: "terminated", runtimeId: threadId };
  try {
    const thread = await readCodexAppserverHistory(rpc, threadId, signal);
    const turns = Array.isArray(thread.turns) ? thread.turns : [];
    const active = turns.some(turn => ["inprogress", "in_progress", "running", "started"].includes(String(object(turn)?.status ?? "").toLowerCase()));
    const observation = await runtimeStatus(rpc, threadId, cwd, signal);
    return { kind: active ? "busy" : "idle", runtimeId: observation.runtimeId };
  } catch (error) { return { kind: "unknown", detail: String(error) }; }
};

const participantIdentity = (route: CodexRouteId, threadId: string, agent: string, cwd: string, profileHash: string, epoch: string) => Object.freeze({
  participantId: `${route}:${threadId}`,
  route,
  hostRuntimeId: threadId,
  agent,
  cwdHash: hashPath(cwd),
  profileHash,
  epochHash: hash(epoch),
});

export const promptCodexTurn = async (rpc: CodexSocketClient, threadId: string, text: string, signal: AbortSignal, wait: (rpc: CodexSocketClient, threadId: string, signal: AbortSignal, turnId?: string) => Promise<JsonObject> = waitForThread): Promise<JsonObject> => {
  checkAbort(signal);
  const result = await rpc.request("turn/start", { threadId, input: [{ type: "text", text }] }, signal);
  const turnId = acceptedTurnId(result);
  const interrupt = (): void => {
    if (!turnId) return;
    void rpc.request("turn/interrupt", { threadId, turnId }, AbortSignal.timeout(10_000)).catch(() => undefined);
  };
  if (signal.aborted) interrupt();
  else signal.addEventListener("abort", interrupt, { once: true });
  try {
    await wait(rpc, threadId, signal, turnId);
    return result;
  } finally {
    signal.removeEventListener("abort", interrupt);
  }
};

const rpcParticipant = (route: CodexRouteId, rpc: CodexSocketClient, threadId: string, cwd: string, profileHash: string, observation: RuntimeObservation, provenance: Readonly<{ model: string; socketHash: string; configHash: string }>, closeImpl: () => Promise<void>): CodexParticipant => {
  let terminated = false;
  let closing: Promise<void> | undefined;
  const close = async (): Promise<void> => {
    if (closing) return closing;
    terminated = true;
    closing = closeImpl();
    return closing;
  };
  const identity = participantIdentity(route, threadId, observation.agent, cwd, profileHash, observation.epoch);
  return Object.freeze({
    kind: "model" as const,
    identity,
    provenance: Object.freeze({ ...provenance, runtimeId: observation.runtimeId }),
    prompt: async (text: string, signal: AbortSignal) => {
      if (terminated) throw new Error(`Codex participant ${threadId} is terminated`);
      return promptCodexTurn(rpc, threadId, text, signal);
    },
    status: (signal: AbortSignal) => statusFor(rpc, threadId, cwd, () => terminated, signal),
    history: (signal: AbortSignal) => readThreadBounded(rpc, threadId, signal),
    close,
  });
};

const startQualificationThread = async (host: HostSession, cwd: string, instructions: string, signal: AbortSignal): Promise<string> => {
  const result = await host.rpc.request("thread/start", {
    cwd, model: host.model, approvalPolicy: "on-request", approvalsReviewer: "auto_review", sandbox: "workspace-write",
    config: host.config, developerInstructions: instructions,
  }, signal);
  const thread = object(result.thread ?? result);
  if (!nonEmpty(thread?.id)) throw new Error("Codex thread/start returned no exact thread ID");
  return thread.id;
};

const initializeThread = async (host: HostSession, route: CodexRouteId, input: LaunchInput, cwd: string, signal: AbortSignal): Promise<ModelParticipant> => {
  const threadId = await startQualificationThread(host, cwd, "Use only the private gptqueue-shared MCP tools. Report the exact get_runtime_status result before doing route work.", signal);
  try {
    const started = await host.rpc.request("turn/start", { threadId, input: [{ type: "text", text: `Initialize ${route} participant ${input.role}. Reply READY-${input.nonce}.` }] }, signal);
    await waitForThread(host.rpc, threadId, signal, acceptedTurnId(started));
    const observation = await runtimeStatus(host.rpc, threadId, cwd, signal);
    return rpcParticipant(route, host.rpc, threadId, cwd, host.configHash, observation, { model: host.model, socketHash: hash(host.socket), configHash: host.configHash }, async () => {
      const thread = await readCodexAppserverHistory(host.rpc, threadId, AbortSignal.timeout(10_000)).catch(() => undefined) as JsonObject | undefined;
      const turns = Array.isArray(thread?.turns) ? thread.turns : [];
      for (const turn of turns) {
        const current = object(turn);
        const status = String(current?.status ?? "").toLowerCase();
        if (current && nonEmpty(current.id) && ["inprogress", "in_progress", "running", "started"].includes(status)) await host.rpc.request("turn/interrupt", { threadId, turnId: current.id }, AbortSignal.timeout(10_000)).catch(() => undefined);
      }
      await host.rpc.request("thread/archive", { threadId }, AbortSignal.timeout(10_000));
    });
  } catch (error) {
    const evidence = await failureEvidence(host.rpc, threadId);
    await host.rpc.request("thread/archive", { threadId }, AbortSignal.timeout(10_000)).catch(() => undefined);
    const failure = error instanceof Error ? error : new Error(String(error));
    Object.assign(failure, { evidence });
    throw failure;
  }
};

const parseLines = (stdout: string): readonly JsonObject[] => stdout.split("\n").flatMap(line => {
  if (!line.trim()) return [];
  try { const value = JSON.parse(line) as unknown; return object(value) ? [value as JsonObject] : []; } catch { return []; }
});
const walk = (value: unknown, visit: (value: JsonObject) => void): void => {
  if (Array.isArray(value)) { value.forEach(item => walk(item, visit)); return; }
  const current = object(value);
  if (!current) return;
  visit(current); Object.values(current).forEach(item => walk(item, visit));
};
const eventThreadId = (events: readonly JsonObject[]): string | undefined => {
  let value: string | undefined;
  events.forEach(event => walk(event, current => {
    if (!value && (nonEmpty(current.thread_id) || nonEmpty(current.threadId))) value = String(current.thread_id ?? current.threadId);
  }));
  return value;
};
const embeddedObjects = (value: unknown): readonly JsonObject[] => {
  if (typeof value === "string") {
    try { return embeddedObjects(JSON.parse(value)); } catch { return []; }
  }
  if (Array.isArray(value)) return value.flatMap(embeddedObjects);
  const current = object(value);
  return current ? [current, ...Object.values(current).flatMap(embeddedObjects)] : [];
};
export const codexHeadlessThreadStartedId = (events: readonly JsonObject[]): string | undefined => {
  let threadId: string | undefined;
  events.forEach(event => walk(event, current => {
    const kind = String(current.type ?? current.event ?? current.name ?? "").toLowerCase();
    if (!threadId && (kind === "thread.started" || kind === "thread_started")) {
      const nested = object(current.thread);
      const candidate = current.thread_id ?? current.threadId ?? nested?.id ?? nested?.thread_id;
      if (nonEmpty(candidate)) threadId = candidate;
    }
  }));
  return threadId;
};
const headlessRuntimeStatus = (events: readonly JsonObject[]): Readonly<{ agent: string; raw: JsonObject }> | undefined => {
  for (const event of events) {
    for (const current of embeddedObjects(event)) {
      const name = String(current.name ?? current.tool ?? current.tool_name ?? "").toLowerCase();
      if (name !== "get_runtime_status") continue;
      for (const candidate of embeddedObjects(current.result ?? current.output ?? current.content)) {
        if (candidate.status === "ok" && nonEmpty(candidate.agent)) return Object.freeze({ agent: candidate.agent, raw: candidate });
      }
    }
  }
  return undefined;
};
export const codexHeadlessRuntimeAgent = (events: readonly JsonObject[]): string | undefined => headlessRuntimeStatus(events)?.agent;
const HEADLESS_CONTROLLER_TASK_PREFIX = "GPTQUEUE_HEADLESS_CONTROLLER_TASK_V1:";
export const encodeHeadlessControllerTask = (instruction: string): string => `${HEADLESS_CONTROLLER_TASK_PREFIX}${JSON.stringify({ instruction })}`;
export const decodeHeadlessControllerTask = (content: unknown): string | undefined => {
  if (typeof content !== "string" || !content.startsWith(HEADLESS_CONTROLLER_TASK_PREFIX)) return undefined;
  try {
    const value = JSON.parse(content.slice(HEADLESS_CONTROLLER_TASK_PREFIX.length)) as JsonObject;
    return typeof value.instruction === "string" ? value.instruction : undefined;
  } catch { return undefined; }
};
const runtimeEvents = (events: readonly JsonObject[]): RuntimeObservation[] => {
  const output: RuntimeObservation[] = [];
  events.forEach(event => walk(event, current => {
    const name = String(current.name ?? current.tool ?? current.tool_name ?? "").toLowerCase();
    if (name !== "get_runtime_status") return;
    try { output.push(runtimeFromToolResult(current.result ?? current.output ?? current.content)); } catch { /* incomplete tool event */ }
  }));
  return output;
};

const headlessParticipant = async (input: LaunchInput, cwd: string, options: CodexOptions, signal: AbortSignal): Promise<CodexParticipant> => {
  const config = codexHeadlessEffectiveConfig(options.config ?? await appConfig(), input.redisUrl);
  const command = options.codexBin ?? process.env.CODEX_BIN ?? DEFAULT_CODEX;
  const control = new RedisClient(null, input.redisUrl);
  const controllerName = `codex-headless-control-${randomUUID()}`;
  const live = startLiveProcess(command, [
    "exec", "--json", "--ignore-user-config", "--skip-git-repo-check", "--approve-for-me", "--model", model(options), "-C", cwd,
    ...configOverrides(config),
    "Remain available as a headless GPTQueue qualification actor. Call get_runtime_status once, then wait with receive_message(timeout=60) for a task; do not poll claim_tasks. Only messages whose metadata has qualification_control=true are controller instructions. Controller content is a GPTQUEUE_HEADLESS_CONTROLLER_TASK_V1 envelope; parse its enclosed instruction exactly, execute that instruction as one inner assignment, and send the exact correlated result to the task sender with in_reply_to. When the inner assignment says stop or finish, stop only that assignment, then return to the outer receive_message(timeout=60) wait loop. Never interpret completion of an enclosed instruction as termination of this headless actor or exec turn. Acknowledge it if a claim exists, continue waiting, and do not treat bootstrap or control traffic as peer qualification evidence.",
  ], cwd, { ...process.env, REDIS_URL: input.redisUrl });
  let observation: RuntimeObservation;
  let registered = false;
  try {
    await control.register("both", controllerName, "Codex headless qualification control channel", {
      label: "qualification-control",
      uuid: null,
      client: null,
      working_directory: null,
    });
    registered = true;
    observation = await waitForLiveRuntime(live, signal);
    const registryDeadline = Date.now() + 60_000;
    let registryBound = false;
    while (Date.now() < registryDeadline) {
      const records = await control.listAgents();
      registryBound = records.some(record => record.name === observation.agent && record.online && record.working_directory === cwd);
      if (registryBound) break;
      await delay(250, signal);
    }
    if (!registryBound) throw new Error("Codex headless runtime agent did not expose an online registry record for its cwd");
  } catch (error) {
    await stopProcess(live.child).catch(() => undefined);
    if (registered) await control.unregister().catch(() => undefined);
    await control.shutdown().catch(() => undefined);
    throw error;
  }
  let closed = false;
  let closing: Promise<void> | undefined;
  const identity = participantIdentity("codex-headless", observation.runtimeId, observation.agent, cwd, configHash(config), observation.epoch);
  const prompt = async (text: string, promptSignal: AbortSignal): Promise<unknown> => {
    checkAbort(promptSignal);
    if (closed) throw new Error(`Codex headless participant ${observation.runtimeId} is terminated`);
    const task: QueueMessage = {
      id: randomUUID(), from: controllerName, to: observation.agent, timestamp: new Date().toISOString(), type: "task",
      payload: { content: encodeHeadlessControllerTask(text), metadata: { qualification_control: true, nonce: input.nonce } },
    };
    const sent = await control.sendMessageIdempotent(task, `${input.pairId}:${input.role}:${input.nonce}:${task.id}`);
    if (sent.status === "full") throw new Error(`Codex headless control mailbox was full for ${observation.agent}`);
    const deadline = Date.now() + 180_000;
    while (Date.now() < deadline) {
      checkAbort(promptSignal);
      const reply = await control.receiveMessage(30, promptSignal);
      if (reply?.from === observation.agent && reply.to === controllerName && reply.payload.in_reply_to === sent.messageId) return reply;
    }
    throw new Error(`Codex headless actor did not reply to control task ${sent.messageId}`);
  };
  const close = async (): Promise<void> => {
    if (closing) return closing;
    closed = true;
    closing = (async () => {
      let failure: unknown;
      live.requestClose();
      try { await stopProcess(live.child); } catch (error) { failure = error; }
      try { if (registered) await control.unregister(); } catch (error) { failure ??= error; }
      try { await control.shutdown(); } catch (error) { failure ??= error; }
      if (failure) throw failure;
    })();
    return closing;
  };
  return Object.freeze({
    kind: "model" as const,
    identity,
    provenance: Object.freeze({ model: model(options), socketHash: hash("codex-headless-control"), configHash: configHash(config), runtimeId: observation.runtimeId }),
    prompt,
    status: async (_statusSignal: AbortSignal): Promise<RuntimeStatus> => {
      if (closed) return { kind: "terminated", runtimeId: observation.runtimeId, detail: JSON.stringify(live.snapshot()) };
      if (processStatus(live.child) !== "running") return { kind: "terminated", runtimeId: observation.runtimeId, detail: JSON.stringify(live.snapshot()) };
      return { kind: "unknown", detail: "Headless actor is waiting through GPTQueue control tasks; idle is not observable" };
    },
    history: async (_historySignal: AbortSignal) => Object.freeze([...live.events, { type: "gptqueue_process_diagnostic", diagnostic: live.snapshot() }]),
    close,
  });
};

const completedParticipant = (route: CodexRouteId, cwd: string, profileHash: string, threadId: string, observation: RuntimeObservation, history: readonly JsonObject[], provenance: Readonly<{ model: string; socketHash: string; configHash: string }>): CodexParticipant => {
  let closed = false;
  const identity = participantIdentity(route, threadId, observation.agent, cwd, profileHash, observation.epoch);
  return Object.freeze({
    kind: "model" as const, identity,
    provenance: Object.freeze({ ...provenance, runtimeId: observation.runtimeId }),
    prompt: async (_text: string, signal: AbortSignal) => { checkAbort(signal); if (closed) throw new Error(`Codex participant ${threadId} is terminated`); throw new Error(`Codex ${route} participant is a completed one-shot process`); },
    status: async (_signal: AbortSignal): Promise<RuntimeStatus> => ({ kind: "terminated", runtimeId: threadId }),
    history: async (_signal: AbortSignal) => history,
    close: async () => { closed = true; },
  });
};

const execParticipant = async (route: "codex-headless" | "codex-resume" | "codex-native-child", input: LaunchInput, cwd: string, options: CodexOptions, signal: AbortSignal): Promise<ModelParticipant> => {
  const config = await privateConfig(input.redisUrl, undefined, options.config);
  const command = options.codexBin ?? process.env.CODEX_BIN ?? DEFAULT_CODEX;
  const base = ["exec", "--json", "--ephemeral", "--ignore-user-config", "--skip-git-repo-check", "--approve-for-me", "--model", model(options), "-C", cwd, ...configOverrides(config)];
  const prompt = route === "codex-native-child"
    ? `Use only gptqueue-shared MCP tools. Call get_runtime_status, then use native collaboration to spawn exactly one child with this instruction: call get_runtime_status and report CHILD_READY-${input.nonce}; do not include any queue task. Wait for the child and preserve the child's exact runtime identity.`
    : `Use only gptqueue-shared MCP tools. Call get_runtime_status and report READY-${input.nonce}.`;
  const first = await runProcess(command, [...base, prompt], cwd, { ...process.env, REDIS_URL: input.redisUrl }, signal);
  const events = [...parseLines(first.stdout)];
  if (first.code !== 0) throw new Error(`Codex ${route} launch failed (${first.code ?? first.signal}): ${first.stderr.slice(-2_000)}`);
  const threadId = eventThreadId(events);
  if (!threadId) throw new Error(`Codex ${route} did not report an exact thread ID`);
  let finalEvents = events;
  if (route === "codex-resume") {
    const resumed = await runProcess(command, [...base, "resume", threadId, `Resume this exact Codex thread and report RESUMED-${input.nonce} after calling get_runtime_status.`], cwd, { ...process.env, REDIS_URL: input.redisUrl }, signal);
    if (resumed.code !== 0) throw new Error(`Codex resume failed (${resumed.code ?? resumed.signal}): ${resumed.stderr.slice(-2_000)}`);
    finalEvents = [...events, ...parseLines(resumed.stdout)];
  }
  const observations = runtimeEvents(finalEvents);
  const distinctObservations = observations.filter((candidate, index) => observations.findIndex(other => other.agent === candidate.agent && other.runtimeId === candidate.runtimeId) === index);
  const observation = route === "codex-native-child"
    ? (distinctObservations.length >= 2 ? distinctObservations.at(-1) : undefined)
    : observations.at(-1);
  if (!observation) throw new Error(`Codex ${route} did not report an exact GPTQueue runtime identity`);
  if (route === "codex-native-child" && (distinctObservations[0]?.agent === observation.agent || distinctObservations[0]?.runtimeId === observation.runtimeId)) throw new Error("Codex native child identity was not distinct from its parent");
  if (route !== "codex-native-child" && observation.runtimeId !== threadId) throw new Error(`Codex ${route} runtime ID does not equal its thread ID`);
  const childThreadId = route === "codex-native-child" ? observation.runtimeId : threadId;
  return completedParticipant(route, cwd, configHash(config), childThreadId, observation, finalEvents, { model: model(options), socketHash: hash("codex-exec"), configHash: configHash(config) });
};

const loadedThread = async (rpc: CodexSocketClient, cwd: string, signal: AbortSignal): Promise<string> => {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const listed = await rpc.request("thread/loaded/list", {}, signal);
    const ids = Array.isArray(listed.data) ? listed.data.filter(nonEmpty) : [];
    for (const id of ids) {
      const thread = await readCodexAppserverHistory(rpc, id, AbortSignal.timeout(10_000)).catch(() => undefined) as JsonObject | undefined;
      if (thread && resolve(String(thread.cwd ?? "")) === resolve(cwd)) return id;
    }
    await delay(500, signal);
  }
  throw new Error(`Codex TUI did not expose a thread for ${cwd}`);
};

const tuiParticipant = async (route: "codex-interactive" | "codex-fork", input: LaunchInput, cwd: string, host: HostSession, options: CodexOptions, signal: AbortSignal, parentThread?: string): Promise<ModelParticipant> => {
  cwd = options.tuiTrustRoot ?? cwd;
  let parent: string | undefined = parentThread;
  if (route === "codex-fork") {
    const parentCwd = join(cwd, "fork-parent"); await mkdir(parentCwd, { recursive: true });
    parent = await startQualificationThread(host, parentCwd, "Use only the private gptqueue-shared MCP tools.", signal);
    await host.rpc.request("turn/start", { threadId: parent, input: [{ type: "text", text: `Initialize fork parent ${input.nonce}.` }] }, signal);
    await waitForThread(host.rpc, parent, signal);
  }
  const terminal = pty.spawn(options.codexBin ?? process.env.CODEX_BIN ?? DEFAULT_CODEX, [
    ...(parent ? ["fork", parent] : []), "--remote", `unix://${host.socket}`, "--no-alt-screen", "--model", model(options), "-C", cwd,
    "--approve-for-me", ...configOverrides(host.config), `Use only gptqueue-shared MCP tools. Call get_runtime_status and report READY-${input.nonce}.`,
  ], { cwd, env: { ...process.env, TERM: "xterm-256color", GPTQUEUE_CODEX_APP_SERVER_SOCKET: host.socket, REDIS_URL: input.redisUrl }, cols: 140, rows: 45, name: "xterm-256color" });
  let terminalExited = false;
  terminal.onExit(() => { terminalExited = true; });
  terminal.onData(data => { if (data.includes("\x1b[6n")) terminal.write("\x1b[1;1R"); });
  try {
    const threadId = await loadedThread(host.rpc, cwd, signal);
    await waitForThread(host.rpc, threadId, signal);
    const observation = await runtimeStatus(host.rpc, threadId, cwd, signal);
    const participant = rpcParticipant(route, host.rpc, threadId, cwd, host.configHash, observation, { model: host.model, socketHash: hash(host.socket), configHash: host.configHash }, async () => {
      if (!terminalExited) terminal.kill("SIGTERM");
      await host.rpc.request("thread/archive", { threadId }, AbortSignal.timeout(10_000)).catch(() => undefined);
      if (parent) await host.rpc.request("thread/archive", { threadId: parent }, AbortSignal.timeout(10_000)).catch(() => undefined);
    });
    return participant;
  } catch (error) {
    if (!terminalExited) terminal.kill("SIGTERM");
    if (parent) await host.rpc.request("thread/archive", { threadId: parent }, AbortSignal.timeout(10_000)).catch(() => undefined);
    throw error;
  }
};

const routeSpec = (id: CodexRouteId): RouteSpec => Object.freeze({ id, host: "codex", modelBacked: true, availability: { kind: "setup_gap" as const, detail: "Codex route preflight not run" } });
const workspace = (options: CodexOptions, input: LaunchInput, route: CodexRouteId): string => resolve(options.workspaceRoot ?? join(repo, ".gptqueue/qualification/codex"), hash(input.pairId).slice(0, 16), route, input.role, hash(input.nonce).slice(0, 16));
const preflight = (options: CodexOptions, route: CodexRouteId, signal: AbortSignal): Promise<Availability> => (async () => {
  checkAbort(signal);
  const executable = options.codexBin ?? process.env.CODEX_BIN ?? DEFAULT_CODEX;
  try { await access(executable, constants.X_OK); } catch { return { kind: "setup_gap", detail: `Codex executable is unavailable: ${executable}` }; }
  if (["codex-appserver", "codex-interactive", "codex-fork"].includes(route)) {
    const socketHome = process.env.CODEX_HOME ?? join(homedir(), ".codex");
    try { await access(join(socketHome, "config.toml"), constants.R_OK); } catch { return { kind: "setup_gap", detail: "Codex config.toml is unavailable for process-local MCP overrides" }; }
  }
  return { kind: "available" };
})();

export type CodexAdapterSet = Readonly<{ adapters: readonly RouteAdapter[]; close: () => Promise<void> }>;

export const createCodexAdapters = (options: CodexOptions = {}): CodexAdapterSet => {
  const host = options.host ?? createHost(options);
  const launch = (route: CodexRouteId) => async (input: LaunchInput, signal: AbortSignal): Promise<Participant> => {
    const cwd = workspace(options, input, route);
    await mkdir(cwd, { recursive: true });
    switch (route) {
      case "codex-headless": return headlessParticipant(input, cwd, options, signal);
      case "codex-resume":
      case "codex-native-child": return execParticipant(route, input, cwd, options, signal);
      case "codex-appserver": {
        const session = await host.ensure(input.redisUrl, signal);
        return initializeThread(session, route, input, cwd, signal);
      }
      case "codex-interactive":
      case "codex-fork": {
        const session = await host.ensure(input.redisUrl, signal);
        return tuiParticipant(route, input, cwd, session, options, signal);
      }
    }
  };
  const adapters = Object.freeze(codexRouteIds.map(id => Object.freeze({ spec: routeSpec(id), preflight: (signal: AbortSignal) => preflight(options, id, signal), launch: launch(id) })));
  return Object.freeze({ adapters, close: host.close });
};

export const createCodexRouteAdapters = (options: CodexOptions = {}): readonly RouteAdapter[] => createCodexAdapters(options).adapters;
export const isCodexRoute = (route: RouteId): route is CodexRouteId => (codexRouteIds as readonly string[]).includes(route);
export const codexCommand = (route: CodexRouteId): readonly string[] => route === "codex-headless" ? ["exec", "--json", "--ephemeral"] : route === "codex-resume" ? ["exec", "resume"] : route === "codex-native-child" ? ["exec", "--json", "--ephemeral", "native-collaboration"] : route === "codex-appserver" ? ["app-server", "--listen", "unix://owned"] : route === "codex-fork" ? ["fork", "<parent-thread>"] : ["tui", "--remote", "unix://owned"];
