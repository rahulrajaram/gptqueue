import { access, constants, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { spawn, type ChildProcess } from "node:child_process";
import * as pty from "node-pty";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { opencodeAgentName } from "../../src/registered-shell/opencode-backend.js";
import { repairConfig, repairPluginPath } from "./opencode-repair-support.js";
import { model, modelsPath, newConfigHome, opencodeBin } from "./opencode-support.js";
import type { QueueMessage } from "../../src/mcp-server/types.js";
import type {
  Availability,
  ModelParticipant,
  ParticipantIdentity,
  RouteAdapter,
  RouteSpec,
  RuntimeStatus,
} from "./qualification-types.js";

export const opencodeInteractiveRoute = "opencode-interactive" as const;
type LaunchInput = Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>;
type Json = Record<string, unknown>;
type PtyProcess = ReturnType<typeof pty.spawn>;
type Binding = Readonly<{ agent: string; runtimeId: string; epoch: string; directory: string; raw: Json }>;
type NativeSession = Readonly<{ id: string; directory: string; parentID?: string; raw: Json }>;
type EvidenceParticipant = ModelParticipant & Readonly<{
  evidence: () => Promise<Readonly<Record<string, unknown>>>;
  ownedProcessIds: readonly number[];
  cleanupFailureHistory: readonly string[];
}>;

const timeoutMs = 150_000;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value)
  ? value as Json : undefined;
const decode = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("OpenCode interactive qualification operation aborted");
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw abortError(signal); };
const delay = async (ms: number, signal: AbortSignal): Promise<void> => {
  checkAbort(signal);
  await new Promise<void>((resolveDelay, reject) => {
    const timer = setTimeout(resolveDelay, ms);
    signal.addEventListener("abort", () => { clearTimeout(timer); reject(abortError(signal)); }, { once: true });
  });
};

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await new Promise<void>((resolveClose) => server.close(() => resolveClose()));
  if (!port) throw new Error("OpenCode interactive could not allocate an owned local port");
  return port;
};

const profile = (redisUrl: string): Readonly<{
  env: NodeJS.ProcessEnv;
  paths: readonly string[];
  profileHash: string;
}> => {
  const configHome = newConfigHome();
  const home = newConfigHome();
  const data = newConfigHome();
  const cache = newConfigHome();
  const state = newConfigHome();
  const effective = JSON.stringify({ config: repairConfig(), redisUrl, home, configHome, data, cache, state, tui: true });
  return Object.freeze({
    env: {
      ...process.env,
      HOME: home,
      XDG_CONFIG_HOME: configHome,
      XDG_DATA_HOME: data,
      XDG_CACHE_HOME: cache,
      XDG_STATE_HOME: state,
      OPENCODE_CONFIG: undefined,
      OPENCODE_CONFIG_DIR: undefined,
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_CONFIG_CONTENT: JSON.stringify(repairConfig()),
      OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
      OPENCODE_MODELS_PATH: modelsPath,
      REDIS_URL: redisUrl,
    },
    paths: Object.freeze([configHome, home, data, cache, state]),
    profileHash: hash(effective),
  });
};

const cleanupPaths = async (paths: readonly string[]): Promise<void> => {
  for (const path of paths) await rm(path, { recursive: true, force: true });
};

const request = async (baseUrl: string, path: string, init?: RequestInit): Promise<unknown> => {
  const response = await fetch(`${baseUrl}${path}`, { ...init, signal: init?.signal ?? AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`OpenCode interactive HTTP ${response.status} ${path}`);
  return response.status === 204 ? undefined : response.json();
};

const sessionFromRecord = (value: unknown, directory: string): NativeSession | undefined => {
  const row = object(decode(value));
  const info = object(row?.info) ?? row;
  if (!nonEmpty(info?.id) || info.directory !== directory) return undefined;
  return Object.freeze({
    id: info.id,
    directory,
    ...(nonEmpty(info.parentID) ? { parentID: info.parentID } : {}),
    raw: row ?? {},
  });
};

const sessionRows = (value: unknown): readonly Json[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) return decoded.flatMap((item) => { const row = object(item); return row ? [row] : []; });
  const row = object(decoded);
  if (!row) return [];
  for (const key of ["data", "sessions", "items", "rows"]) {
    if (row[key] !== undefined) return sessionRows(row[key]);
  }
  return [row];
};

const shellQuoteSql = (value: string): string => `'${value.replaceAll("'", "''")}'`;

const commandJson = async (
  command: readonly string[], cwd: string, env: NodeJS.ProcessEnv, signal: AbortSignal,
): Promise<unknown> => new Promise((resolveCommand, rejectCommand) => {
  checkAbort(signal);
  const child = spawn(opencodeBin, command, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  let settled = false;
  const timer = setTimeout(() => { if (!settled) { settled = true; child.kill("SIGTERM"); rejectCommand(new Error(`OpenCode interactive command timed out: ${command.join(" ")}`)); } }, timeoutMs);
  child.stdout?.on("data", (chunk: Buffer) => { stdout = (stdout + chunk.toString()).slice(-500_000); });
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-20_000); });
  child.once("error", (error) => { if (!settled) { settled = true; clearTimeout(timer); rejectCommand(error); } });
  child.once("exit", (code, childSignal) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    if (code !== 0) rejectCommand(new Error(`OpenCode interactive command failed code=${code ?? "none"} signal=${childSignal ?? "none"}: ${stderr}`));
    else {
      try { resolveCommand(JSON.parse(stdout)); }
      catch (error) { rejectCommand(new Error(`OpenCode interactive command returned invalid JSON: ${String(error)}`)); }
    }
  });
  signal.addEventListener("abort", () => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    child.kill("SIGTERM");
    rejectCommand(abortError(signal));
  }, { once: true });
});

class NativeStore {
  public constructor(
    private readonly baseUrl: string,
    private readonly cwd: string,
    private readonly env: NodeJS.ProcessEnv,
  ) {}

  public async sessions(signal: AbortSignal): Promise<readonly NativeSession[]> {
    const rows = sessionRows(await request(this.baseUrl, `/session?directory=${encodeURIComponent(this.cwd)}`, { signal }));
    return rows.flatMap((row) => { const session = sessionFromRecord(row, this.cwd); return session ? [session] : []; });
  }

  public async record(sessionID: string, signal: AbortSignal): Promise<NativeSession> {
    const session = sessionFromRecord(await request(this.baseUrl, `/session/${encodeURIComponent(sessionID)}?directory=${encodeURIComponent(this.cwd)}`, { signal }), this.cwd);
    if (!session || session.id !== sessionID) throw new Error("OpenCode interactive native session record mismatch");
    return session;
  }

  public async history(sessionID: string, signal: AbortSignal): Promise<readonly unknown[]> {
    const value = await request(this.baseUrl, `/session/${encodeURIComponent(sessionID)}/message?directory=${encodeURIComponent(this.cwd)}`, { signal });
    return Array.isArray(value) ? Object.freeze([...value]) : Object.freeze([value]);
  }

  public async status(signal: AbortSignal): Promise<unknown> {
    return request(this.baseUrl, `/session/status?directory=${encodeURIComponent(this.cwd)}`, { signal });
  }

  public async export(sessionID: string, signal: AbortSignal): Promise<readonly unknown[]> {
    const value = await commandJson(["export", sessionID, "--sanitize"], this.cwd, this.env, signal);
    return Array.isArray(value) ? Object.freeze([...value]) : Object.freeze([value]);
  }

  public async databaseSessions(signal: AbortSignal): Promise<readonly NativeSession[]> {
    const query = `SELECT id, directory, parent_id AS parentID, data FROM session WHERE directory = ${shellQuoteSql(this.cwd)} ORDER BY time_created, id`;
    const value = await commandJson(["db", query, "--format", "json"], this.cwd, this.env, signal);
    return sessionRows(value).flatMap((row) => {
      const data = object(decode(row.data));
      const source = { ...row, ...(data ?? {}) };
      const session = sessionFromRecord(source, this.cwd);
      return session ? [session] : [];
    });
  }
}

class InteractiveTerminal {
  public readonly processId: number;
  private exited = false;
  private exitCode: number | null = null;
  private signal: NodeJS.Signals | null = null;
  private output = "";

  public constructor(public readonly child: PtyProcess) {
    this.processId = child.pid;
    child.onData((value) => { this.output = (this.output + value).slice(-20_000); });
    child.onExit(({ exitCode, signal }) => { this.exited = true; this.exitCode = exitCode; this.signal = signal ? `SIG${signal}` as NodeJS.Signals : null; });
  }

  public snapshot(): Readonly<Record<string, unknown>> {
    return Object.freeze({ process_id: this.processId, exited: this.exited, exit_code: this.exitCode, signal: this.signal, terminal_output_tail: this.output });
  }

  public async close(): Promise<void> {
    if (this.exited) return;
    try { process.kill(-this.processId, "SIGTERM"); } catch { this.child.kill("SIGTERM"); }
    const deadline = Date.now() + 5_000;
    while (!this.exited && Date.now() < deadline) await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
    if (!this.exited) {
      try { process.kill(-this.processId, "SIGKILL"); } catch { this.child.kill("SIGKILL"); }
    }
  }

  public get isExited(): boolean { return this.exited; }
}

const parsedMessage = (value: unknown): Json | undefined => {
  const row = object(decode(value));
  if (!row) return undefined;
  const message = object(row.message);
  return message ?? row;
};

const walk = (value: unknown, visit: (row: Json) => boolean): boolean => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) return decoded.some((item) => walk(item, visit));
  const row = object(decoded);
  if (!row) return false;
  if (visit(row)) return true;
  return Object.values(row).some((item) => walk(item, visit));
};

const textParts = (value: unknown): readonly string[] => {
  const output: string[] = [];
  walk(value, (row) => {
    if (row.type === "text" && typeof row.text === "string") output.push(row.text);
    return false;
  });
  return output;
};

const assistantNonceEvidence = (
  history: readonly unknown[], baselineLength: number, nonce: string, agent: string, runtimeID: string,
): Readonly<{ text: string; runtime: Json }> | undefined => {
  const fresh = history.slice(baselineLength);
  let completedText: string | undefined;
  for (const item of fresh) {
    const message = parsedMessage(item);
    if (!message || message.role !== "assistant") continue;
    const text = textParts(message).join("");
    const complete = message.stopReason === "stop" || message.status === "completed" || message.finishReason === "stop";
    if (complete && text === nonce) completedText = text;
  }
  if (completedText === undefined) return undefined;
  let runtime: Json | undefined;
  walk(fresh, (row) => {
    const state = object(row.state);
    const output = object(state?.output) ?? object(row.output) ?? row;
    const structured = object(output.structuredContent) ?? output;
    const runtimeValue = object(structured.runtime);
    if (row.tool === "gptqueue_get_runtime_status" && state?.status === "completed" && structured.agent === agent &&
        runtimeValue?.runtime_id === runtimeID && nonEmpty(runtimeValue.epoch)) {
      runtime = Object.freeze({ ...structured });
      return true;
    }
    return false;
  });
  return runtime ? Object.freeze({ text: completedText, runtime }) : undefined;
};

const bindingFor = async (redis: Redis, cwd: string, sessionID: string, signal: AbortSignal): Promise<Binding> => {
  const agent = opencodeAgentName(sessionID);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const registrationRaw = await redis.hget(SESSION_KEYS.registry, agent);
    const bindingRaw = await redis.get(`gptq:runtime-binding:${agent}`);
    const registration = object(registrationRaw ? JSON.parse(registrationRaw) : undefined);
    const metadata = object(registration?.metadata);
    const binding = object(bindingRaw ? JSON.parse(bindingRaw) : undefined);
    if (metadata?.working_directory === cwd && binding?.client === "opencode" && binding.runtime_id === sessionID &&
        binding.working_directory === cwd && nonEmpty(binding.epoch)) {
      return Object.freeze({ agent, runtimeId: sessionID, epoch: binding.epoch, directory: cwd, raw: binding });
    }
    await delay(100, signal);
  }
  throw new Error(`OpenCode interactive session ${sessionID} did not expose an exact registry/runtime binding`);
};

const waitForSession = async (store: NativeStore, signal: AbortSignal): Promise<Readonly<{ session: NativeSession; source: "http" | "db" }>> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const fromHttp = await store.sessions(signal).catch(() => [] as readonly NativeSession[]);
    if (fromHttp.length > 0) return Object.freeze({ session: fromHttp.at(-1)!, source: "http" as const });
    const fromDatabase = await store.databaseSessions(signal).catch(() => [] as readonly NativeSession[]);
    if (fromDatabase.length > 0) return Object.freeze({ session: fromDatabase.at(-1)!, source: "db" as const });
    await delay(100, signal);
  }
  throw new Error("OpenCode interactive TUI did not expose a native session through HTTP or the private database");
};

const preflight = async (signal: AbortSignal): Promise<Availability> => {
  checkAbort(signal);
  try {
    await Promise.all([
      access(opencodeBin, constants.X_OK),
      access(repairPluginPath, constants.R_OK),
      access(modelsPath, constants.R_OK),
    ]);
    return Object.freeze({ kind: "available" as const });
  } catch (error) {
    return Object.freeze({ kind: "blocked_prerequisite" as const, detail: `OpenCode interactive prerequisite unavailable: ${String(error)}` });
  }
};

const identityOf = (session: NativeSession, binding: Binding, profileHash: string): ParticipantIdentity => Object.freeze({
  participantId: `${opencodeInteractiveRoute}:${session.id}`,
  route: opencodeInteractiveRoute,
  hostRuntimeId: session.id,
  agent: binding.agent,
  cwdHash: hash(session.directory),
  profileHash,
  epochHash: hash(binding.epoch),
});

export const startOpenCodeInteractiveParticipant = async (
  input: LaunchInput, signal: AbortSignal,
): Promise<EvidenceParticipant> => {
  let ownedProfile: ReturnType<typeof profile> | undefined;
  let cwd: string | undefined;
  let redis: Redis | undefined;
  let terminal: InteractiveTerminal | undefined;
  let closed = false;
  const cleanupFailures: string[] = [];
  try {
    ownedProfile = profile(input.redisUrl);
    cwd = await mkdtemp(join(tmpdir(), "gptq-opencode-interactive-"));
    const port = await freePort();
    redis = new Redis(input.redisUrl, { maxRetriesPerRequest: 3, retryStrategy: () => null });
    const command = [cwd, "--port", String(port), "--hostname", "127.0.0.1", "--model", model, "--agent", "build", "--auto"] as const;
    const child = pty.spawn(opencodeBin, [...command], {
      cwd,
      env: ownedProfile.env,
      cols: 140,
      rows: 45,
      name: "xterm-256color",
    });
    terminal = new InteractiveTerminal(child);
    const store = new NativeStore(`http://127.0.0.1:${port}`, cwd, ownedProfile.env);
    const located = await waitForSession(store, signal);
    const native = await store.record(located.session.id, signal).catch(() => located.session);
    const binding = await bindingFor(redis, cwd, native.id, signal);
    const identity = identityOf(native, binding, ownedProfile.profileHash);
    const initialHistory = await store.history(native.id, signal).catch(() => store.export(native.id, signal));
    const close = async (): Promise<void> => {
      if (closed) return;
      const failures: unknown[] = [];
      try { await terminal?.close(); } catch (error) { failures.push(error); cleanupFailures.push(`terminal:${String(error)}`); }
      try { await redis?.quit(); } catch (error) { failures.push(error); cleanupFailures.push(`redis:${String(error)}`); }
      try { if (ownedProfile) await cleanupPaths(ownedProfile.paths); } catch (error) { failures.push(error); cleanupFailures.push(`profile:${String(error)}`); }
      try { if (cwd) await rm(cwd, { recursive: true, force: true }); } catch (error) { failures.push(error); cleanupFailures.push(`directory:${String(error)}`); }
      if (failures.length > 0) throw new AggregateError(failures, "OpenCode interactive cleanup failed");
      closed = true;
    };
    return Object.freeze({
      kind: "model" as const,
      identity,
      prompt: async (text: string, promptSignal: AbortSignal): Promise<unknown> => {
        checkAbort(promptSignal);
        if (closed || !terminal || terminal.isExited) throw new Error(`OpenCode interactive participant ${native.id} is terminated`);
        const before = await store.history(native.id, promptSignal).catch(() => store.export(native.id, promptSignal));
        terminal.child.write(`${text}\r`);
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
          checkAbort(promptSignal);
          const current = await store.history(native.id, promptSignal).catch(() => store.export(native.id, promptSignal));
          const evidence = assistantNonceEvidence(current, before.length, text.match(/^Reply with this exact nonce and no other text:\s*(.+)$/u)?.[1] ?? text, identity.agent, native.id);
          if (evidence) return Object.freeze({ route: opencodeInteractiveRoute, sessionID: native.id, assistantText: evidence.text, runtime: evidence.runtime, history: current });
          await delay(250, promptSignal);
        }
        throw new Error(`OpenCode interactive prompt did not expose a fresh completed assistant turn with exact runtime evidence for ${native.id}`);
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        checkAbort(statusSignal);
        if (closed || terminal?.isExited) return { kind: "terminated", runtimeId: native.id };
        return { kind: "unknown", detail: "OpenCode interactive TUI exposes no reliable native quiescence state" };
      },
      history: async (historySignal: AbortSignal): Promise<readonly unknown[]> => {
        checkAbort(historySignal);
        return store.history(native.id, historySignal).catch(() => store.export(native.id, historySignal));
      },
      close,
      evidence: async () => Object.freeze({
        route: opencodeInteractiveRoute,
        command: [opencodeBin, ...command],
        native_session: native,
        native_source: located.source,
        runtime_binding: binding,
        initial_history: initialHistory,
        terminal: terminal?.snapshot(),
        cleanup_failure_history: Object.freeze([...cleanupFailures]),
      }),
      get ownedProcessIds(): readonly number[] { return terminal ? Object.freeze([terminal.processId]) : Object.freeze([]); },
      get cleanupFailureHistory(): readonly string[] { return Object.freeze([...cleanupFailures]); },
    });
  } catch (error) {
    const failures: unknown[] = [error];
    try { await terminal?.close(); } catch (cleanupError) { failures.push(cleanupError); }
    try { await redis?.quit(); } catch (cleanupError) { failures.push(cleanupError); }
    try { if (ownedProfile) await cleanupPaths(ownedProfile.paths); } catch (cleanupError) { failures.push(cleanupError); }
    try { if (cwd) await rm(cwd, { recursive: true, force: true }); } catch (cleanupError) { failures.push(cleanupError); }
    if (failures.length > 1) throw new AggregateError(failures, "OpenCode interactive launch failed");
    throw error;
  }
};

const routeSpec: RouteSpec = Object.freeze({
  id: opencodeInteractiveRoute,
  host: "opencode",
  modelBacked: true,
  availability: { kind: "setup_gap" as const, detail: "OpenCode interactive PTY preflight not run" },
});

export const createOpenCodeInteractiveAdapter = (): RouteAdapter => Object.freeze({
  spec: routeSpec,
  preflight,
  launch: (input, signal) => startOpenCodeInteractiveParticipant(input, signal),
});

export const openCodeInteractiveRouteSource = Object.freeze({
  binary: opencodeBin,
  route: opencodeInteractiveRoute,
  native_command: "opencode <private-workdir> --port <owned-port> --hostname 127.0.0.1 --model <model> --agent build --auto",
  history_sources: Object.freeze(["owned HTTP session API", "private OpenCode database/export"]),
});
