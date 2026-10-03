import { access, mkdir, rm, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import type { Availability, ModelParticipant, Participant, RouteAdapter, RouteSpec, RuntimeStatus } from "./qualification-types.js";
import { createRegisteredPiExtension } from "../../src/registered-shell/pi-extension.js";
import { nodePrefixPath } from "./local-tools.js";

export const piQualificationRoute = "pi-rpc-cli" as const;
export const piSdkQualificationRoute = "pi-sdk" as const;
const installed = nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist");
const repo = resolve(import.meta.dirname, "../..");
type Json = Record<string, unknown>;
type LaunchInput = Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>;
type PiState = Readonly<{ sessionId?: unknown; sessionFile?: unknown; isStreaming?: unknown; model?: unknown }>;
type PiRpcClient = Readonly<{
  start: () => Promise<void>;
  stop: () => Promise<void>;
  getState: () => Promise<PiState>;
  getMessages: () => Promise<unknown>;
  promptAndWait: (message: string, images?: unknown, timeout?: number) => Promise<unknown>;
  abort?: () => Promise<void>;
  onEvent: (listener: (event: unknown) => void) => unknown;
}>;
type PiOptions = Readonly<{
  installedRoot?: string;
  workspaceRoot?: string;
  model?: string;
  provider?: string;
  extensionPath?: string;
}>;
export type PiRpcParticipant = ModelParticipant & Readonly<{ provenance: Readonly<{ sessionId: string; sessionFileHash: string; profileHash: string; provider: string; model: string }> }>;

export type PiTurnEvidenceExpectation = Readonly<{ marker: string; agent: string; runtimeId: string }>;

const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const exactModel = (value: unknown): Readonly<{ provider: string; model: string }> | undefined => {
  const candidate = object(value);
  if (!candidate || !nonEmpty(candidate.provider) || !nonEmpty(candidate.id)) return undefined;
  return Object.freeze({ provider: candidate.provider, model: candidate.id });
};
const parsedObjects = (value: unknown): readonly Json[] => {
  if (typeof value === "string") {
    try { return parsedObjects(JSON.parse(value)); } catch { return []; }
  }
  if (Array.isArray(value)) return value.flatMap(parsedObjects);
  const current = object(value);
  if (!current) return [];
  return [current, ...Object.values(current).flatMap(parsedObjects)];
};

/** Require an assistant text marker and a correlated successful runtime tool result. */
export const hasPiAssistantRuntimeEvidence = (history: unknown, expected: PiTurnEvidenceExpectation): boolean => {
  if (!Array.isArray(history)) return false;
  const messages = history.map(object).filter((value): value is Json => value !== undefined);
  const assistantTurns = messages.filter(message => message.role === "assistant");
  const markerSeen = assistantTurns.some(message => Array.isArray(message.content) && message.content.some(block => {
    const current = object(block);
    return current?.type === "text" && typeof current.text === "string" && current.text.includes(expected.marker);
  }));
  if (!markerSeen) return false;
  const calls = assistantTurns.flatMap(message => Array.isArray(message.content) ? message.content.map(object).filter((value): value is Json => value !== undefined) : [])
    .filter(call => call.type === "toolCall" && call.name === "get_runtime_status" && nonEmpty(call.id));
  return calls.some(call => messages.some(message => {
    if (message.role !== "toolResult" || message.toolCallId !== call.id || message.toolName !== "get_runtime_status") return false;
    return parsedObjects(message).some(candidate => {
      const runtime = object(candidate.runtime);
      return candidate.status === "ok" && candidate.agent === expected.agent && runtime?.runtime_id === expected.runtimeId && runtime.client === "pi";
    });
  }));
};
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const hashPath = (value: string): string => hash(resolve(value));
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("Pi qualification operation aborted");
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw abortError(signal); };
const withAbort = <T>(promise: Promise<T>, signal: AbortSignal): Promise<T> => new Promise<T>((resolvePromise, reject) => {
  if (signal.aborted) { reject(abortError(signal)); return; }
  const abort = () => reject(abortError(signal));
  signal.addEventListener("abort", abort, { once: true });
  promise.then(value => { signal.removeEventListener("abort", abort); resolvePromise(value); }, error => { signal.removeEventListener("abort", abort); reject(error); });
});
const delay = (ms: number, signal: AbortSignal): Promise<void> => new Promise((resolveDelay, reject) => {
  checkAbort(signal);
  const timer = setTimeout(resolveDelay, ms);
  const abort = () => { clearTimeout(timer); reject(abortError(signal)); };
  signal.addEventListener("abort", abort, { once: true });
  setTimeout(() => signal.removeEventListener("abort", abort), ms + 1);
});

export const validatePiRedisUrl = (value: string): void => {
  let parsed: URL;
  try { parsed = new URL(value); } catch { throw new Error("Pi qualification requires a valid Redis URL"); }
  if (parsed.protocol !== "redis:" || !["127.0.0.1", "localhost", "::1", "[::1]"].includes(parsed.hostname) || parsed.pathname !== "/15" || parsed.username || parsed.password) {
    throw new Error("Pi qualification requires an unauthenticated loopback Redis db15 URL");
  }
};

const workspace = (options: PiOptions, input: LaunchInput): string => resolve(options.workspaceRoot ?? join(repo, ".gptqueue/qualification/pi"), hash(input.pairId).slice(0, 16), input.role, hash(input.nonce).slice(0, 16));
const routeSpec: RouteSpec = Object.freeze({ id: piQualificationRoute, host: "pi", modelBacked: true, availability: { kind: "setup_gap" as const, detail: "Pi RPC route preflight not run" } });
const bindingFor = async (redis: Redis, cwd: string, sessionId: string, signal: AbortSignal): Promise<Readonly<{ agent: string; epoch: string; runtimeId: string; workingDirectory: string }>> => {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const registry = await redis.hgetall(SESSION_KEYS.registry);
    for (const [agent, raw] of Object.entries(registry)) {
      const value = object(JSON.parse(raw));
      const metadata = object(value?.metadata);
      if (metadata?.client !== "pi" || metadata.working_directory !== cwd) continue;
      const binding = object(JSON.parse(await redis.get(`gptq:runtime-binding:${agent}`) ?? "null"));
      if (binding?.client === "pi" && binding.runtime_id === sessionId && binding.working_directory === cwd && nonEmpty(binding.epoch)) {
        return Object.freeze({ agent, epoch: binding.epoch, runtimeId: binding.runtime_id, workingDirectory: cwd });
      }
    }
    await delay(250, signal);
  }
  throw new Error(`Pi RPC session ${sessionId} did not expose an exact registry/runtime binding for ${cwd}`);
};

const rpcModule = async (root: string): Promise<{ RpcClient: new (options: Record<string, unknown>) => PiRpcClient }> => {
  const module = await import(pathToFileURL(join(root, "modes/rpc/rpc-client.js")).href) as { RpcClient?: new (options: Record<string, unknown>) => PiRpcClient };
  if (!module.RpcClient) throw new Error("Installed Pi RPC client is unavailable");
  return { RpcClient: module.RpcClient };
};

type SdkTool = Readonly<{ name?: unknown; execute?: (id: string, args: unknown, signal: AbortSignal) => Promise<unknown> }>;
type SdkSession = {
  readonly sessionManager: Readonly<{ getSessionId: () => unknown; getSessionFile: () => unknown }>;
  readonly agent?: Readonly<{ state?: Readonly<{ tools?: readonly SdkTool[] }> }>;
  readonly model?: unknown;
  readonly state?: Readonly<{ model?: unknown; messages?: unknown; isStreaming?: unknown }>;
  readonly isStreaming?: boolean;
  bindExtensions: (options: Readonly<{ onError: () => void }>) => Promise<void> | void;
  dispose: () => void;
  prompt: (text: string, options?: Record<string, unknown>) => Promise<unknown>;
};
type SdkModelRuntime = Readonly<{
  getModel: (provider: string, model: string) => unknown;
  hasConfiguredAuth?: (provider: string) => boolean;
}>;
type SdkRuntime = Readonly<{ session: SdkSession; dispose: () => Promise<void> }>;
type SdkModule = Readonly<{
  ModelRuntime: Readonly<{ create: (value: Record<string, unknown>) => Promise<SdkModelRuntime> }>;
  SettingsManager: Readonly<{ inMemory: (value: Record<string, unknown>) => unknown }>;
  SessionManager: Readonly<{ create: (cwd: string, sessionDir: string) => unknown }>;
  createAgentSessionServices: (value: Record<string, unknown>) => Promise<unknown>;
  createAgentSessionFromServices: (value: Record<string, unknown>) => Promise<Readonly<{ session: SdkSession; extensionsResult: unknown }>>;
  createAgentSessionRuntime: (factory: (value: Readonly<{ cwd: string; agentDir: string; sessionManager: unknown; sessionStartEvent?: unknown }>) => Promise<Readonly<{ session: SdkSession; extensionsResult: unknown; services: unknown; diagnostics: readonly unknown[] }>>, value: Readonly<{ cwd: string; agentDir: string; sessionManager: unknown }>) => Promise<SdkRuntime>;
}>;
const sdkModule = async (root: string): Promise<SdkModule> => import(pathToFileURL(join(root, "index.js")).href) as unknown as SdkModule;
const sdkModel = (value: unknown): Readonly<{ provider: string; model: string }> | undefined => exactModel(value);
const sdkToolValue = (value: unknown): unknown => {
  const current = object(value);
  return current?.structuredContent ?? (current?.details && object(current.details)?.structuredContent) ?? current?.content ?? value;
};
const sdkRuntimeBinding = (value: unknown): Readonly<{ agent: string; runtimeId: string; cwd: string }> | undefined => {
  for (const candidate of [value, sdkToolValue(value)]) {
    const current = object(candidate);
    const runtime = object(current?.runtime);
    if (current && nonEmpty(current.agent) && runtime && nonEmpty(runtime.runtime_id) && nonEmpty(runtime.working_directory)) {
      return Object.freeze({ agent: current.agent, runtimeId: runtime.runtime_id, cwd: resolve(runtime.working_directory) });
    }
  }
  return undefined;
};

export const startSdkParticipant = async (options: PiOptions, input: LaunchInput, signal: AbortSignal): Promise<PiRpcParticipant> => {
  validatePiRedisUrl(input.redisUrl);
  if (!nonEmpty(options.provider) || !nonEmpty(options.model)) throw new Error("Pi SDK qualification requires explicit provider and model options");
  const root = options.installedRoot ?? installed;
  const cwd = workspace(options, input);
  const sessionDir = join(cwd, "sessions");
  await mkdir(sessionDir, { recursive: true });
  const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
  const sdk = await sdkModule(root);
  const modelRuntime = await sdk.ModelRuntime.create({ allowModelNetwork: false });
  const modelValue = modelRuntime.getModel(options.provider, options.model);
  if (!modelValue) throw new Error(`Pi SDK model is unavailable: ${options.provider}/${options.model}`);
  if (modelRuntime.hasConfiguredAuth && !modelRuntime.hasConfiguredAuth(options.provider)) throw new Error(`Pi SDK auth is unavailable for ${options.provider}`);
  const settings = sdk.SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
  const sessionManager = sdk.SessionManager.create(cwd, sessionDir);
  const resourceLoaderOptions = {
    noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    systemPrompt: "You are an isolated Pi SDK qualification participant. Use only GPTQueue tools for qualification work.",
    extensionFactories: [createRegisteredPiExtension({ redisUrl: input.redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") })],
  };
  const redis = new Redis(input.redisUrl, { maxRetriesPerRequest: 3, retryStrategy: () => null });
  let session: SdkSession | undefined;
  let runtime: SdkRuntime | undefined;
  let cleaned = false;
  const cleanup = async (): Promise<void> => {
    if (cleaned) return;
    cleaned = true;
    let failure: unknown;
    try { if (runtime) await runtime.dispose(); else session?.dispose(); } catch (error) { failure = error; }
    try { await redis.quit(); } catch (error) { failure ??= error; }
    try { await rm(cwd, { recursive: true, force: true }); } catch (error) { failure ??= error; }
    if (failure) throw failure;
  };
  try {
    checkAbort(signal);
    const previousCwd = process.cwd();
    process.chdir(cwd);
    try {
      const services = await sdk.createAgentSessionServices({ cwd, agentDir, modelRuntime, settingsManager: settings, resourceLoaderOptions });
      runtime = await sdk.createAgentSessionRuntime(async ({ sessionManager: currentSessionManager, sessionStartEvent }) => {
        const created = await sdk.createAgentSessionFromServices({ services, sessionManager: currentSessionManager, sessionStartEvent, model: modelValue, thinkingLevel: "low", noTools: "builtin" });
        return { ...created, services, diagnostics: [] };
      }, { cwd, agentDir, sessionManager });
      session = runtime.session;
      await session.bindExtensions({ onError: () => undefined });
    } finally {
      process.chdir(previousCwd);
    }
    const sessionManagerValue = session.sessionManager;
    const sessionId = typeof sessionManagerValue.getSessionId === "function" ? sessionManagerValue.getSessionId() : undefined;
    const sessionFile = typeof sessionManagerValue.getSessionFile === "function" ? sessionManagerValue.getSessionFile() : undefined;
    if (!nonEmpty(sessionId) || !nonEmpty(sessionFile) || !resolve(sessionFile).startsWith(`${resolve(sessionDir)}/`)) throw new Error("Pi SDK session did not expose an exact private session ID/file");
    const tools = session.agent?.state?.tools;
    const statusTool = tools?.find(tool => tool.name === "get_runtime_status");
    if (!statusTool?.execute) throw new Error("Pi SDK runtime status tool is unavailable");
    const statusResult = await statusTool.execute(randomUUID(), {}, signal);
    const statusBinding = sdkRuntimeBinding(statusResult);
    if (!statusBinding || statusBinding.cwd !== resolve(cwd) || statusBinding.runtimeId !== sessionId) throw new Error("Pi SDK runtime status did not match exact native session identity/cwd");
    const registryBinding = await bindingFor(redis, cwd, sessionId, signal);
    if (registryBinding.runtimeId !== sessionId || registryBinding.agent !== statusBinding.agent || registryBinding.workingDirectory !== resolve(cwd)) throw new Error("Pi SDK registry/runtime binding did not match exact native session identity/agent/cwd");
    const actualModel = sdkModel(session.model ?? session.state?.model);
    if (!actualModel || actualModel.provider !== options.provider || actualModel.model !== options.model) throw new Error("Pi SDK session model did not match requested provider/model");
    const activeSession = session;
    let closed = false;
    let closing: Promise<void> | undefined;
    const profileHash = hash(JSON.stringify({ agentDir: "existing", sessionDir: resolve(sessionDir), noExtensions: true, noSkills: true, noContextFiles: true }));
    const identity = Object.freeze({ participantId: `${piSdkQualificationRoute}:${sessionId}`, route: piSdkQualificationRoute, hostRuntimeId: sessionId, agent: registryBinding.agent, cwdHash: hashPath(cwd), profileHash, epochHash: hash(registryBinding.epoch) });
    const close = async (): Promise<void> => {
      if (closing) return closing;
      closed = true;
      closing = cleanup();
      return closing;
    };
    return Object.freeze({
      kind: "model" as const, identity,
      provenance: Object.freeze({ sessionId, sessionFileHash: hashPath(sessionFile), profileHash, provider: actualModel.provider, model: actualModel.model }),
      prompt: async (text: string, promptSignal: AbortSignal) => {
        checkAbort(promptSignal);
        if (closed) throw new Error(`Pi SDK participant ${sessionId} is terminated`);
        return withAbort(activeSession.prompt(text, { source: "rpc" }), promptSignal);
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        if (closed) return { kind: "terminated", runtimeId: sessionId };
        const state = await withAbort(Promise.resolve(activeSession.state), statusSignal);
        return { kind: activeSession.isStreaming === true || state?.isStreaming === true ? "busy" : "idle", runtimeId: sessionId };
      },
      history: async (historySignal: AbortSignal) => withAbort(Promise.resolve(activeSession.state?.messages ?? []), historySignal),
      close,
    });
  } catch (error) {
    await cleanup().catch(() => undefined);
    throw error;
  }
};

const startParticipant = async (options: PiOptions, input: LaunchInput, signal: AbortSignal): Promise<PiRpcParticipant> => {
  validatePiRedisUrl(input.redisUrl);
  if (!nonEmpty(options.provider) || !nonEmpty(options.model)) {
    throw new Error("Pi RPC qualification requires explicit provider and model options");
  }
  const root = options.installedRoot ?? installed;
  const cwd = workspace(options, input);
  const sessionDir = join(cwd, "sessions");
  await mkdir(sessionDir, { recursive: true });
  const extension = options.extensionPath ?? join(cwd, "gptqueue-evaluation.ts");
  await writeFile(extension,
    `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(repo, "dist/registered-shell/pi-extension.js")).href)};\n` +
    `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl: input.redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") })});\n`, { mode: 0o600 });
  const { RpcClient } = await rpcModule(root);
  const client = new RpcClient({
    cliPath: join(root, "cli.js"), cwd,
    provider: options.provider,
    model: options.model,
    env: { ...process.env, PI_OFFLINE: "1", GPTQ_LOG_DIR: join(cwd, "lifecycle") },
    args: ["--offline", "--no-extensions", "--extension", extension, "--no-skills", "--no-prompt-templates",
      "--no-themes", "--no-context-files", "--no-builtin-tools", "--session-dir", sessionDir,
      "--append-system-prompt", `Use only the private GPTQueue tools. This is the ${input.role} participant for ${input.pairId}.`],
  });
  const redis = new Redis(input.redisUrl, { maxRetriesPerRequest: 3, retryStrategy: () => null });
  let closed = false;
  try {
    checkAbort(signal);
    await withAbort(client.start(), signal);
    const state = await withAbort(client.getState(), signal);
    const sessionId = nonEmpty(state.sessionId) ? state.sessionId : undefined;
    const sessionFile = nonEmpty(state.sessionFile) ? state.sessionFile : undefined;
    if (!sessionId || !sessionFile || !resolve(sessionFile).startsWith(`${resolve(sessionDir)}${process.platform === "win32" ? "\\" : "/"}`)) throw new Error("Pi RPC state did not report an exact private session ID/file");
    const actualModel = exactModel(state.model);
    if (!actualModel || actualModel.provider !== options.provider || actualModel.model !== options.model) {
      throw new Error("Pi RPC state did not report the requested exact provider/model");
    }
    const binding = await bindingFor(redis, cwd, sessionId, signal);
    const profileHash = hash(JSON.stringify({ agentDir: process.env.PI_CODING_AGENT_DIR ? "explicit-existing" : "default-existing", sessionDir: resolve(sessionDir), flags: ["--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-builtin-tools"] }));
    const identity = Object.freeze({ participantId: `${piQualificationRoute}:${sessionId}`, route: piQualificationRoute, hostRuntimeId: sessionId, agent: binding.agent, cwdHash: hashPath(cwd), profileHash, epochHash: hash(binding.epoch) });
    const close = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      let failure: unknown;
      try { await client.stop(); } catch (error) { failure = error; }
      try { await redis.quit(); } catch (error) { failure ??= error; }
      if (failure) throw failure;
    };
    return Object.freeze({
      kind: "model" as const, identity,
      provenance: Object.freeze({ sessionId, sessionFileHash: hashPath(sessionFile), profileHash, provider: actualModel.provider, model: actualModel.model }),
      prompt: async (text: string, promptSignal: AbortSignal) => {
        checkAbort(promptSignal);
        if (closed) throw new Error(`Pi RPC participant ${sessionId} is terminated`);
        try { return await withAbort(client.promptAndWait(text, undefined, 300_000), promptSignal); }
        catch (error) { if (promptSignal.aborted) await client.abort?.().catch(() => undefined); throw error; }
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        if (closed) return { kind: "terminated", runtimeId: sessionId };
        const current = await withAbort(client.getState(), statusSignal);
        if (current.sessionId !== sessionId || current.sessionFile !== sessionFile) throw new Error("Pi RPC status changed session identity");
        return { kind: current.isStreaming === true ? "busy" : "idle", runtimeId: sessionId };
      },
      history: async (historySignal: AbortSignal) => {
        const current = await withAbort(client.getState(), historySignal);
        if (current.sessionId !== sessionId || current.sessionFile !== sessionFile) throw new Error("Pi RPC history changed session identity");
        return withAbort(client.getMessages(), historySignal);
      },
      close,
    });
  } catch (error) {
    await client.stop().catch(() => undefined);
    await redis.quit().catch(() => undefined);
    await rm(cwd, { recursive: true, force: true }).catch(() => undefined);
    throw error;
  }
};

const preflight = (options: PiOptions, signal: AbortSignal): Promise<Availability> => (async () => {
  checkAbort(signal);
  const root = options.installedRoot ?? installed;
  const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
  try { await access(join(root, "cli.js"), constants.X_OK); await access(join(root, "modes/rpc/rpc-client.js"), constants.R_OK); await access(join(repo, "dist/registered-shell/pi-extension.js"), constants.R_OK); await access(join(agentDir, "auth.json"), constants.R_OK); }
  catch { return { kind: "setup_gap", detail: "Installed Pi RPC client or built GPTQueue extension is unavailable" }; }
  return { kind: "available" };
})();

export type PiAdapterSet = Readonly<{ adapters: readonly RouteAdapter[]; close: () => Promise<void> }>;
export const createPiRpcAdapter = (options: PiOptions = {}): RouteAdapter => Object.freeze({
  spec: routeSpec,
  preflight: signal => preflight(options, signal),
  launch: (input, signal): Promise<Participant> => startParticipant(options, input, signal),
});
export const createPiAdapters = (options: PiOptions = {}): PiAdapterSet => Object.freeze({ adapters: Object.freeze([createPiRpcAdapter(options)]), close: async () => undefined });
