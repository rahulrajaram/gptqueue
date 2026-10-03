import { access, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { createRegisteredPiExtension } from "../../src/registered-shell/pi-extension.js";
import type { Availability, ModelParticipant, Participant, RouteAdapter, RouteSpec, RuntimeStatus } from "./qualification-types.js";
import { validatePiRedisUrl } from "./qualification-pi.js";
import { homePath, nodePrefixPath } from "./local-tools.js";

export const piNativeChildQualificationRoute = "pi-native-child" as const;
const installed = nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist");
const repo = resolve(import.meta.dirname, "../..");
// Test-only override: PI_SUBAGENTS_SOURCE points at a pi-subagents extension
// checkout's src/index.ts. Defaults to a generic user-local extension path.
const subagentsSource = process.env.PI_SUBAGENTS_SOURCE ?? homePath(".pi/agent/extensions/pi-subagents/src/index.ts");
const subagentsRoot = resolve(dirname(subagentsSource), "..");
type Json = Record<string, any>;
type LaunchInput = Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>;
type ChildOptions = Readonly<{ installedRoot?: string; workspaceRoot?: string; provider?: string; model?: string; profileRoot?: string; modelRuntime?: unknown }>;
type PiSession = any;
type ChildBinding = Readonly<{ agent: string; runtimeId: string; epoch: string; cwd: string }>;
export type PiNativeChildParticipant = ModelParticipant & Readonly<{ provenance: Readonly<{ sessionId: string; sessionFileHash: string; profileHash: string; provider: string; model: string; parentSessionHash: string; nativeChildId: string }> }>;
export type PiNativeChildCleanupEvidence = Readonly<{ status: "not_started" | "attempted" | "completed" | "failed"; steps: readonly Json[] }>;
const cleanupEvidence = new WeakMap<object, PiNativeChildCleanupEvidence>();
export const getPiNativeChildCleanupEvidence = (participant: PiNativeChildParticipant): PiNativeChildCleanupEvidence =>
  cleanupEvidence.get(participant) ?? Object.freeze({ status: "not_started" as const, steps: [] });

/** Prefix used only for adapter-to-child control tasks. Peer traffic remains ordinary GPTQueue tasks. */
export const PI_NATIVE_CHILD_CONTROL_PREFIX = "GPTQUEUE_NATIVE_CHILD_CONTROL_V1:";
type NativeChildControl = Readonly<{ kind: "controller_prompt"; prompt: string }>;

let startupTail: Promise<void> = Promise.resolve();
const withSerializedStartup = async <T>(work: () => Promise<T>): Promise<T> => {
  const predecessor = startupTail;
  let release!: () => void;
  startupTail = new Promise<void>(resolveRelease => { release = resolveRelease; });
  await predecessor;
  try { return await work(); } finally { release(); }
};

let activeProfileScope: { readonly token: symbol; readonly prior: string | undefined; readonly profile: string } | undefined;
export const enterPiNativeChildProfileScope = (profile: string): (() => void) => {
  if (activeProfileScope) throw new Error("Pi native-child owned profile scope is already active");
  const scope = { token: Symbol("pi-native-child-profile"), prior: process.env.PI_CODING_AGENT_DIR, profile: resolve(profile) } as const;
  activeProfileScope = scope;
  process.env.PI_CODING_AGENT_DIR = scope.profile;
  return () => {
    if (activeProfileScope?.token !== scope.token) throw new Error("Pi native-child owned profile scope is stale or already released");
    if (scope.prior === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = scope.prior;
    activeProfileScope = undefined;
  };
};

const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const hashPath = (value: string): string => hash(resolve(value));
const delay = (ms: number, signal: AbortSignal): Promise<void> => new Promise((resolveDelay, reject) => {
  if (signal.aborted) { reject(signal.reason instanceof Error ? signal.reason : new Error("Pi child operation aborted")); return; }
  const timer = setTimeout(resolveDelay, ms);
  signal.addEventListener("abort", () => { clearTimeout(timer); reject(signal.reason instanceof Error ? signal.reason : new Error("Pi child operation aborted")); }, { once: true });
});
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("Pi child operation aborted"); };
const workspace = (options: ChildOptions, input: LaunchInput): string => resolve(options.workspaceRoot ?? join(repo, ".gptqueue/qualification/pi-native-child"), hash(input.pairId).slice(0, 16), input.role, hash(input.nonce).slice(0, 16));
const routeSpec: RouteSpec = Object.freeze({ id: piNativeChildQualificationRoute, host: "pi", modelBacked: true, availability: { kind: "setup_gap" as const, detail: "Pi native-child route preflight not run" } });

const loadSubagents = async (): Promise<unknown> => {
  const module = await import(pathToFileURL(join(installed, "../node_modules/jiti/lib/jiti.mjs")).href) as any;
  const createJiti = module.createJiti as (url: string, options: Json) => { import: (path: string) => Promise<unknown> };
  const loader = createJiti(import.meta.url, { interopDefault: true, alias: {
    "@earendil-works/pi-coding-agent": join(installed, "index.js"),
    "@earendil-works/pi-tui": nodePrefixPath("lib/node_modules/@earendil-works/pi-tui/dist/index.js"),
    "@sinclair/typebox": join(subagentsRoot, "node_modules/@sinclair/typebox/build/cjs/index.js"),
  } });
  const loaded = await loader.import(subagentsSource) as any;
  return loaded.default ?? loaded;
};
const toolValue = (result: any): any => result?.details?.structuredContent ?? result?.structuredContent ?? result?.details ?? result;
const tool = async (session: PiSession, name: string, args: Json, signal: AbortSignal): Promise<any> => {
  const definition = session.agent.state.tools.find((item: any) => item.name === name);
  if (!definition) throw new Error(`Pi native-child tool ${name} is unavailable`);
  const result = await definition.execute(`pi-native-child-${randomUUID()}`, args, signal);
  if (result?.isError) throw new Error(`${name} failed`);
  return toolValue(result);
};
const registryChild = async (redis: Redis, before: ReadonlySet<string>, parent: string, cwd: string, signal: AbortSignal): Promise<string> => {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const registry = await redis.hgetall(SESSION_KEYS.registry);
    const child = Object.entries(registry).find(([name, raw]) => {
      if (before.has(name) || name === parent) return false;
      const metadata = object(object(JSON.parse(raw))?.metadata);
      return metadata?.client === "pi" && metadata.working_directory === cwd;
    })?.[0];
    if (child) return child;
    await delay(250, signal);
  }
  throw new Error(`Pi native child did not register under the exact parent cwd ${cwd}`);
};
const childSession = async (sessionDir: string, parentFile: string, signal: AbortSignal): Promise<Readonly<{ id: string; file: string; parent: string }>> => {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const files = await readdir(sessionDir).catch(() => [] as string[]);
    for (const name of files.filter(file => file.endsWith(".jsonl"))) {
      const file = join(sessionDir, name);
      const first = (await readFile(file, "utf8")).split("\n")[0];
      if (!first) continue;
      try {
        const header = JSON.parse(first) as Json;
        if (header.type === "session" && header.parentSession === parentFile && nonEmpty(header.id)) return { id: header.id, file, parent: header.parentSession };
      } catch { /* file is still being written */ }
    }
    await delay(250, signal);
  }
  throw new Error("Pi native child session did not expose exact parentSession lineage");
};
const childBinding = async (redis: Redis, agent: string, cwd: string, sessionId: string, signal: AbortSignal): Promise<ChildBinding> => {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const raw = await redis.hget(SESSION_KEYS.registry, agent);
    const metadata = object(object(raw ? JSON.parse(raw) : undefined)?.metadata);
    const binding = object(JSON.parse(await redis.get(`gptq:runtime-binding:${agent}`) ?? "null"));
    if (metadata?.working_directory === cwd && binding?.runtime_id === sessionId && binding.working_directory === cwd && nonEmpty(binding.epoch)) {
      return Object.freeze({ agent, runtimeId: sessionId, epoch: binding.epoch, cwd });
    }
    await delay(250, signal);
  }
  throw new Error("Pi native child registry/runtime binding did not match exact child session/cwd");
};
const parsedHistory = async (file: string): Promise<readonly unknown[]> => (await readFile(file, "utf8")).split("\n").flatMap(line => {
  try { return line ? [JSON.parse(line)] : []; } catch { return []; }
});

export const encodePiNativeChildControl = (prompt: string): string => `${PI_NATIVE_CHILD_CONTROL_PREFIX}${JSON.stringify({ kind: "controller_prompt", prompt })}`;
export const decodePiNativeChildControl = (content: unknown): NativeChildControl | undefined => {
  if (typeof content !== "string" || !content.startsWith(PI_NATIVE_CHILD_CONTROL_PREFIX)) return undefined;
  try {
    const value = JSON.parse(content.slice(PI_NATIVE_CHILD_CONTROL_PREFIX.length)) as Record<string, unknown>;
    return value.kind === "controller_prompt" && typeof value.prompt === "string"
      ? Object.freeze({ kind: "controller_prompt", prompt: value.prompt })
      : undefined;
  } catch { return undefined; }
};

const nestedObjects = (value: unknown): readonly Json[] => {
  if (Array.isArray(value)) return value.flatMap(nestedObjects);
  const current = object(value);
  return current ? [current, ...Object.values(current).flatMap(nestedObjects)] : [];
};

const redactNativeDiagnostic = (value: unknown): unknown => {
  if (typeof value === "string") return value.replace(/(?:token|secret|password|authorization|api[_-]?key)\s*[:=]\s*[^\s,}]+/giu, "[redacted]");
  if (Array.isArray(value)) return value.map(redactNativeDiagnostic);
  const current = object(value);
  if (!current) return value;
  return Object.fromEntries(Object.entries(current).map(([key, item]) => [
    key,
    /token|secret|password|authorization|api[_-]?key/iu.test(key) ? "[redacted]" : redactNativeDiagnostic(item),
  ]));
};

/** Classify only states exposed by the native host; a running subagent is not proof of idle. */
export const classifyPiNativeChildStatus = (value: unknown, closed: boolean, runtimeId: string): RuntimeStatus => {
  if (closed) return { kind: "terminated", runtimeId };
  const statuses = nestedObjects(value).flatMap(candidate => [candidate.status, candidate.state, candidate.lifecycle]);
  if (statuses.includes("completed") || statuses.includes("failed") || statuses.includes("error") || statuses.includes("aborted") || statuses.includes("stopped")) {
    return { kind: "terminated", runtimeId };
  }
  if (statuses.includes("busy") || statuses.includes("running") || statuses.includes("in_progress")) {
    return { kind: "unknown", detail: "Pi native Agent reports an active child, but does not expose turn quiescence" };
  }
  if (statuses.includes("idle")) return { kind: "idle", runtimeId };
  return { kind: "unknown", detail: "Pi native Agent did not expose a trustworthy lifecycle state" };
};

export const startPiNativeChildParticipant = async (options: ChildOptions, input: LaunchInput, signal: AbortSignal): Promise<PiNativeChildParticipant> => {
  validatePiRedisUrl(input.redisUrl);
  if (!nonEmpty(options.provider) || !nonEmpty(options.model)) throw new Error("Pi native-child qualification requires explicit provider and model options");
  const profileRoot = options.profileRoot && resolve(options.profileRoot);
  if (!profileRoot || activeProfileScope?.profile !== profileRoot || process.env.PI_CODING_AGENT_DIR !== profileRoot || !options.modelRuntime) {
    throw new Error("Pi native-child qualification requires an active owned profile scope and preloaded model runtime");
  }
  const root = options.installedRoot ?? installed;
  const cwd = workspace(options, input);
  const profile = profileRoot;
  const sessionDir = join(cwd, "sessions");
  const childConfigDir = join(cwd, ".pi", "agents");
  const childExtension = join(cwd, "gptqueue-evaluation.ts");
  const priorCwd = process.cwd();
  const redis = new Redis(input.redisUrl, { maxRetriesPerRequest: 3, retryStrategy: () => null });
  let parent: PiSession | undefined;
  let childToolId = "";
  let childAgent = "";
  let childFile = "";
  const launchEvidence: Json = {
    cwd,
    profile: resolve(profile),
    session_dir: resolve(sessionDir),
    agent_tool: "Agent",
    subagent_type: "general-purpose",
    model: `${options.provider}/${options.model}`,
    run_in_background: true,
    max_turns: 64,
    pi_coding_agent_dir: resolve(profile),
    profile_selection: "owned process-lifetime PI_CODING_AGENT_DIR scope",
    child_profile_inheritance: "installed pi-subagents child uses the owned process profile and this workspace agent override; parent inline extensionFactories are not inherited",
    child_extension: resolve(childExtension),
    child_agent_config: join(childConfigDir, "general-purpose.md"),
  };
  let closed = false;
  let closing: Promise<void> | undefined;
  try {
    await mkdir(profile, { recursive: true }); await mkdir(sessionDir, { recursive: true }); await mkdir(childConfigDir, { recursive: true });
    await writeFile(childExtension,
      `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(repo, "dist/registered-shell/pi-extension.js")).href)};\n` +
      `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl: input.redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") })});\n`, { mode: 0o600 });
    await writeFile(join(childConfigDir, "general-purpose.md"), [
      "---",
      "name: general-purpose",
      "extensions: ./gptqueue-evaluation.ts",
      "tools: ext:gptqueue-evaluation",
      "skills: none",
      "persist_session: true",
      "session_dir: ./sessions",
      "output_transcript: false",
      "max_turns: 64",
      "---",
      "You are the owned GPTQueue native child. Use only the private GPTQueue extension tools that are loaded for this workspace. Do not use shell, files, or any other extension.",
    ].join("\n") + "\n", { mode: 0o600 });
    const sdk = await import(pathToFileURL(join(root, "index.js")).href) as any;
    const modelRuntime: any = options.modelRuntime ?? await sdk.ModelRuntime.create({ allowModelNetwork: false });
    const model = modelRuntime.getModel(options.provider, options.model);
    if (!model || !modelRuntime.hasConfiguredAuth(options.provider)) throw new Error("Pi native-child model/auth is unavailable");
    const subagents = await loadSubagents();
    const settings = sdk.SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: profile, settingsManager: settings, noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      systemPrompt: "You are a Pi native-child parent. Use only GPTQueue and the installed native Agent tool.", extensionFactories: [createRegisteredPiExtension({ redisUrl: input.redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") }), subagents] });
    const sessionManager = sdk.SessionManager.create(cwd, sessionDir);
    const before = new Set(Object.keys(await redis.hgetall(SESSION_KEYS.registry)));
    await withSerializedStartup(async () => {
      process.chdir(cwd);
      try {
        await loader.reload();
        ({ session: parent } = await sdk.createAgentSession({ cwd, agentDir: profile, modelRuntime, model, thinkingLevel: "low", settingsManager: settings, sessionManager, resourceLoader: loader, noTools: "builtin" }));
        await parent.bindExtensions({ onError: () => undefined });
      } finally { process.chdir(priorCwd); }
    });
    const parentStatus = await tool(parent, "get_runtime_status", {}, signal);
    const parentAgent = String(parentStatus.agent);
    const parentSessionFile = String(parent.sessionManager.getSessionFile());
    const agentTool = parent.agent.state.tools.find((item: any) => item.name === "Agent");
    if (!agentTool) throw new Error("Pi native Agent tool is unavailable");
    const startedRaw = await withSerializedStartup(async () => {
      const launchCwd = process.cwd();
      process.chdir(cwd);
      try {
        return await agentTool.execute(`pi-native-child-${randomUUID()}`, { description: "native child qualification", name: `native-child-${input.nonce.slice(-8)}`, subagent_type: "general-purpose", model: `${options.provider}/${options.model}`, run_in_background: true, max_turns: 64,
      prompt: `You are the actual native Pi child. Use only the available GPTQueue tools; do not use shell, files, or other extensions. Call get_runtime_status once, then wait for work by calling receive_message with timeout=60. If receive_message returns no_messages, call it again; do not spin on empty polling. A controller task has content beginning exactly ${PI_NATIVE_CHILD_CONTROL_PREFIX}; parse the JSON after that prefix, perform its prompt as your own model work, and send one result containing your actual assistant answer to task.from with type result, in_reply_to equal to task.id, and idempotency_key equal to task.id. A normal task is qualified peer traffic: perform the requested work yourself and send one result to task.from with the requested answer and the same correlation fields. receive_message is the intentional legacy consumption path here, so do not call acknowledge_tasks for these messages. Never echo a controller instruction merely because it was supplied by the controller. Continue waiting.` }, signal);
      } finally { process.chdir(launchCwd); }
    });
    launchEvidence.agent_tool_result = redactNativeDiagnostic(startedRaw);
    const started = toolValue(startedRaw);
    childToolId = String(started?.agentId ?? started?.agent_id ?? JSON.stringify(startedRaw).match(/Agent ID:\s*([^\\"\s]+)/)?.[1] ?? "");
    if (!childToolId) throw new Error("Pi native Agent did not return a child ID");
    childAgent = await registryChild(redis, before, parentAgent, cwd, signal);
    launchEvidence.parent_agent = parentAgent;
    launchEvidence.child_agent = childAgent;
    const lineage = await childSession(sessionDir, parentSessionFile, signal);
    childFile = lineage.file;
    launchEvidence.child_session_id = lineage.id;
    launchEvidence.child_session_file = lineage.file;
    const binding = await childBinding(redis, childAgent, cwd, lineage.id, signal);
    launchEvidence.runtime_binding = binding;
    if (binding.runtimeId !== lineage.id || lineage.parent !== parentSessionFile) throw new Error("Pi native child lineage/runtime identity mismatch");
    const childAgentDir = typeof sdk.getAgentDir === "function" ? String(sdk.getAgentDir()) : "existing-default-agent-dir";
    launchEvidence.child_agent_dir = "existing-auth-profile:" + hash(resolve(childAgentDir));
    const profileHash = hash(JSON.stringify({ profile: resolve(childAgentDir), cwd: resolve(cwd), sessionDir: resolve(sessionDir), nativeTool: "Agent", subagentsSource }));
    const identity = Object.freeze({ participantId: `${piNativeChildQualificationRoute}:${lineage.id}`, route: piNativeChildQualificationRoute, hostRuntimeId: lineage.id, agent: binding.agent, cwdHash: hashPath(cwd), profileHash, epochHash: hash(binding.epoch) });
    const cleanupState: { status: PiNativeChildCleanupEvidence["status"]; steps: Json[] } = { status: "not_started", steps: [] };
    const cleanup = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      cleanupState.status = "attempted";
      const failures: string[] = [];
      const step = async (name: string, action: () => Promise<unknown> | unknown): Promise<unknown> => {
        try {
          const value = await action();
          cleanupState.steps.push({ name, status: "fulfilled", observation: redactNativeDiagnostic(value) });
          return value;
        } catch (error) {
          const detail = String(error);
          cleanupState.steps.push({ name, status: "rejected", error: detail });
          failures.push(`${name}: ${detail}`);
          return undefined;
        }
      };
      if (parent && childToolId) {
        await step("abort_subagent", () => tool(parent!, "abort_subagent", { agent_id: childToolId, reason: "qualification cleanup" }, AbortSignal.timeout(10_000)));
        let lifecycle: RuntimeStatus = { kind: "unknown", detail: "Pi native Agent lifecycle status was unavailable" };
        let activeObserved = false;
        const lifecycleDeadline = Date.now() + 10_000;
        do {
          const observation = await step("get_subagent_result", () => tool(parent!, "get_subagent_result", { agent_id: childToolId, wait: false, verbose: false }, AbortSignal.timeout(2_000)));
          if (observation === undefined) break;
          lifecycle = classifyPiNativeChildStatus(observation, false, lineage.id);
          activeObserved ||= lifecycle.kind === "unknown" && lifecycle.detail?.includes("active") === true;
          if (lifecycle.kind === "terminated" || !activeObserved) break;
          await new Promise(resolveDelay => setTimeout(resolveDelay, 100));
        } while (Date.now() < lifecycleDeadline);
        cleanupState.steps.push({ name: "child_lifecycle_terminal_check", status: lifecycle.kind === "terminated" ? "fulfilled" : "unknown", observation: lifecycle });
        if (activeObserved && lifecycle.kind !== "terminated") failures.push("child_lifecycle_terminal_check: native child remained active after abort");
      }
      await step("parent.abort", () => parent?.abort());
      await step("session_shutdown", () => parent?.extensionRunner?.emit({ type: "session_shutdown" }));
      await step("parent.dispose", () => parent?.dispose());
      await step("redis.quit", () => redis.quit());
      await step("workspace.remove", () => rm(cwd, { recursive: true, force: true }));
      cleanupState.status = failures.length > 0 ? "failed" : "completed";
      if (failures.length > 0) throw new Error(`Pi native child cleanup failed: ${failures.join("; ")}`);
    };
    const close = async (): Promise<void> => { if (!closing) closing = cleanup(); return closing; };
    const participant = Object.freeze({
      kind: "model" as const, identity,
      provenance: Object.freeze({ sessionId: lineage.id, sessionFileHash: hash(await readFile(childFile, "utf8")), profileHash, provider: options.provider, model: options.model, parentSessionHash: hash(parentSessionFile), nativeChildId: childToolId }),
      prompt: async (text: string, promptSignal: AbortSignal) => {
        checkAbort(promptSignal); if (closed) throw new Error(`Pi native child ${lineage.id} is terminated`);
        const sent = await tool(parent, "send_message", { to: childAgent, type: "task", content: encodePiNativeChildControl(text), idempotency_key: `${input.nonce}:${randomUUID()}` }, promptSignal);
        const requestId = String(sent.message_id);
        const deadline = Date.now() + 300_000;
        while (Date.now() < deadline) {
          checkAbort(promptSignal);
          const claim = await tool(parent, "claim_tasks", { max_batch: 1 }, promptSignal).catch(() => undefined);
          const tasks = Array.isArray(claim?.claim?.tasks) ? claim.claim.tasks.map((value: unknown) => { try { return typeof value === "string" ? JSON.parse(value) : value; } catch { return undefined; } }).filter((value: any) => value?.payload?.in_reply_to === requestId) : [];
          if (tasks[0]) {
            await tool(parent, "acknowledge_tasks", { claim_id: claim.claim.claim_id }, promptSignal);
            const content = tasks[0].payload?.content;
            return typeof content === "string" ? content : content;
          }
          await delay(500, promptSignal);
        }
        throw new Error(`Pi native child ${lineage.id} did not reply to task ${requestId}`);
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        checkAbort(statusSignal);
        if (closed) return { kind: "terminated", runtimeId: lineage.id };
        try {
          const native = await tool(parent, "get_subagent_result", { agent_id: childToolId, wait: false, verbose: false }, statusSignal);
          return classifyPiNativeChildStatus(native, false, lineage.id);
        } catch { return { kind: "unknown", detail: "Pi native Agent lifecycle status was unavailable" }; }
      },
      history: async (historySignal: AbortSignal) => { checkAbort(historySignal); return parsedHistory(childFile); },
      close,
    });
    cleanupEvidence.set(participant, cleanupState);
    return participant;
  } catch (error) {
    const cleanupFailures: string[] = [];
    const cleanupStep = async (name: string, action: () => Promise<unknown> | unknown): Promise<void> => {
      try { launchEvidence[`cleanup_${name}`] = redactNativeDiagnostic(await action()); }
      catch (cleanupError) { const detail = String(cleanupError); launchEvidence[`cleanup_${name}_error`] = detail; cleanupFailures.push(`${name}: ${detail}`); }
    };
    if (parent && childToolId) {
      launchEvidence.child_lifecycle_before_cleanup = await tool(parent, "get_subagent_result", { agent_id: childToolId, wait: false, verbose: false }, AbortSignal.timeout(10_000))
        .then(redactNativeDiagnostic).catch(lifecycleError => ({ lifecycle_error: String(lifecycleError) }));
      await cleanupStep("abort_subagent", () => tool(parent!, "abort_subagent", { agent_id: childToolId, reason: "qualification setup failure" }, AbortSignal.timeout(10_000)));
    }
    launchEvidence.registry_before_cleanup = await redis.hgetall(SESSION_KEYS.registry).then(redactNativeDiagnostic).catch(registryError => ({ registry_error: String(registryError) }));
    await cleanupStep("parent_abort", () => parent?.abort());
    await cleanupStep("session_shutdown", () => parent?.extensionRunner?.emit({ type: "session_shutdown" }));
    await cleanupStep("parent_dispose", () => parent?.dispose());
    await cleanupStep("redis_quit", () => redis.quit());
    await cleanupStep("workspace_remove", () => rm(cwd, { recursive: true, force: true }));
    launchEvidence.cleanup_status = cleanupFailures.length > 0 ? "failed" : "completed";
    if (cleanupFailures.length > 0) launchEvidence.cleanup_failures = cleanupFailures;
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${message}; native_launch_evidence=${JSON.stringify(redactNativeDiagnostic(launchEvidence))}`);
  }
};

const preflight = (options: ChildOptions, signal: AbortSignal): Promise<Availability> => (async () => {
  checkAbort(signal);
  const root = options.installedRoot ?? installed;
  try { await access(join(root, "index.js"), constants.R_OK); await access(subagentsSource, constants.R_OK); await access(join(repo, "dist/registered-shell/pi-extension.js"), constants.R_OK); }
  catch { return { kind: "setup_gap", detail: "Pi SDK, installed pi-subagents Agent tool, or built GPTQueue extension is unavailable" }; }
  return { kind: "available" };
})();

export const createPiNativeChildAdapter = (options: ChildOptions = {}): RouteAdapter => Object.freeze({
  spec: routeSpec,
  preflight: signal => preflight(options, signal),
  launch: (input, signal): Promise<Participant> => startPiNativeChildParticipant(options, input, signal),
});
