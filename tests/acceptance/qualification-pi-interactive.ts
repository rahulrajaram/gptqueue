import { access, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Redis } from "ioredis";
import * as pty from "node-pty";
import { SESSION_KEYS } from "../../src/core/keys.js";
import type { Availability, ModelParticipant, Participant, RouteAdapter, RouteSpec, RuntimeStatus } from "./qualification-types.js";
import { validatePiRedisUrl } from "./qualification-pi.js";

export const piInteractiveQualificationRoute = "pi-interactive" as const;
const installed = "/home/rahul/nodeenv2251-311/lib/node_modules/@earendil-works/pi-coding-agent/dist";
const repo = resolve(import.meta.dirname, "../..");
type Json = Record<string, unknown>;
type LaunchInput = Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>;
type InteractiveOptions = Readonly<{ installedRoot?: string; workspaceRoot?: string; provider?: string; model?: string }>;
type Binding = Readonly<{ agent: string; runtimeId: string; epoch: string; cwd: string }>;
type PtyProcess = ReturnType<typeof pty.spawn>;
export type PiInteractiveParticipant = ModelParticipant & Readonly<{ provenance: Readonly<{ sessionId: string; sessionFileHash: string; profileHash: string; provider: string; model: string }> }>;

export type PiInteractiveTurnExpectation = Readonly<{ marker: string; agent: string; runtimeId: string }>;

const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const hashPath = (value: string): string => hash(resolve(value));
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("Pi interactive qualification operation aborted");
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw abortError(signal); };
const delay = (ms: number, signal: AbortSignal): Promise<void> => new Promise((resolveDelay, reject) => {
  checkAbort(signal);
  const timer = setTimeout(resolveDelay, ms);
  signal.addEventListener("abort", () => { clearTimeout(timer); reject(abortError(signal)); }, { once: true });
});
const workspace = (options: InteractiveOptions, input: LaunchInput): string => resolve(options.workspaceRoot ?? join(repo, ".gptqueue/qualification/pi-interactive"), hash(input.pairId).slice(0, 16), input.role, hash(input.nonce).slice(0, 16));
const routeSpec: RouteSpec = Object.freeze({ id: piInteractiveQualificationRoute, host: "pi", modelBacked: true, availability: { kind: "setup_gap" as const, detail: "Pi interactive PTY route preflight not run" } });

const registryBinding = async (redis: Redis, cwd: string, signal: AbortSignal): Promise<Binding> => {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    checkAbort(signal);
    const registry = await redis.hgetall(SESSION_KEYS.registry);
    for (const [agent, raw] of Object.entries(registry)) {
      const entry = object(JSON.parse(raw));
      const metadata = object(entry?.metadata);
      if (metadata?.client !== "pi" || metadata.working_directory !== cwd) continue;
      const runtime = object(JSON.parse(await redis.get(`gptq:runtime-binding:${agent}`) ?? "null"));
      if (runtime?.client === "pi" && runtime.working_directory === cwd && nonEmpty(runtime.runtime_id) && nonEmpty(runtime.epoch)) {
        return Object.freeze({ agent, runtimeId: runtime.runtime_id, epoch: runtime.epoch, cwd });
      }
    }
    await delay(250, signal);
  }
  throw new Error(`Pi interactive session did not expose an exact registry/runtime binding for ${cwd}`);
};

const sessionFileFor = async (sessionDir: string, sessionId: string, cwd: string): Promise<string | undefined> => {
  const files = await readdir(sessionDir).catch(() => [] as string[]);
  for (const name of files.filter(file => file.endsWith(".jsonl"))) {
    const file = join(sessionDir, name);
    const first = (await readFile(file, "utf8")).split("\n")[0];
    if (!first) continue;
    try {
      const header = JSON.parse(first) as Json;
      if (header.type === "session" && header.id === sessionId && (!nonEmpty(header.cwd) || resolve(header.cwd) === resolve(cwd))) return file;
    } catch { /* file is still being written */ }
  }
  return undefined;
};

const stopPty = async (child: PtyProcess, hasExited: () => boolean): Promise<void> => {
  if (child.process && !hasExited()) {
    try { process.kill(-child.pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
    const deadline = Date.now() + 5_000;
    while (!hasExited() && Date.now() < deadline) await new Promise(resolveDelay => setTimeout(resolveDelay, 50));
    if (!hasExited()) {
      try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
    }
  }
};

const historyOf = async (sessionDir: string, sessionId: string, cwd: string): Promise<readonly unknown[]> => {
  const sessionFile = await sessionFileFor(sessionDir, sessionId, cwd);
  if (!sessionFile) return [];
  return (await readFile(sessionFile, "utf8")).split("\n").flatMap(line => {
  try { return line ? [JSON.parse(line)] : []; } catch { return []; }
  });
};

const messageOf = (value: unknown): Json | undefined => {
  const current = object(value);
  const message = object(current?.message);
  return message ?? current;
};
const parsedObjects = (value: unknown): readonly Json[] => {
  if (typeof value === "string") { try { return parsedObjects(JSON.parse(value)); } catch { return []; } }
  if (Array.isArray(value)) return value.flatMap(parsedObjects);
  const current = object(value);
  return current ? [current, ...Object.values(current).flatMap(parsedObjects)] : [];
};
const contentBlocks = (message: Json): readonly Json[] => Array.isArray(message.content)
  ? message.content.map(object).filter((value): value is Json => value !== undefined) : [];
const assistantText = (message: Json): string => contentBlocks(message)
  .filter(block => block.type === "text" && typeof block.text === "string")
  .map(block => String(block.text)).join("\n");

/** Require fresh assistant output, a completed native turn, and a correlated runtime result. */
export const hasFreshPiInteractiveTurnEvidence = (
  history: readonly unknown[], baselineLength: number, expected: PiInteractiveTurnExpectation,
): boolean => {
  const fresh = history.slice(baselineLength).map(messageOf).filter((value): value is Json => value !== undefined);
  const assistant = fresh.filter(message => message.role === "assistant");
  const marked = assistant.filter(message => assistantText(message).includes(expected.marker) && message.stopReason === "stop");
  if (!marked.length) return false;
  const calls = assistant.flatMap(contentBlocks).filter(call => call.type === "toolCall" && call.name === "get_runtime_status" && nonEmpty(call.id));
  return calls.some(call => fresh.some(message => message.role === "toolResult" && message.toolCallId === call.id && message.toolName === "get_runtime_status" && parsedObjects(message).some(candidate => {
    const runtime = object(candidate.runtime);
    return candidate.status === "ok" && candidate.agent === expected.agent && runtime?.client === "pi" && runtime.runtime_id === expected.runtimeId;
  })));
};

export const freshPiInteractiveAssistantText = (
  history: readonly unknown[], baselineLength: number, expected: PiInteractiveTurnExpectation,
): string | undefined => {
  if (!hasFreshPiInteractiveTurnEvidence(history, baselineLength, expected)) return undefined;
  return history.slice(baselineLength).map(messageOf).filter((value): value is Json => value !== undefined)
    .filter(message => message.role === "assistant" && message.stopReason === "stop" && assistantText(message).includes(expected.marker))
    .map(assistantText).find(nonEmpty);
};

export const markerFromPiInteractivePrompt = (prompt: string): string | undefined =>
  [...prompt.matchAll(/\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b/g)].map(match => match[0]).at(-1);

export const startPiInteractiveParticipant = async (options: InteractiveOptions, input: LaunchInput, signal: AbortSignal): Promise<PiInteractiveParticipant> => {
  validatePiRedisUrl(input.redisUrl);
  if (!nonEmpty(options.provider) || !nonEmpty(options.model)) throw new Error("Pi interactive qualification requires explicit provider and model options");
  const root = options.installedRoot ?? installed;
  const cwd = workspace(options, input);
  const sessions = join(cwd, "sessions");
  await mkdir(sessions, { recursive: true });
  const extension = join(cwd, "gptqueue-evaluation.ts");
  await writeFile(extension,
    `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(repo, "dist/registered-shell/pi-extension.js")).href)};\n` +
    `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl: input.redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") })});\n`, { mode: 0o600 });
  const redis = new Redis(input.redisUrl, { maxRetriesPerRequest: 3, retryStrategy: () => null });
  let child: PtyProcess | undefined;
  let exited = false;
  let sessionFile = "";
  let closed = false;
  let closing: Promise<void> | undefined;
  let terminalOutput = "";
  let terminalExit: number | undefined;
  try {
    checkAbort(signal);
    const args = [join(root, "cli.js"), "--offline", "--provider", options.provider, "--model", options.model, "--no-extensions", "--extension", extension, "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-builtin-tools", "--session-dir", sessions, "--append-system-prompt", "Use only the private GPTQueue tools for qualification work."];
    child = pty.spawn(process.execPath, args, { cwd, env: { ...process.env, PI_OFFLINE: "1", GPTQUEUE_PI_PROVIDER: options.provider, GPTQUEUE_PI_MODEL: options.model, TERM: "xterm-256color" }, cols: 140, rows: 45, name: "xterm-256color" });
    child.onData(chunk => {
      terminalOutput = `${terminalOutput}${String(chunk)}`.slice(-20_000);
      if (String(chunk).includes("\u001b[6n")) child?.write("\u001b[1;1R");
    });
    child.onExit(({ exitCode }) => { exited = true; terminalExit = exitCode; });
    const binding = await registryBinding(redis, cwd, signal);
    const sessionId = binding.runtimeId;
    sessionFile = await sessionFileFor(sessions, sessionId, cwd) ?? "";
    const profileHash = hash(JSON.stringify({ agentDir: process.env.PI_CODING_AGENT_DIR ? "explicit-existing" : "default-existing", cwd: resolve(cwd), sessionDir: resolve(sessions), pty: true, noExtensions: true, noSkills: true, noContextFiles: true }));
    const identity = Object.freeze({ participantId: `${piInteractiveQualificationRoute}:${sessionId}`, route: piInteractiveQualificationRoute, hostRuntimeId: sessionId, agent: binding.agent, cwdHash: hashPath(cwd), profileHash, epochHash: hash(binding.epoch) });
    const close = async (): Promise<void> => {
      if (closing) return closing;
      closed = true;
      closing = (async () => {
        if (child) await stopPty(child, () => exited);
        await redis.quit();
        await rm(cwd, { recursive: true, force: true });
      })();
      return closing;
    };
    return Object.freeze({
      kind: "model" as const, identity,
      provenance: Object.freeze({ sessionId, sessionFileHash: hash(sessionFile ? await readFile(sessionFile, "utf8") : ""), profileHash, provider: options.provider, model: options.model }),
      prompt: async (text: string, promptSignal: AbortSignal) => {
        checkAbort(promptSignal);
        if (closed || !child || exited) throw new Error(`Pi interactive participant ${sessionId} is terminated`);
        const before = await historyOf(sessions, sessionId, cwd);
        const marker = `GPTQUEUE_PI_INTERACTIVE_COMPLETION_${createHash("sha256").update(`${sessionId}:${Date.now()}:${text}`).digest("hex").slice(0, 16).toUpperCase()}`;
        child.write(`${text}\nFor this qualification turn, call get_runtime_status once and include this exact completion marker in your final answer: ${marker}\r`);
        const deadline = Date.now() + 300_000;
        while (Date.now() < deadline) {
          checkAbort(promptSignal);
          const current = await historyOf(sessions, sessionId, cwd);
          const completed = freshPiInteractiveAssistantText(current, before.length, { marker, agent: binding.agent, runtimeId: sessionId });
          if (completed !== undefined) return completed;
          await delay(250, promptSignal);
        }
        const diagnostic = terminalOutput.replace(/(?:token|secret|password|authorization|api[_-]?key)\s*[:=]\s*\S+/giu, "[redacted]").slice(-2_000);
        throw new Error(`Pi interactive prompt did not expose a fresh completed assistant turn with correlated runtime status for ${sessionId}; exit=${String(terminalExit)} output=${diagnostic}`);
      },
      status: async (statusSignal: AbortSignal): Promise<RuntimeStatus> => {
        checkAbort(statusSignal);
        if (closed || exited) return { kind: "terminated", runtimeId: sessionId };
        return { kind: "unknown", detail: "Pi interactive PTY exposes no native turn lifecycle or quiescence status" };
      },
      history: async (historySignal: AbortSignal) => { checkAbort(historySignal); return historyOf(sessions, sessionId, cwd); },
      close,
    });
  } catch (error) {
    if (child) await stopPty(child, () => exited).catch(() => undefined);
    await redis.quit().catch(() => undefined);
    await rm(cwd, { recursive: true, force: true }).catch(() => undefined);
    throw error;
  }
};

const preflight = (options: InteractiveOptions, signal: AbortSignal): Promise<Availability> => (async () => {
  checkAbort(signal);
  const root = options.installedRoot ?? installed;
  const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
  try {
    await access(join(root, "cli.js"), constants.X_OK);
    await access(join(repo, "dist/registered-shell/pi-extension.js"), constants.R_OK);
    await access(join(agentDir, "auth.json"), constants.R_OK);
  } catch {
    return { kind: "setup_gap", detail: "Installed Pi CLI, built GPTQueue extension, or existing Pi auth is unavailable" };
  }
  return { kind: "available" };
})();

export const createPiInteractiveAdapter = (options: InteractiveOptions = {}): RouteAdapter => Object.freeze({
  spec: routeSpec,
  preflight: signal => preflight(options, signal),
  launch: (input, signal): Promise<Participant> => startPiInteractiveParticipant(options, input, signal),
});
