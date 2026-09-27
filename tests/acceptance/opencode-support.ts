import { sanitizeEvidence } from './public-evidence.js';
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { homePath } from "./local-tools.js";

export const repo = process.cwd();
export const opencodeBin = process.env.OPENCODE_BIN ?? homePath(".opencode/bin/opencode");
export const model = "zai-coding-plan/glm-5.3";
export const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
export const mcpEntry = join(repo, "dist/mcp-server/index.js");
export const timeoutMs = Math.min(300_000, Math.max(60_000, Number(process.env.GPTQUEUE_OPENCODE_TIMEOUT_MS ?? 240_000)));
export const modelsPath = homePath(".cache/opencode/models.json");

export type Json = Record<string, unknown>;

/** Config overlay: no project/global config, external plugins, or auth copy. */
export const makeConfig = () => ({
  $schema: "https://opencode.ai/config.json",
  model,
  autoupdate: false,
  provider: {
    "zai-coding-plan": {
      name: "Z.AI Coding Plan",
      npm: "@ai-sdk/openai-compatible",
      options: { baseURL: "https://api.z.ai/api/coding/paas/v4" },
      models: { "glm-5.3": { name: "GLM-5.3" } },
    },
  },
  mcp: {
    gptqueue: {
      type: "local",
      command: [process.execPath, mcpEntry],
      enabled: true,
      timeout: 60_000,
      environment: { REDIS_URL: redisUrl },
    },
  },
});

export const isolatedEnv = (configHome: string): NodeJS.ProcessEnv => ({
  ...process.env,
  XDG_CONFIG_HOME: configHome,
  OPENCODE_CONFIG: undefined,
  OPENCODE_CONFIG_DIR: undefined,
  OPENCODE_DISABLE_PROJECT_CONFIG: "1",
  OPENCODE_CONFIG_CONTENT: JSON.stringify(makeConfig()),
  OPENCODE_PURE: "1",
  OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
  // Use the already-present catalog; do not refresh it from the network.
  OPENCODE_MODELS_PATH: modelsPath,
});

export const newConfigHome = () => mkdtempSync(join(tmpdir(), "gptq-opencode-config-"));
export const newWorkDir = () => mkdtempSync(join(tmpdir(), "gptq-opencode-work-"));

export type ToolTrace = { name: string; status: string; hasOutput: boolean; error: boolean; input?: unknown; output?: unknown };
export type SessionLineageHash = { parent: string; child: string };
export type RunResult = { code: number | null; signal: NodeJS.Signals | null; timedOut: boolean; traces: ToolTrace[]; events: unknown[]; sessionLineageEvents: number; sessionLineageHashes: SessionLineageHash[]; jsonEvents: number; stderr: string; processError?: string; /** kept in memory for native resume/fork only; never written to evidence */ sessionId?: string };

const sanitize = (value: unknown): unknown => sanitizeEvidence(value, { redactSessionObjectIds: true, parseEmbeddedJson: true });
export const publicEvidence = sanitize;

function lineageHashes(value: unknown, found: SessionLineageHash[] = []): SessionLineageHash[] {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try { return lineageHashes(JSON.parse(trimmed), found); } catch { return found; }
    }
    return found;
  }
  if (Array.isArray(value)) { value.forEach((item) => lineageHashes(item, found)); return found; }
  if (!value || typeof value !== "object") return found;
  const row = value as Record<string, unknown>;
  const parent = Object.entries(row).find(([key, item]) => /^parent[_-]?(?:session[_-]?)?id$/i.test(key) && typeof item === "string")?.[1];
  const child = Object.entries(row).find(([key, item]) => /^(?:session|child[_-]?session)[_-]?id$/i.test(key) && typeof item === "string")?.[1];
  if (typeof parent === "string" && typeof child === "string") {
    found.push({ parent: createHash("sha256").update(parent).digest("hex"), child: createHash("sha256").update(child).digest("hex") });
  }
  Object.values(row).forEach((item) => lineageHashes(item, found));
  return found;
}

export function parseTrace(stdout: string): Pick<RunResult, "traces" | "events" | "sessionLineageEvents" | "sessionLineageHashes" | "jsonEvents" | "sessionId"> {
  const traces: ToolTrace[] = [];
  const events: unknown[] = [];
  const sessionLineageHashes: SessionLineageHash[] = [];
  let sessionLineageEvents = 0;
  let jsonEvents = 0;
  let sessionId: string | undefined;
  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    let event: any;
    try { event = JSON.parse(line); } catch { continue; }
    jsonEvents += 1;
    if (!sessionId) sessionId = sessionIdValue(event);
    if (event?.type === "session.created" && event.properties?.info?.parentID) sessionLineageEvents += 1;
    lineageHashes(event, sessionLineageHashes);
    events.push(sanitize(event));
    if (event?.type !== "tool_use" || event.part?.type !== "tool") continue;
    const state = event.part.state ?? {};
    traces.push({
      name: typeof event.part.tool === "string" ? event.part.tool : "unknown",
      status: typeof state.status === "string" ? state.status : "unknown",
      hasOutput: state.output !== undefined,
      error: state.status === "error" || state.error !== undefined,
      input: sanitize(state.input),
      output: sanitize(state.output),
    });
  }
  return { traces, events, sessionLineageEvents: sessionLineageEvents + sessionLineageHashes.length, sessionLineageHashes, jsonEvents, sessionId };
}

function sessionIdValue(value: unknown): string | undefined {
  if (Array.isArray(value)) { for (const item of value) { const found = sessionIdValue(item); if (found) return found; } return undefined; }
  if (!value || typeof value !== "object") return undefined;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (/^session[_-]?id$/i.test(key) && typeof item === "string") return item;
    const found = sessionIdValue(item); if (found) return found;
  }
  return undefined;
}

export function runOpenCodeRoute(prompt: string, configHome: string, workDir: string, routeArgs: readonly string[] = [], configOverlay: Json = {}): Promise<RunResult> {
  const child = spawn(opencodeBin, ["run", ...routeArgs, "--format", "json", "--model", model, "--agent", "build", "--dir", workDir, "--auto", prompt], {
    cwd: workDir,
    env: { ...isolatedEnv(configHome), OPENCODE_CONFIG_CONTENT: JSON.stringify({ ...makeConfig(), ...configOverlay }) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  child.stdout?.on("data", (chunk: Buffer) => { stdout = (stdout + chunk.toString()).slice(-500_000); });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-20_000); });
  return waitForChild(child, () => ({ ...parseTrace(stdout), stderr: sanitize(stderr) as string }));
}

export function runOpenCode(prompt: string, configHome: string, workDir: string): Promise<RunResult> {
  return runOpenCodeRoute(prompt, configHome, workDir);
}

function waitForChild(child: ChildProcess, trace: () => Pick<RunResult, "traces" | "events" | "sessionLineageEvents" | "sessionLineageHashes" | "jsonEvents" | "stderr" | "sessionId">): Promise<RunResult> {
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), 5_000).unref();
      resolve({ code: null, signal: "SIGTERM", timedOut: true, ...trace() });
    }, timeoutMs);
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code: null, signal: null, timedOut: false, processError: String(error), ...trace() });
    });
    child.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, signal, timedOut: false, ...trace() });
    });
  });
}

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", () => resolve()); });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

async function request(base: string, path: string, init?: RequestInit): Promise<any> {
  const response = await fetch(`${base}${path}`, { ...init, signal: init?.signal ?? AbortSignal.timeout(30_000) });
  if (!response.ok) throw new Error(`OpenCode HTTP ${response.status} ${path}`);
  if (response.status === 204) return undefined;
  return response.json();
}

async function stopServerProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  try {
    if (child.pid) process.kill(-child.pid, "SIGTERM"); else child.kill("SIGTERM");
  } catch { child.kill("SIGTERM"); }
  await Promise.race([exited, new Promise<void>((resolve) => setTimeout(resolve, 5_000))]);
  if (child.exitCode === null && child.signalCode === null) {
    try { if (child.pid) process.kill(-child.pid, "SIGKILL"); else child.kill("SIGKILL"); } catch { /* already exited */ }
    await Promise.race([exited, new Promise<void>((resolve) => setTimeout(resolve, 1_000))]);
  }
}

export type OpenCodePeer = {
  sessionId: string;
  registeredAgent: string;
  prompt: (text: string) => Promise<Json>;
  history: () => Promise<unknown>;
  close: () => Promise<void>;
};

export type OpenCodeServer = { baseUrl: string; peer: (agent: string) => Promise<OpenCodePeer>; stderr: () => string; close: () => Promise<void> };

/** Start `serve` without inference; useful for idle/attach and history probes. */
export async function startOpenCodeServer(agent: string): Promise<OpenCodeServer> {
  if (!existsSync(opencodeBin) || !existsSync(mcpEntry) || !existsSync(modelsPath)) throw new Error("OpenCode serve prerequisites unavailable");
  const configHome = newConfigHome();
  const workDir = newWorkDir();
  const port = await freePort();
  const child = spawn(opencodeBin, ["serve", "--port", String(port), "--hostname", "127.0.0.1"], { cwd: repo, env: isolatedEnv(configHome), stdio: ["ignore", "pipe", "pipe"], detached: true });
  let startupError: string | undefined;
  child.once("error", (error) => { startupError = String(error); });
  let serverStderr = "";
  child.stdout?.on("data", () => undefined);
  child.stderr?.on("data", (chunk: Buffer) => { serverStderr = (serverStderr + chunk.toString()).slice(-20_000); });
  const baseUrl = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (startupError || child.exitCode !== null || child.signalCode !== null) break;
    try { await request(baseUrl, "/config", { signal: AbortSignal.timeout(500) }); break; } catch { await new Promise((resolve) => setTimeout(resolve, 250)); }
  }
  if (Date.now() >= deadline || startupError) { await stopServerProcess(child); rmSync(configHome, { recursive: true, force: true }); rmSync(workDir, { recursive: true, force: true }); throw new Error(startupError ?? "OpenCode serve did not become ready"); }
  let closed = false;
  return {
    baseUrl,
    stderr: () => sanitize(serverStderr) as string,
    peer: async (registeredAgent) => {
      const session = await request(baseUrl, `/session?directory=${encodeURIComponent(workDir)}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ title: `GPTQueue acceptance ${agent}` }) });
      if (!session?.id) throw new Error("OpenCode session create returned no id");
      const sessionId = String(session.id);
      return {
        sessionId,
        registeredAgent,
        prompt: (text) => request(baseUrl, `/session/${encodeURIComponent(sessionId)}/message?directory=${encodeURIComponent(workDir)}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ parts: [{ type: "text", text }] }) }),
        history: () => request(baseUrl, `/session/${encodeURIComponent(sessionId)}/message?directory=${encodeURIComponent(workDir)}`),
        close: async () => { await request(baseUrl, `/session/${encodeURIComponent(sessionId)}?directory=${encodeURIComponent(workDir)}`, { method: "DELETE" }).catch(() => undefined); },
      };
    },
    close: async () => { if (closed) return; closed = true; await stopServerProcess(child); rmSync(configHome, { recursive: true, force: true }); rmSync(workDir, { recursive: true, force: true }); },
  };
}
