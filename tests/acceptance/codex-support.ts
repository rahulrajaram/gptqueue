import { mkdir, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { CodexSocketClient } from "../../src/registered-shell/codex-socket.js";
import { CodexThreadReader } from "../../src/registered-shell/codex-history.js";

export const enabled = process.env.GPTQUEUE_ACCEPTANCE_CODEX === "1";
export const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
export const repo = resolve(import.meta.dirname, "../..");
export const artifactRoot = join(repo, ".gptqueue/acceptance/20260912-evaluation/codex");
export const runId = randomUUID();
export const model = process.env.GPTQUEUE_CODEX_MODEL ?? "gpt-5.6-luna";

export type Json = Record<string, unknown>;
export type ToolCall = Json & { tool?: string; server?: string; arguments?: unknown; input?: unknown; result?: unknown; output?: unknown; receiverThreadIds?: unknown };

export const abort = (ms: number) => AbortSignal.timeout(ms);
export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export const appConfig = async (): Promise<Json> => {
  const path = join(process.env.CODEX_HOME ?? join(homedir(), ".codex"), "config.toml");
  const text = await readFile(path, "utf8");
  let names: string[] = [];
  try {
    names = JSON.parse(execFileSync("python3", ["-c", "import json,sys,tomllib;print(json.dumps(list(tomllib.load(open(sys.argv[1],'rb')).get('mcp_servers',{}))))", path], { encoding: "utf8" })) as string[];
  } catch {
    names = [...text.matchAll(/^\[mcp_servers\.([^.\]]+)(?:\.[^\]]+)?\]/gmu)].map((m) => m[1]!).filter((v, i, a) => a.indexOf(v) === i);
  }
  const config: Json = Object.fromEntries(names.flatMap((name) => [[`mcp_servers.${name}.enabled`, false], [`mcp_servers.${name}.required`, false]]));
  Object.assign(config, {
    "mcp_servers.gptqueue-shared.enabled": true,
    "mcp_servers.gptqueue-shared.required": true,
    "mcp_servers.gptqueue-shared.command": process.execPath,
    "mcp_servers.gptqueue-shared.args": [join(repo, "bin/gptqueue-session"), "--client", "codex", "--redis-url", redisUrl],
    model_reasoning_effort: "low",
  });
  return config;
};

export const startThread = async (rpc: CodexSocketClient, cwd: string, instructions: string, config: Json) => {
  const result = await rpc.request("thread/start", { cwd, model, approvalPolicy: "on-request", approvalsReviewer: "auto_review", sandbox: "workspace-write", config, developerInstructions: instructions }, abort(30_000));
  const thread = (result.thread ?? result) as Json;
  if (typeof thread.id !== "string") throw new Error("thread/start returned no thread id");
  return thread.id;
};

export const tool = (rpc: CodexSocketClient, threadId: string, name: string, args: Json = {}) => rpc.request("mcpServer/tool/call", { threadId, server: "gptqueue-shared", tool: name, arguments: args }, abort(30_000));

export const readThread = async (rpc: CodexSocketClient, threadId: string) => {
  const result = await rpc.request("thread/read", { threadId, includeTurns: true }, abort(30_000));
  return (result.thread ?? result) as Json;
};

export const threadReader = (rpc: CodexSocketClient) => new CodexThreadReader(rpc);

const retireThread = async (rpc: CodexSocketClient, threadId: string): Promise<void> => {
  try {
    const history = await readThread(rpc, threadId);
    const turns = Array.isArray(history.turns) ? history.turns : [];
    for (const turn of turns) {
      if (!turn || typeof turn !== "object") continue;
      const value = turn as Json;
      const state = String(value.status ?? "").toLowerCase();
      if (["inprogress", "in_progress", "running", "started"].includes(state) && typeof value.id === "string") {
        await rpc.request("turn/interrupt", { threadId, turnId: value.id }, abort(10_000)).catch(() => undefined);
      }
    }
  } finally {
    await rpc.request("thread/archive", { threadId }, abort(10_000)).catch(() => undefined);
  }
};

export const startCodexPeer = async (options: { cwd: string; redisUrl?: string; instructions?: string }) => {
  await mkdir(options.cwd, { recursive: true });
  const rpc = new CodexSocketClient(undefined, 20_000);
  let threadId: string;
  try {
    const config = await appConfig();
    const instructions = options.instructions ??
      "You are an isolated integration participant. Follow the available native MCP instructions, answer the initialization prompt with READY, and provide useful answers to work requests. Do not use shell commands or filesystem tools.";
    threadId = await startThread(rpc, options.cwd, instructions, {
      ...config,
      "mcp_servers.gptqueue-shared.args": [join(repo, "bin/gptqueue-session"), "--client", "codex", "--redis-url", options.redisUrl ?? redisUrl],
    });
  } catch (error) {
    await rpc.close();
    throw error;
  }
  let status: Json;
  try {
    await rpc.request("turn/start", { threadId, input: [{ type: "text", text: "Initialize this isolated participant. Reply READY." }] }, abort(30_000));
    const ready = await waitFor(
      async () => tool(rpc, threadId, "get_runtime_status"),
      (value) => (value.structuredContent as Json | undefined)?.activation_ready === true,
      45_000,
      `automatic runtime binding for ${threadId}`,
    );
    status = ready.structuredContent as Json;
    if (typeof status.agent !== "string" || status.activation_ready !== true || !status.runtime) throw new Error(`Codex peer ${threadId} did not become activation-ready`);
  } catch (error) {
    await retireThread(rpc, threadId).catch(() => undefined);
    await rpc.close();
    throw error;
  }
  const reader = threadReader(rpc);
  return {
    threadId,
    agent: status.agent,
    ready: status,
    prompt: (text: string) => rpc.request("turn/start", { threadId, input: [{ type: "text", text }] }, abort(120_000)),
    history: () => readThread(rpc, threadId),
    inspectThread: (id: string) => readThread(rpc, id),
    inspectTool: (id: string, name: string, args: Json = {}) => tool(rpc, id, name, args),
    historyView: () => reader.read(threadId, abort(30_000)),
    tool: (name: string, args: Json = {}) => tool(rpc, threadId, name, args),
    close: async () => {
      try {
        await retireThread(rpc, threadId);
      } finally { await rpc.close(); }
    },
  };
};

const walk = (value: unknown, out: ToolCall[]): void => {
  if (Array.isArray(value)) { value.forEach((v) => walk(v, out)); return; }
  if (!value || typeof value !== "object") return;
  const object = value as Json;
  if (typeof object.tool === "string" && (typeof object.type !== "string" || /mcp|collab/iu.test(object.type))) out.push(object as ToolCall);
  Object.values(object).forEach((v) => walk(v, out));
};

export const toolCalls = (thread: Json): readonly ToolCall[] => {
  const out: ToolCall[] = [];
  walk(thread.turns, out);
  return out;
};

export const parsePayload = (value: unknown): Json | null => {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as Json;
  if (typeof value !== "string") return null;
  try { const parsed = JSON.parse(value) as unknown; return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Json : null; } catch { return null; }
};

export const callArgs = (call: ToolCall): Json | null => parsePayload(call.arguments ?? call.input);
export const callResult = (call: ToolCall): Json | null => {
  const result = call.result ?? call.output;
  const direct = parsePayload(result);
  if (direct) return direct;
  if (result && typeof result === "object") {
    const content = (result as Json).content;
    if (Array.isArray(content)) for (const item of content) if (item && typeof item === "object") {
      const parsed = parsePayload((item as Json).text);
      if (parsed) return parsed;
    }
  }
  return null;
};

export const trace = async (redis: Redis, agent: string) => {
  const rows = await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+");
  return rows.map(([, fields]) => Object.fromEntries(Array.from({ length: Math.floor(fields.length / 2) }, (_, i) => [fields[i * 2]!, fields[i * 2 + 1]!])));
};

export const inboxEvents = async (redis: Redis, agent: string) => {
  const rows = await redis.xrange(`gptq:inbox-events:${agent}`, "-", "+");
  return rows.map(([, fields]) => Object.fromEntries(Array.from({ length: Math.floor(fields.length / 2) }, (_, i) => [fields[i * 2]!, fields[i * 2 + 1]!])))
    .filter((row) => typeof row.message_id === "string");
};

export const waitFor = async <T>(read: () => Promise<T>, test: (value: T) => boolean, timeoutMs: number, label: string): Promise<T> => {
  const end = Date.now() + timeoutMs;
  let value = await read();
  while (!test(value) && Date.now() < end) { await sleep(1000); value = await read(); }
  if (!test(value)) throw new Error(`Timed out waiting for ${label}`);
  return value;
};

export const ensureArtifacts = () => mkdir(join(artifactRoot, runId), { recursive: true });
