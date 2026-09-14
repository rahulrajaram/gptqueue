import { sanitizeEvidence } from './public-evidence.js';
import { describe, expect, it, vi } from "vitest";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, writeFile, rm } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { appConfig, redisUrl, repo, type Json } from "./codex-support.js";

const enabled = process.env.GPTQUEUE_ACCEPTANCE_CODEX_CLI === "1";
const artifactRoot = join(repo, ".gptqueue/acceptance/20260912-evaluation/codex-cli");
const timeoutMs = 420_000;
const codexBin = process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex";
vi.setConfig({ testTimeout: timeoutMs + 30_000, hookTimeout: 30_000 });

type ToolCall = { name: string; arguments?: unknown; result?: unknown; threadId?: string | null };
type Run = { code: number | null; signal: NodeJS.Signals | null; timedOut: boolean; stdout: string; stderr: string };
const sanitize = (value: unknown): unknown => sanitizeEvidence(value, { parseEmbeddedJson: true });

const canonical = (value: unknown) => typeof value === "string" ? value.split(/[/.]/u).at(-1) ?? value : "";
const parse = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};
export const parseEvents = (stdout: string): { events: unknown[]; calls: ToolCall[] } => {
  const events: unknown[] = [], calls: ToolCall[] = [];
  const walk = (value: unknown, threadId: string | null = null): void => {
    if (Array.isArray(value)) { value.forEach((item) => walk(item, threadId)); return; }
    if (typeof value === "string" && /^[[{]/u.test(value.trim())) { try { walk(JSON.parse(value), threadId); } catch { /* ordinary text */ } return; }
    if (!value || typeof value !== "object") return;
    const object = value as Json;
    const inherited = typeof object.thread_id === "string" ? object.thread_id : typeof object.threadId === "string" ? object.threadId : threadId;
    const type = String(object.type ?? "").toLowerCase();
    const name = object.name ?? object.tool_name ?? object.tool;
    const result = object.result ?? object.output ?? object.content;
    // Codex emits an initial in_progress tool call with no result, followed by
    // the completed call. Only completed calls are evidence of execution.
    if (typeof name === "string" && result !== undefined && (type.includes("tool") || type.includes("mcp") || type.includes("function") || name.includes("gptqueue") || ["spawnAgent", "spawn_agent"].includes(name))) {
      calls.push({ name, arguments: object.arguments ?? object.input ?? object.args, result, threadId: inherited });
    }
    Object.values(object).forEach((item) => walk(item, inherited));
  };
  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    try { const event = JSON.parse(line) as unknown; events.push(sanitize(event)); walk(event); } catch { /* Codex may emit a non-JSON diagnostic line. */ }
  }
  return { events, calls };
};

const object = (value: unknown): Json | null => {
  const parsed = parse(value);
  return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Json : null;
};
const structured = (value: unknown): Json | null => {
  const parsed = object(value);
  if (!parsed) return null;
  const direct = object(parsed.structuredContent ?? parsed.structured_content);
  if (direct) return direct;
  if (Array.isArray(parsed.content)) for (const item of parsed.content) {
    const nested = structured(item && typeof item === "object" ? (item as Json).text ?? item : item);
    if (nested) return nested;
  }
  return parsed;
};
const findObject = (value: unknown, predicate: (item: Json) => boolean): Json | null => {
  if (Array.isArray(value)) for (const item of value) { const found = findObject(item, predicate); if (found) return found; }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const item = value as Json;
  if (predicate(item)) return item;
  for (const child of Object.values(item)) { const found = findObject(child, predicate); if (found) return found; }
  return null;
};
const toml = (value: unknown) => JSON.stringify(value);

export const runCodex = async (cwd: string, prompt: string, config: Json): Promise<Run> => {
  const overrides = Object.entries(config).flatMap(([key, value]) => ["-c", `${key}=${toml(value)}`]);
  const child = spawn(codexBin, ["exec", "--json", "--ephemeral", "--ignore-user-config", "--skip-git-repo-check", "--approve-for-me", "--model", "gpt-5.6-luna", "-C", cwd, ...overrides, prompt], {
    cwd, env: { ...process.env }, stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "", stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => { stdout = (stdout + chunk.toString()).slice(-1_500_000); });
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-40_000); });
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: Run) => { if (settled) return; settled = true; clearTimeout(timer); resolve(result); };
    const timer = setTimeout(() => { child.kill("SIGTERM"); setTimeout(() => child.kill("SIGKILL"), 5_000).unref(); finish({ code: null, signal: "SIGTERM", timedOut: true, stdout, stderr }); }, timeoutMs);
    child.once("error", (error) => finish({ code: null, signal: null, timedOut: false, stdout, stderr: `${stderr}\n${String(error)}` }));
    child.once("exit", (code, signal) => finish({ code, signal, timedOut: false, stdout, stderr }));
  });
};

const registry = async (redis: Redis) => {
  const rows = await redis.hgetall(SESSION_KEYS.registry);
  return Object.entries(rows).flatMap(([name, raw]) => { try { const value = JSON.parse(raw) as Json; return [{ name, value }]; } catch { return []; } });
};
const ownedCodex = (rows: readonly { name: string; value: Json }[], before: ReadonlySet<string>, cwd: string) => rows.filter(({ name, value }) => !before.has(name) && object(value.metadata)?.client === "codex" && object(value.metadata)?.working_directory === cwd);
const streamRows = async (redis: Redis, key: string) => (await redis.xrange(key, "-", "+")).map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, index) => [fields[index * 2]!, fields[index * 2 + 1]!])));
const removeAgent = async (redis: Redis, name: string) => {
  const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
  await redis.hdel(SESSION_KEYS.registry, name);
  await redis.del(SESSION_KEYS.agentSessions(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name), `gptq:inbox-events:${name}`, `gptq:inbox-trace:${name}`, `gptq:runtime-binding:${name}`, ...sessions.flatMap((session) => [SESSION_KEYS.session(session), SESSION_KEYS.lease(session)]));
};

describe.skipIf(!enabled)("standalone Codex CLI mechanics probe", () => {
  it("self-communicates and observes one distinct native Luna child", async () => {
    const runId = randomUUID();
    await mkdir(artifactRoot, { recursive: true });
    const artifactDir = join(artifactRoot, runId); await mkdir(artifactDir, { recursive: true });
    const workRoot = await mkdtemp(join(tmpdir(), `gptq-codex-cli-${runId}-`));
    const cwd = join(workRoot, "workspace"); await mkdir(cwd, { recursive: true });
    const parentNonce = `codex-cli-parent-${randomUUID()}`, childNonce = `codex-cli-child-${randomUUID()}`;
    const parentIdempotency = `status-${parentNonce}`, childIdempotency = `status-${childNonce}`;
    const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
    const receipt: Json = { schema_version: 1, run_id: runId, database: 15, model: "gpt-5.6-luna", auth_source: process.env.CODEX_HOME ?? join(homedir(), ".codex"), passed: false };
    let before = new Set<string>();
    const observed = new Set<string>();
    try {
      await redis.ping();
      before = new Set((await registry(redis)).map(({ name }) => name));
      const config = Object.fromEntries(Object.entries(await appConfig()).filter(([key]) => key === "model_reasoning_effort" || key.startsWith("mcp_servers.gptqueue-shared.")));
      const childPrompt = `Use only the inherited gptqueue-shared MCP tools. Call get_runtime_status once and preserve its structured result. Then call send_message to yourself with to equal your own reported agent, type status, content ${childNonce}, and idempotency_key ${childIdempotency}; then call receive_message with timeout 10 and verify the returned message has exactly that nonce, type, and message id. Do not send to any other agent, do not call task tools, do not use shell or files, and report the original structured tool results.`;
      const prompt = `Use only the available gptqueue-shared MCP tools and native collaboration. First call get_runtime_status once and preserve its structured result. Then call send_message to yourself (to equal your own reported agent) with type status, content ${parentNonce}, and idempotency_key ${parentIdempotency}; call receive_message with timeout 10 and verify the returned message has exactly that nonce, type status, and the sent message_id. Do not use task tools or send any notification to another agent. Next use the native collaboration spawn tool exactly once to create one child with model gpt-5.6-luna and this exact child instruction: ${childPrompt} . Wait for that child to finish. Do not use shell or filesystem tools. Report original structured tool results and native tool results; do not infer success from prose.`;
      const run = await runCodex(cwd, prompt, config);
      const parsed = parseEvents(run.stdout);
      const calls = parsed.calls;
      const statuses = calls.filter((call) => canonical(call.name) === "get_runtime_status").map((call) => ({ call, value: structured(call.result) })).filter(({ value }) => typeof value?.agent === "string");
      const parentStatus = statuses[0]?.value ?? null;
      const sends = calls.filter((call) => canonical(call.name) === "send_message").map((call) => ({ call, args: object(call.arguments), result: structured(call.result) }));
      const receives = calls.filter((call) => canonical(call.name) === "receive_message").map((call) => ({ call, result: structured(call.result) }));
      const parentSend = sends.find(({ args }) => args?.content === parentNonce && args.type === "status");
      const parentMessageId = typeof parentSend?.result?.message_id === "string" ? parentSend.result.message_id : null;
      const parentReceived = receives.find(({ result }) => findObject(result, (item) => item.id === parentMessageId && (item.content === parentNonce || object(item.payload)?.content === parentNonce) && (item.type === "status" || object(item.payload)?.type === "status")) !== null);
      const childSend = sends.find(({ args }) => args?.content === childNonce && args.type === "status");
      const childMessageId = typeof childSend?.result?.message_id === "string" ? childSend.result.message_id : null;
      const childReceived = receives.find(({ result }) => findObject(result, (item) => item.id === childMessageId && (item.content === childNonce || object(item.payload)?.content === childNonce) && (item.type === "status" || object(item.payload)?.type === "status")) !== null);
      const childStatus = statuses.find(({ value }) => value?.agent !== parentStatus?.agent)?.value ?? null;
      const spawn = calls.find((call) => ["spawnAgent", "spawn_agent"].includes(canonical(call.name)));
      const childRows = ownedCodex(await registry(redis), before, cwd);
      const parentAgent = typeof parentStatus?.agent === "string" ? parentStatus.agent : null;
      const childAgent = typeof childStatus?.agent === "string" ? childStatus.agent : typeof childSend?.args?.to === "string" ? childSend.args.to : null;
      if (parentAgent) observed.add(parentAgent);
      if (childAgent) observed.add(childAgent);
      const rowsByAgent = await Promise.all([parentAgent, childAgent].filter((name, index, list): name is string => typeof name === "string" && list.indexOf(name) === index).map(async (agent) => ({ agent, events: await streamRows(redis, `gptq:inbox-events:${agent}`), trace: await streamRows(redis, `gptq:inbox-trace:${agent}`), registry: childRows.find(({ name }) => name === agent)?.value ?? null })));
      receipt.config = { source: "appConfig() read; CLI --ignore-user-config", mcp_server: "gptqueue-shared", redis_url: redisUrl, hooks: "disabled by --ignore-user-config", auth: "normal CODEX_HOME auth" };
      receipt.run = { code: run.code, signal: run.signal, timed_out: run.timedOut, stdout: sanitize(run.stdout), stderr: sanitize(run.stderr), tool_calls: calls.map((call) => sanitize(call)), native_json_events: parsed.events };
      receipt.parent = { agent: parentAgent, status: sanitize(parentStatus), send: sanitize(parentSend?.result), receive: sanitize(parentReceived?.result), message_id: parentMessageId, queue_depth: parentAgent === null ? null : await redis.llen(SESSION_KEYS.queue(parentAgent)), activation_ready: parentStatus?.activation_ready ?? null };
      receipt.child = { agent: childAgent, status: sanitize(childStatus), send: sanitize(childSend?.result), receive: sanitize(childReceived?.result), message_id: childMessageId, queue_depth: childAgent === null ? null : await redis.llen(SESSION_KEYS.queue(childAgent)), registry: rowsByAgent.find(({ agent }) => agent === childAgent)?.registry ?? null };
      receipt.redis = rowsByAgent;
      receipt.native_spawn = sanitize(spawn);
      receipt.passed = run.code === 0 && !run.timedOut && parentAgent !== null && childAgent !== null && childStatus !== null && parentAgent !== childAgent && parentAgent === parentSend?.args?.to && childAgent === childSend?.args?.to && parentSend?.args?.idempotency_key === parentIdempotency && childSend?.args?.idempotency_key === childIdempotency && parentMessageId !== null && parentSend?.result?.status === "sent" && parentReceived !== undefined && childMessageId !== null && childSend?.result?.status === "sent" && childReceived !== undefined && spawn !== undefined && object(spawn.arguments)?.model === "gpt-5.6-luna" && childRows.some(({ name }) => name === parentAgent) && childRows.some(({ name }) => name === childAgent);
      expect(receipt.passed).toBe(true);
    } catch (error) {
      receipt.error = String(error);
      throw error;
    } finally {
      const current = await registry(redis).catch(() => []);
      const cleanup = new Set([...observed].filter((name) => !before.has(name)).concat(ownedCodex(current, before, cwd).map(({ name }) => name)));
      await Promise.all([...cleanup].map((name) => removeAgent(redis, name).catch(() => undefined)));
      await writeFile(join(artifactDir, "receipt.json"), JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600 });
      await redis.quit();
      await rm(workRoot, { recursive: true, force: true }).catch(() => undefined);
    }
  }, timeoutMs + 10_000);
});
