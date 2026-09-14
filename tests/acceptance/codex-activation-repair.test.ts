import { expect, it } from "vitest";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { Redis } from "ioredis";
import { appConfig, repo, startThread, readThread } from "./codex-support.js";
import { sanitizeEvidence } from "./public-evidence.js";
import { CodexSocketClient } from "../../src/registered-shell/codex-socket.js";
import { bindCodexHook } from "../../src/registered-shell/codex-hook.js";
import { CodexThreadReader } from "../../src/registered-shell/codex-history.js";
import { startOwnedRedis } from "./owned-redis.js";

const enabled = process.env.GPTQUEUE_CODEX_ACTIVATION_REPAIR === "1";
const node = "/home/rahul/nodeenv2251-311/bin/node";
const codex = "/home/rahul/.local/bin/codex";
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/codex-activation");
const stage = { setup: 45_000, participant: 150_000, exchange: 180_000, cleanup: 30_000 } as const;
const timeout = stage.setup + stage.participant * 2 + stage.exchange + stage.cleanup + 30_000;
type JsonObject = Record<string, unknown>;
const asObject = (value: unknown): JsonObject | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
const turnsOf = (value: unknown): readonly JsonObject[] => Array.isArray(asObject(value)?.turns) ? (asObject(value)?.turns as unknown[]).flatMap(item => { const object = asObject(item); return object ? [object] : []; }) : [];
const itemsOf = (value: unknown): readonly JsonObject[] => Array.isArray(value) ? value.flatMap(item => { const object = asObject(item); return object ? [object] : []; }) : [];

const digest = async (file: string): Promise<string> => createHash("sha256").update(await readFile(file)).digest("hex");
const waitFor = async (read: () => Promise<boolean>, deadline: number): Promise<void> => {
  while (Date.now() < deadline) { if (await read()) return; await new Promise(resolve => setTimeout(resolve, 1_000)); }
  throw new Error("native activation repair did not complete before the observation deadline");
};
const killTree = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { if (child.pid) process.kill(-child.pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
  await new Promise(resolve => setTimeout(resolve, 2_000));
  if (child.exitCode === null && child.signalCode === null) {
    try { if (child.pid) process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
  }
};
type Envelope = { id: string; from: string; to: string; type: string; payload?: { content?: string; in_reply_to?: string } };
const parseEnvelope = (value: unknown): Envelope | undefined => {
  if (typeof value !== "string") return value && typeof value === "object" ? value as Envelope : undefined;
  try { return parseEnvelope(JSON.parse(value)); } catch { return undefined; }
};
const claimEnvelopes = (value: unknown): Envelope[] => {
  if (Array.isArray(value)) return value.flatMap(claimEnvelopes);
  if (!value || typeof value !== "object") return [];
  const object = value as Record<string, unknown>;
  if (Array.isArray(object.tasks)) return object.tasks.flatMap(task => { const envelope = parseEnvelope(task); return envelope ? [envelope] : []; });
  return Object.values(object).flatMap(claimEnvelopes);
};
const exactClaimEnvelope = (item: any, expected: Envelope): boolean => claimEnvelopes(item.result).some(actual =>
  actual.id === expected.id && actual.from === expected.from && actual.to === expected.to && actual.type === expected.type &&
  actual.payload?.content === expected.payload?.content && actual.payload?.in_reply_to === expected.payload?.in_reply_to);
const exactAckClaim = (item: any, claimId: string | undefined): boolean => item?.type === "mcpToolCall" && item.tool === "acknowledge_tasks" &&
  Boolean(item.arguments && typeof item.arguments === "object" && (item.arguments as Record<string, unknown>).claim_id === claimId);

it.skipIf(!enabled)("binds owned Codex threads and activates an idle arithmetic task", async () => {
  const run = randomUUID();
  const directory = join(artifactRoot, run);
  const socketDir = await mkdtemp(join(tmpdir(), "gptq-codex-repair-"));
  const socket = join(socketDir, "control.sock");
  const cwd = join(directory, "workspace");
  const previousSocket = process.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET;
  const previousRedis = process.env.REDIS_URL;
  process.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET = socket;
  await mkdir(cwd, { recursive: true });
  await mkdir(join(directory, "phases"), { recursive: true });
  const ownedRedis = await startOwnedRedis();
  const redisUrl = ownedRedis.url;
  process.env.REDIS_URL = redisUrl;
  const redis = new Redis(redisUrl);
  const receipt: Record<string, unknown> = {
    schema_version: 1, run, database: 15, model: "gpt-5.6-luna", route: "codex-owned-app-server",
    command: [codex, "app-server", "--listen", `unix://${socket}`], socket, roles: {}, passed: false,
  };
  let server: ChildProcess | undefined;
  let rpc: CodexSocketClient | undefined;
  let reader: CodexThreadReader | undefined;
  const threads: string[] = [];
  const histories: Record<string, unknown> = {};
  let phase = 0;
  const persist = async (label: string, value: unknown): Promise<void> => {
    const entry = { sequence: ++phase, label, at: new Date().toISOString(), value: sanitizeEvidence(value) };
    await writeFile(join(directory, "phases", `${String(entry.sequence).padStart(4, "0")}-${label}.json`), JSON.stringify(entry, null, 2) + "\n", { mode: 0o600 });
    await writeFile(join(directory, "receipt.json"), JSON.stringify(sanitizeEvidence(receipt), null, 2) + "\n", { mode: 0o600 });
  };
  try {
    const installedHookPath = "/home/rahul/.codex/hooks.json";
    try {
      const installedHooks = JSON.parse(await readFile(installedHookPath, "utf8")) as Record<string, any>;
      receipt.installed_hook = { path: installedHookPath, events: Object.fromEntries(Object.entries(installedHooks.hooks ?? {}).map(([event, groups]) => [event, (groups as any[]).flatMap(group => group.hooks ?? []).map(hook => ({ type: hook.type, command: hook.command }))])) };
    } catch (error) { receipt.installed_hook = { path: installedHookPath, read_error: String(error) }; }
    const config = await appConfig();
    Object.assign(config, {
      "mcp_servers.gptqueue-shared.env.REDIS_URL": redisUrl,
      "mcp_servers.gptqueue-shared.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET": socket,
      "mcp_servers.gptqueue-shared.args": [join(repo, "bin/gptqueue-session"), "--client", "codex", "--redis-url", redisUrl],
    });
    server = spawn(codex, ["app-server", "--listen", `unix://${socket}`, ...Object.entries(config).flatMap(([key, value]) => ["-c", `${key}=${JSON.stringify(value)}`])], {
      cwd, env: { ...process.env, REDIS_URL: redisUrl, GPTQUEUE_CODEX_APP_SERVER_SOCKET: socket },
      stdio: ["ignore", "pipe", "pipe"], detached: true,
    });
    let stderr = "";
    server.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-20_000); });
    await waitFor(async () => existsSync(socket), Date.now() + 20_000);
    receipt.server = { started: true, stderr };
    rpc = new CodexSocketClient(undefined, 20_000);
    reader = new CodexThreadReader(rpc);
    const call = async (threadId: string, name: string, args: Record<string, unknown> = {}) => {
      await persist(`${name}-before`, { thread_id: threadId, arguments: args });
      try {
        const result = await rpc!.request("mcpServer/tool/call", { threadId, server: "gptqueue-shared", tool: name, arguments: args }, AbortSignal.timeout(30_000));
        await persist(`${name}-after`, { thread_id: threadId, result });
        return result;
      } catch (error) {
        await persist(`${name}-error`, { thread_id: threadId, error: String(error) });
        throw error;
      }
    };
    const participant = async (role: string, prompt: string): Promise<{ threadId: string; agent: string; baseline_turn_count: number }> => {
      const participantCwd = join(cwd, role);
      await mkdir(participantCwd, { recursive: true });
      await persist(`${role}-thread-start-before`, { cwd: participantCwd });
      const threadId = await startThread(rpc!, participantCwd, "Use only gptqueue-shared MCP tools. " + prompt, config);
      await persist(`${role}-thread-start-after`, { thread_id: threadId, cwd: participantCwd });
      threads.push(threadId);
      await persist(`${role}-turn-start-before`, { thread_id: threadId });
      await rpc!.request("turn/start", { threadId, input: [{ type: "text", text: "Initialize this idle participant and reply READY." }] }, AbortSignal.timeout(60_000));
      await persist(`${role}-turn-start-after`, { thread_id: threadId });
      await waitFor(async () => {
        try {
          const thread = await reader!.read(threadId, AbortSignal.timeout(15_000));
          const current = turnsOf(thread).at(-1);
          return ["completed", "succeeded", "failed", "interrupted"].includes(String(current?.status ?? "").toLowerCase());
        } catch { return false; }
      }, Date.now() + stage.participant);
      const beforeBinding = (await call(threadId, "get_runtime_status")).structuredContent as Record<string, any>;
      const automaticBindingObserved = beforeBinding.activation_ready === true;
      let bound = automaticBindingObserved;
      let unavailableCode: string | undefined;
      for (let attempt = 0; attempt < 3 && !bound; attempt++) {
        bound = await bindCodexHook({ session_id: threadId, cwd: participantCwd, hook_event_name: "SessionStart" }, rpc!, { timeoutMs: 10_000, retryMs: 250, onUnavailable: code => { unavailableCode = code; } });
      }
      if (!bound) {
        receipt.binding_diagnostic = { role, thread_id: threadId, unavailable_code: unavailableCode, status: await call(threadId, "get_runtime_status").catch(error => String(error)) };
        throw new Error(`${role} hook binding failed after three attempts`);
      }
      const status = (await call(threadId, "get_runtime_status")).structuredContent as Record<string, any>;
      if (status.activation_ready !== true || typeof status.agent !== "string") throw new Error(`${role} is not activation-ready`);
      if (status.runtime?.runtime_id !== threadId || status.runtime?.working_directory !== participantCwd) throw new Error(`${role} runtime binding does not match its exact thread/cwd`);
      const roleEvidence = { thread_id: threadId, agent: status.agent, before_binding: beforeBinding, after_binding: status, automatic_binding_observed: automaticBindingObserved, unavailable_code: unavailableCode };
      (receipt.roles as Record<string, unknown>)[role] = roleEvidence;
      return { threadId, agent: status.agent, baseline_turn_count: turnsOf(await reader!.read(threadId, AbortSignal.timeout(15_000))).length };
    };
    const sender = await participant("sender", "When an incoming result arrives, claim and acknowledge it, then report SENDER_RECEIVED_42. For tasks, compute and reply with the exact answer.");
    const recipient = await participant("recipient", "When an incoming task arrives, claim it, compute the arithmetic, send a correlated result with idempotency_key equal to the task ID, acknowledge the claim, and do not wait for another user prompt.");
    const senderBaselineTurnCount = sender.baseline_turn_count;
    const sent = (await call(sender.threadId, "send_message", { to: recipient.agent, type: "task", content: "Compute 17 + 25 and reply with only the decimal answer.", idempotency_key: `repair-${run}` })).structuredContent as JsonObject;
    receipt.task = sent;
    await persist("task-send-response", sent);
    if (sent.status !== "sent" || typeof sent.message_id !== "string") throw new Error(`task send did not return a message ID: ${JSON.stringify(sent)}`);
    const taskId = sent.message_id;
    await waitFor(async () => {
      const [senderThread, recipientThread] = await Promise.all([reader!.read(sender.threadId, AbortSignal.timeout(15_000)), reader!.read(recipient.threadId, AbortSignal.timeout(15_000))]);
      const senderTurns = turnsOf(senderThread);
      const recipientTurns = turnsOf(recipientThread);
      const senderItems = senderTurns.flatMap(turn => itemsOf(turn.items));
      const recipientItems = recipientTurns.flatMap(turn => itemsOf(turn.items));
      const reply = recipientItems.find(item => item.type === "mcpToolCall" && item.tool === "send_message" && asObject(item.arguments)?.in_reply_to === taskId);
      const senderContinuationItems = senderTurns.slice(senderBaselineTurnCount).flatMap(turn => itemsOf(turn.items));
      const marker = senderContinuationItems.some(item => item.type === "agentMessage" && String(item.text ?? "").includes("SENDER_RECEIVED_42"));
      const traces = await Promise.all([sender.agent, recipient.agent].map(agent => redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+")));
      const asRows = (rows: [string, string[]][]) => rows.map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[i * 2]!, fields[i * 2 + 1]!]))) as Record<string, string>[];
      const senderTrace = asRows(traces[0]!);
      const recipientTrace = asRows(traces[1]!);
      const replyId = asObject(asObject(reply?.result)?.structuredContent)?.message_id;
      const recipientClaim = recipientTrace.find(row => row.stage === "task_claimed" && row.message_id === taskId);
      const senderClaim = typeof replyId === "string" ? senderTrace.find(row => row.stage === "task_claimed" && row.message_id === replyId) : undefined;
      const recipientAck = recipientClaim && recipientTrace.some(row => row.stage === "task_acknowledged" && row.claim_id === recipientClaim.claim_id);
      const senderAck = senderClaim && senderTrace.some(row => row.stage === "task_acknowledged" && row.claim_id === senderClaim.claim_id);
      const recipientClaimCall = recipientItems.find(item => item.type === "mcpToolCall" && item.tool === "claim_tasks" && exactClaimEnvelope(item, { id: taskId, from: sender.agent, to: recipient.agent, type: "task", payload: { content: "Compute 17 + 25 and reply with only the decimal answer." } }));
      const recipientAckCall = recipientItems.find(item => exactAckClaim(item, recipientClaim?.claim_id));
      const senderClaimCall = typeof replyId === "string" ? senderContinuationItems.find(item => item.type === "mcpToolCall" && item.tool === "claim_tasks" && exactClaimEnvelope(item, { id: replyId, from: recipient.agent, to: sender.agent, type: "result", payload: { content: "42", in_reply_to: taskId } })) : undefined;
      const senderAckCall = senderClaim && senderContinuationItems.find(item => exactAckClaim(item, senderClaim.claim_id));
      receipt.observed = { sender_turns: senderTurns.length, recipient_turns: recipientTurns.length, reply, marker, traces, claims: { recipientClaim, recipientAck, senderClaim, senderAck }, claim_tool_envelopes: { recipientClaimCall, recipientAckCall, senderClaimCall, senderAckCall } };
      const replyArguments = asObject(reply?.arguments);
      return Boolean(replyArguments?.content === "42" && replyArguments.type === "result" && replyArguments.in_reply_to === taskId && replyArguments.to === sender.agent && marker && recipientClaim && recipientAck && senderClaim && senderAck && recipientClaimCall && recipientAckCall && senderClaimCall && senderAckCall);
    }, Date.now() + stage.exchange);
    receipt.passed = true;
  } catch (error) {
    receipt.error = String(error);
    throw error;
  } finally {
    if (reader) for (const threadId of threads) histories[threadId] = await reader.read(threadId, AbortSignal.timeout(15_000)).catch(error => ({ read_error: String(error) }));
    const sourceHashes: Record<string, string> = {};
    receipt.source_hashes = sourceHashes;
    for (const file of ["src/registered-shell/codex-socket.ts", "src/registered-shell/codex-hook.ts", "src/registered-shell/codex-runtime.ts", "tests/acceptance/codex-activation-repair.test.ts", "tests/acceptance/owned-redis.ts"]) sourceHashes[file] = await digest(join(repo, file));
    await mkdir(directory, { recursive: true });
    const historyPath = join(directory, "native-history.json");
    await writeFile(historyPath, JSON.stringify(sanitizeEvidence(histories), null, 2) + "\n", { mode: 0o600 });
    receipt.native_history_file = historyPath;
    await writeFile(join(directory, "receipt.json"), JSON.stringify(sanitizeEvidence(receipt), null, 2) + "\n", { mode: 0o600 });
    for (const threadId of threads) await rpc?.request("thread/archive", { threadId }, AbortSignal.timeout(10_000)).catch(() => undefined);
    await rpc?.close();
    await redis.quit();
    await ownedRedis.close();
    if (server) await killTree(server);
    await rm(socketDir, { recursive: true, force: true });
    if (previousSocket === undefined) delete process.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET;
    else process.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET = previousSocket;
    if (previousRedis === undefined) delete process.env.REDIS_URL;
    else process.env.REDIS_URL = previousRedis;
  }
}, timeout + 30_000);
