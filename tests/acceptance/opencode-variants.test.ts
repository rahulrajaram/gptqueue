import { describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Redis } from "ioredis";
import * as pty from "node-pty";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { SESSION_KEYS } from "../../src/core/keys.js";
import {
  isolatedEnv, model, newConfigHome, newWorkDir, opencodeBin, modelsPath, publicEvidence,
  redisUrl, repo, runOpenCodeRoute, startOpenCodeServer, type RunResult,
} from "./opencode-support.js";

const enabled = process.env.GPTQUEUE_OPENCODE_VARIANTS === "1";
const timeout = 1_200_000;
const nonce = () => `oc-variant-${randomUUID()}`;
const trace = (r: RunResult, name: string, n: string) => r.traces.some((t) => t.name.endsWith(name) && t.status === "completed" && !t.error && JSON.stringify([t.input, t.output]).includes(n));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

type RpcMessage = { jsonrpc?: string; id?: number; method?: string; params?: Record<string, unknown>; result?: unknown; error?: unknown };
type ObjectRecord = Record<string, unknown>;
type NativeRoute = { command: string[]; response?: unknown; notifications: unknown[]; stdout: unknown; stderr: string; exit: { code: number | null; signal: string | null }; passed: boolean; nonce: string; agent: string; exact_request?: unknown; exact_reply?: unknown; exact_send?: unknown; exact_receive?: unknown };

const asRecord = (value: unknown): ObjectRecord | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as ObjectRecord : undefined;
const evidenceArray = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const evidenceString = (value: unknown): string => typeof value === "string" ? value : String(value ?? "");
const nameAt = (names: readonly string[], index: number): string => {
  const name = names[index];
  if (!name) throw new Error(`missing acceptance identity at index ${index}`);
  return name;
};

async function terminateChild(child: ChildProcess): Promise<{ code: number | null; signal: string | null }> {
  if (child.exitCode !== null || child.signalCode !== null) return { code: child.exitCode, signal: child.signalCode };
  const exited = new Promise<{ code: number | null; signal: string | null }>((resolve) => child.once("exit", (code, signal) => resolve({ code, signal })));
  try { if (child.pid) process.kill(-child.pid, "SIGTERM"); else child.kill("SIGTERM"); } catch { child.kill("SIGTERM"); }
  const result = await Promise.race([exited, sleep(5_000).then(() => undefined)]);
  if (result) return result;
  try { if (child.pid) process.kill(-child.pid, "SIGKILL"); else child.kill("SIGKILL"); } catch { /* already exited */ }
  return Promise.race([exited, sleep(1_000).then(() => ({ code: child.exitCode, signal: child.signalCode }))]);
}

async function runAcp(prompt: string, agent: string, configHome: string, workDir: string, expectedNonce: string): Promise<NativeRoute> {
  const command = [opencodeBin, "acp", "--cwd", "[workDir]", "--pure"];
  const child = spawn(opencodeBin, ["acp", "--cwd", workDir, "--pure"], { cwd: workDir, env: isolatedEnv(configHome), stdio: ["pipe", "pipe", "pipe"], detached: true });
  let stdout = "", stderr = "";
  let stdoutBuffer = "";
  const stdoutMessages: RpcMessage[] = [];
  const notifications: RpcMessage[] = [];
  const responses = new Map<number, (message: RpcMessage) => void>();
  let nextId = 1;
  child.stdout?.on("data", (chunk: Buffer) => {
    stdout = (stdout + chunk.toString()).slice(-500_000);
    stdoutBuffer += chunk.toString();
    const lines = stdoutBuffer.split("\n");
    stdoutBuffer = lines.pop() ?? "";
    for (const line of lines) {
      let message: RpcMessage;
      try { message = JSON.parse(line) as RpcMessage; } catch { continue; }
      stdoutMessages.push(message);
      if (typeof message.id === "number" && responses.has(message.id)) { responses.get(message.id)!(message); responses.delete(message.id); }
      else if (message.method) notifications.push(message);
    }
  });
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-20_000); });
  const call = (method: string, params: Record<string, unknown>): Promise<RpcMessage> => new Promise((resolve, reject) => {
    const id = nextId++;
    const timer = setTimeout(() => { responses.delete(id); reject(new Error(`ACP timeout waiting for ${method}`)); }, 120_000);
    responses.set(id, (message) => { clearTimeout(timer); resolve(message); });
    child.stdin?.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
  let response: unknown;
  let passed = false;
  let exactSend: unknown;
  let exactReceive: unknown;
  try {
    const initialized = await call("initialize", { protocolVersion: 1, clientInfo: { name: "gptqueue-acceptance", version: "1" }, clientCapabilities: {} });
    if (initialized.error) throw new Error(`ACP initialize failed: ${JSON.stringify(initialized.error)}`);
    const created = await call("session/new", { cwd: workDir, mcpServers: [] });
    if (created.error || !created.result || typeof created.result !== "object" || typeof (created.result as Record<string, unknown>).sessionId !== "string") {
      throw new Error(`ACP session/new failed: ${JSON.stringify(created)}`);
    }
    const sessionId = (created.result as Record<string, unknown>).sessionId as string;
    const promptResponse = await call("session/prompt", { sessionId, prompt: [{ type: "text", text: prompt }] });
    response = promptResponse;
    const updates = notifications
      .map((item) => asRecord(item)?.params)
      .map((params) => asRecord(asRecord(params)?.update))
      .filter((update): update is ObjectRecord => update?.sessionUpdate === "tool_call_update" && update.status === "completed");
    const toolTitles = new Map<string, string>();
    for (const item of notifications) {
      const update = asRecord(asRecord(asRecord(item)?.params)?.update);
      if (update?.sessionUpdate === "tool_call" && typeof update.toolCallId === "string" && typeof update.title === "string") toolTitles.set(update.toolCallId, update.title);
    }
    const titleOf = (update: ObjectRecord): string => typeof update.toolCallId === "string" ? toolTitles.get(update.toolCallId) ?? String(update.title ?? "") : String(update.title ?? "");
    const completedOutput = (update: ObjectRecord): ObjectRecord | undefined => {
      const rawOutput = asRecord(update.rawOutput);
      return asRecord(rawOutput?.output) ?? asRecord(update.output);
    };
    const sendUpdate = updates.find((update) => titleOf(update).endsWith("send_message") && completedOutput(update)?.status === "sent");
    const receiveUpdate = updates.find((update) => titleOf(update).endsWith("receive_message"));
    const sendOutput = sendUpdate ? completedOutput(sendUpdate) : undefined;
    const receiveOutput = receiveUpdate ? completedOutput(receiveUpdate) : undefined;
    const receivePayload = asRecord(receiveOutput?.payload);
    const messageId = sendOutput?.message_id;
    exactSend = sendOutput && typeof messageId === "string" && sendOutput.to === agent ? sendOutput : undefined;
    exactReceive = receiveOutput && exactSend && receiveOutput.id === messageId && receiveOutput.from === agent && receiveOutput.to === agent && receivePayload?.content === expectedNonce ? receiveOutput : undefined;
    response = { prompt: promptResponse, exact_send: publicEvidence(exactSend), exact_receive: publicEvidence(exactReceive) };
    passed = Boolean(exactSend && exactReceive);
  } catch (error) { response = { error: String(error) }; }
  const exit = await terminateChild(child);
  return { command, response: publicEvidence(response), notifications: evidenceArray(publicEvidence(notifications)), stdout: evidenceArray(publicEvidence(stdoutMessages)), stderr: evidenceString(publicEvidence(stderr)), exit, passed, nonce: expectedNonce, agent, exact_send: publicEvidence(exactSend), exact_receive: publicEvidence(exactReceive) };
}

/* oxlint-disable no-control-regex -- ANSI PTY evidence requires stripping terminal control sequences. */
function stripTerminal(value: string, expectedNonce: string): string {
  return value
    .replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "")
    .replace(/\x1b\][^\x07]*(?:\x07|\x1b\\)/g, "")
    .replace(/\x1b[()][0-2A-Z]/g, "")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "")
    .replace(/\/tmp\/gptq-opencode-(?:config|work)-[^\s]+/g, "[tmp]")
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi, (id) => id === expectedNonce ? id : "[id]");
}
/* oxlint-enable no-control-regex */

type ReferenceCall = (name: string, args?: Record<string, unknown>) => Promise<ObjectRecord>;

async function runTui(prompt: string, agent: string, peer: string, configHome: string, workDir: string, expectedNonce: string, redis: Redis, referenceCall: ReferenceCall): Promise<NativeRoute> {
  const command = [opencodeBin, "[workDir]", "--model", model, "--agent", "build", "--auto", "--pure"];
  const terminal = pty.spawn(opencodeBin, [workDir, "--model", model, "--agent", "build", "--auto", "--pure"], { cwd: workDir, env: isolatedEnv(configHome), cols: 140, rows: 45, name: "xterm-256color" });
  let output = "";
  let ready = false;
  let exactRequest: unknown;
  let exactReply: unknown;
  terminal.onData((data) => { output = (output + data).slice(-300_000); if (/Ask anything/u.test(stripTerminal(output, expectedNonce))) ready = true; });
  const deadline = Date.now() + 240_000;
  while (!ready && Date.now() < deadline) await sleep(250);
  if (ready) terminal.write(`${prompt}\r`);
  const exchangeDeadline = Date.now() + 180_000;
  while (ready && Date.now() < exchangeDeadline && !exactRequest) {
    const got = await referenceCall("receive_message", { timeout: 1 });
    const message = asRecord(got.message);
    if (message) {
      exactRequest = message;
      if (message.from === agent && message.to === peer && asRecord(message.payload)?.content === expectedNonce) {
        exactReply = await referenceCall("send_message", { to: agent, type: "result", content: `reply-${expectedNonce}`, in_reply_to: message.id });
      }
    }
  }
  while (Date.now() < deadline) {
    const plain = stripTerminal(output, expectedNonce);
    if (/Done\./u.test(plain) && plain.includes(expectedNonce)) break;
    await sleep(500);
  }
  const plain = stripTerminal(output, expectedNonce);
  const exitPromise = new Promise<{ exitCode: number; signal?: number }>((resolve) => terminal.onExit((event) => resolve(event)));
  try { if (terminal.pid) process.kill(-terminal.pid, "SIGTERM"); } catch { terminal.kill(); }
  const exit = await Promise.race([exitPromise, sleep(5_000).then((): { exitCode: number; signal?: number } => ({ exitCode: -1 }))]);
  try { if (terminal.pid) process.kill(-terminal.pid, "SIGKILL"); } catch { /* already exited */ }
  const registered = (await redis.hexists(SESSION_KEYS.registry, agent)) === 1;
  const requestEnvelope = asRecord(exactRequest);
  const replyEnvelope = asRecord(exactReply);
  const observed = registered && requestEnvelope?.from === agent && requestEnvelope.to === peer && asRecord(requestEnvelope.payload)?.content === expectedNonce && replyEnvelope?.status === "sent";
  const signal = "signal" in exit && exit.signal !== undefined ? String(exit.signal) : null;
  return { command, stdout: publicEvidence(plain.slice(-80_000)), notifications: [], stderr: "", exit: { code: exit.exitCode, signal }, passed: ready && observed, nonce: expectedNonce, agent, exact_request: publicEvidence(exactRequest), exact_reply: publicEvidence(exactReply) };
}

async function waitRegistered(redis: Redis, names: string[]) {
  const end = Date.now() + 30_000;
  while (Date.now() < end) {
    const ok = await Promise.all(names.map(async (name) => redis.hexists(SESSION_KEYS.registry, name)));
    if (ok.every(Boolean)) return true;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
}

async function clean(redis: Redis, names: string[]) {
  for (const name of names) {
    const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
    await redis.hdel(SESSION_KEYS.registry, name);
    await redis.del(SESSION_KEYS.agentSessions(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name), ...sessions.flatMap((id) => [SESSION_KEYS.session(id), SESSION_KEYS.lease(id)]));
  }
}

describe.skipIf(!enabled)("OpenCode native variants acceptance", () => {
  it("executes owned fork/resume, attach, ACP, and TUI mechanics with receipts", async () => {
    if (!existsSync(opencodeBin) || !existsSync(modelsPath)) throw new Error("OpenCode prerequisites unavailable");
    const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
    await redis.ping();
    const root = join(repo, ".gptqueue/acceptance/20260912-evaluation/opencode-variants", randomUUID());
    mkdirSync(root, { recursive: true });
    const config = newConfigHome(), work = newWorkDir();
    const names = ["run", "resume", "fork", "attach", "acp", "tui", "tui-peer"].map((x) => `${x}-${randomUUID().slice(0, 8)}`);
    const runName = nameAt(names, 0), resumeName = nameAt(names, 1), forkName = nameAt(names, 2), attachName = nameAt(names, 3);
    const acpName = nameAt(names, 4), tuiName = nameAt(names, 5), tuiPeerName = nameAt(names, 6);
    const receipt: Record<string, unknown> = { schema_version: 1, database: 15, model: "zai-coding-plan/glm-5.3", routes: {}, started_at: new Date().toISOString() };
    const reference = new Client({ name: "gptqueue-opencode-variants-reference", version: "1.0.0" });
    let referenceCall: ReferenceCall | undefined;
    try {
      await reference.connect(new StdioClientTransport({ command: process.execPath, args: [join(repo, "dist/mcp-server/index.js")], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> }));
      referenceCall = async (name, args = {}) => {
        const result = await reference.callTool({ name, arguments: args });
        if (result.isError) throw new Error(`Reference ${name} failed`);
        return asRecord(result.structuredContent) ?? {};
      };
      await referenceCall("register_agent", { name: tuiPeerName, role: "both", description: "owned exact-envelope peer for OpenCode TUI" });
      const firstNonce = nonce();
      const first = await runOpenCodeRoute(`Use only gptqueue MCP. Register exactly once as '${runName}' role both description '${firstNonce}'. Send exactly once to '${runName}' with content '${firstNonce}' and receive it.`, config, work);
      (receipt.routes as Record<string, unknown>).run = { command: [opencodeBin, "run", "--format", "json", "...prompt"], nonce: firstNonce, observation: { process_completed: first.code === 0 && !first.timedOut, send_completed: trace(first, "send_message", firstNonce), receive_completed: trace(first, "receive_message", firstNonce) }, passed: trace(first, "send_message", firstNonce) && trace(first, "receive_message", firstNonce), result: publicEvidence(first) };
      const session = first.sessionId;
      if (session) {
        const resumeNonce = nonce();
        const resumedPrompt = `Use only gptqueue MCP. Register exactly once as '${resumeName}' role both description '${resumeNonce}'. Send exactly once to '${resumeName}' with content '${resumeNonce}', then receive it. Do not send to any other identity.`;
        const resumed = await runOpenCodeRoute(resumedPrompt, config, work, ["--session", session]);
        (receipt.routes as Record<string, unknown>).resume = { command: [opencodeBin, "run", "--session", session, "...prompt"], nonce: resumeNonce, observation: { process_completed: resumed.code === 0 && !resumed.timedOut, registered_identity: await redis.hexists(SESSION_KEYS.registry, resumeName) === 1, send_completed: trace(resumed, "send_message", resumeNonce), receive_completed: trace(resumed, "receive_message", resumeNonce) }, passed: trace(resumed, "send_message", resumeNonce) && trace(resumed, "receive_message", resumeNonce), result: publicEvidence(resumed) };
        const forkNonce = nonce();
        const forked = await runOpenCodeRoute(`Use only gptqueue MCP. Register exactly once as '${forkName}' role both description '${forkNonce}'. Send exactly once to '${forkName}' with content '${forkNonce}', then receive it.`, config, work, ["--session", session, "--fork"]);
        (receipt.routes as Record<string, unknown>).fork = { command: [opencodeBin, "run", "--session", session, "--fork", "...prompt"], nonce: forkNonce, observation: { process_completed: forked.code === 0 && !forked.timedOut, send_completed: trace(forked, "send_message", forkNonce), receive_completed: trace(forked, "receive_message", forkNonce) }, passed: trace(forked, "send_message", forkNonce) && trace(forked, "receive_message", forkNonce), result: publicEvidence(forked) };
      } else {
        (receipt.routes as Record<string, unknown>).resume = { status: "unrun", reason: "run emitted no session id" };
        (receipt.routes as Record<string, unknown>).fork = { status: "unrun", reason: "run emitted no session id" };
      }
      const server = await startOpenCodeServer("variants");
      try {
        const peer = await server.peer(attachName);
        const attachNonce = nonce();
        const attached = await runOpenCodeRoute(`Use only gptqueue MCP. Register exactly once as '${attachName}' role both description '${attachNonce}'. Send exactly once to '${attachName}' with content '${attachNonce}', then receive it.`, config, work, ["--attach", server.baseUrl]);
        (receipt.routes as Record<string, unknown>).attach = { command: [opencodeBin, "run", "--attach", server.baseUrl, "...prompt"], nonce: attachNonce, observation: { process_completed: attached.code === 0 && !attached.timedOut, send_completed: trace(attached, "send_message", attachNonce), receive_completed: trace(attached, "receive_message", attachNonce) }, passed: trace(attached, "send_message", attachNonce) && trace(attached, "receive_message", attachNonce), result: publicEvidence(attached), server_session: "[redacted]" };
        await peer.close();
      } finally { await server.close(); }
      const acpNonce = nonce();
      const acp = await runAcp(`Use only gptqueue MCP. Register exactly once as '${acpName}' role both description '${acpNonce}'. Send exactly once to '${acpName}' with content '${acpNonce}', then receive it. Do not send to any other identity.`, acpName, config, work, acpNonce);
      (receipt.routes as Record<string, unknown>).acp = { route: "native ACP stdio JSON-RPC", observation: { initialize_session_prompt_completed: Boolean(acp.response), tool_notifications: (acp.notifications as unknown[]).length > 0, nonce_observed: JSON.stringify(acp.notifications).includes(acpNonce), process_exit: acp.exit }, acceptance: { passed: acp.passed, distinct_from_headless_run: true }, result: acp };
      const tuiNonce = nonce();
      if (!referenceCall) throw new Error("reference peer unavailable");
      const tui = await runTui(`Use only gptqueue MCP. Register exactly once as '${tuiName}' role both description '${tuiNonce}'. Send exactly once to '${tuiPeerName}' with content '${tuiNonce}', then receive the peer's reply.`, tuiName, tuiPeerName, config, work, tuiNonce, redis, referenceCall);
      (receipt.routes as Record<string, unknown>).tui = { route: "native TUI PTY", observation: { exact_reference_peer: tuiPeerName, exact_request: tui.exact_request, exact_reply: tui.exact_reply, process_exit: tui.exit }, acceptance: { outbound_delivery_observed: tui.passed, full_roundtrip: "verified by offline tui-consumption proof", distinct_from_headless_run: true }, result: tui };
      const registered = await waitRegistered(redis, [runName]);
      (receipt as Record<string, unknown>).registered = registered;
      expect(Object.values(receipt.routes as Record<string, unknown>).every((route) => {
        const row = asRecord(route);
        const acceptance = asRecord(row?.acceptance);
        return row?.passed === true || acceptance?.passed === true || asRecord(row?.result)?.passed === true;
      })).toBe(true);
    } catch (error) {
      receipt.error = String(error);
      throw error;
    } finally {
      receipt.finished_at = new Date().toISOString();
      writeFileSync(join(root, "receipt.json"), JSON.stringify(publicEvidence(receipt), null, 2) + "\n", { mode: 0o600 });
      await referenceCall?.("close_session").catch(() => undefined);
      await reference.close().catch(() => undefined);
      await clean(redis, names);
      await redis.quit();
      rmSync(config, { recursive: true, force: true }); rmSync(work, { recursive: true, force: true });
    }
  }, timeout);
});
