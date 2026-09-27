import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { Redis } from "ioredis";
import { describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { sanitizeEvidence } from "./public-evidence.js";
import { nodePrefixPath } from "./local-tools.js";

type Json = Record<string, unknown>;
type Category = "UNSUPPORTED_CLIENT" | "OTHER_ERROR" | "SUCCESSFUL_MECHANICAL_REGISTRATION";
const enabled = process.env.GPTQUEUE_GEMINI_PREFLIGHT === "1";
const repo = resolve(import.meta.dirname, "../..");
const gemini = nodePrefixPath("bin/gemini");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/gemini-preflight");
const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const safe = (value: unknown): unknown => sanitizeEvidence(value, { parseEmbeddedJson: true });

const stopProcess = async (child: ChildProcess): Promise<void> => {
  const waitForDeath = async (timeout: number): Promise<boolean> => {
    if (child.exitCode !== null || child.signalCode !== null) return true;
    await new Promise<void>(resolveWait => {
      const timer = setTimeout(resolveWait, timeout);
      child.once("close", () => { clearTimeout(timer); resolveWait(); });
    });
    return child.exitCode !== null || child.signalCode !== null;
  };
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { if (child.pid) process.kill(-child.pid, "SIGTERM"); else child.kill("SIGTERM"); } catch { child.kill("SIGTERM"); }
  if (await waitForDeath(5_000)) return;
  try { if (child.pid) process.kill(-child.pid, "SIGKILL"); else child.kill("SIGKILL"); } catch { child.kill("SIGKILL"); }
  if (!(await waitForDeath(2_000))) throw new Error("Gemini process did not exit after SIGKILL");
};

const runCli = (cwd: string, redisUrl: string, prompt: string): Promise<Readonly<{ code: number | null; timedOut: boolean; error?: string; stdout: string; stderr: string }>> => new Promise(resolveRun => {
  const child = spawn(gemini, ["--extensions", "none", "--allowed-mcp-server-names", "gptqueue", "--approval-mode", "yolo", "--output-format", "stream-json", "--prompt", prompt], {
    cwd, detached: true, stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_UPDATE_NOTIFIER: "1", GPTQUEUE_REDIS_URL: redisUrl },
  });
  let stdout = "", stderr = "", timedOut = false, spawnError: string | undefined;
  child.stdout?.on("data", chunk => { stdout = (stdout + String(chunk)).slice(-2_000_000); });
  child.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-100_000); });
  child.once("error", error => { spawnError = String(error); });
  const timer = setTimeout(() => { timedOut = true; void stopProcess(child).catch(error => { stderr += `\n${String(error)}`; }); }, 180_000);
  child.once("close", code => { clearTimeout(timer); resolveRun({ code, timedOut, error: spawnError, stdout, stderr }); });
});

const classify = (result: Readonly<{ code: number | null; timedOut: boolean; error?: string; stdout: string; stderr: string }>, registered: boolean, exactSelfMessage: boolean): Category => {
  if (registered && exactSelfMessage && result.code === 0 && !result.timedOut) return "SUCCESSFUL_MECHANICAL_REGISTRATION";
  const text = `${result.error ?? ""}\n${result.stderr}`;
  return /unsupported|not supported|unknown option|unrecognized option|command not found|ENOENT/iu.test(text) ? "UNSUPPORTED_CLIENT" : "OTHER_ERROR";
};

describe.skipIf(!enabled)("Gemini prerequisite preflight", () => {
  it("records assisted registration and exact self-message mechanics only", async () => {
    const run = randomUUID(), artifactDir = join(artifactRoot, run), workspace = await mkdtemp(join(tmpdir(), "gptq-gemini-preflight-"));
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const agent = `qualification-gemini-${run}`, nonce = randomUUID();
    const receipt: Json = { schema_version: 1, run_id: run, route: "gemini-cli", category: "OTHER_ERROR", passed: false, scope: "assisted registration and self-message only; no pair or initiative qualification", phases: [] };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: safe(value) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(safe(receipt), null, 2)}\n`, { mode: 0o600 });
    };
    const sourceFiles = ["tests/acceptance/qualification-gemini-preflight.test.ts", "dist/mcp-server/index.js"];
    const sourceHashes = async (): Promise<Json> => Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
    const cliHash = async (): Promise<string> => digest(gemini);
    let owned: OwnedRedis | undefined;
    let redis: Redis | undefined;
    let childResult: Awaited<ReturnType<typeof runCli>> | undefined;
    let failure: unknown;
    try {
      receipt.source_hashes = await sourceHashes();
      receipt.gemini_cli_sha256_before = await cliHash();
      await persist("source-provenance", { source_hashes: receipt.source_hashes, gemini_cli_sha256: receipt.gemini_cli_sha256_before });
      owned = await startOwnedRedis();
      receipt.redis = { protocol: "redis:", hostname: "127.0.0.1", port: new URL(owned.url).port, database: "15" };
      await persist("owned-redis-ready", receipt.redis);
      const settings = { hooksConfig: { enabled: false }, tools: { core: [] }, security: { folderTrust: { enabled: false } }, mcpServers: { gptqueue: { command: process.execPath, args: [join(repo, "dist/mcp-server/index.js")], env: { REDIS_URL: owned.url }, trust: true } } };
      await mkdir(join(workspace, ".gemini"), { recursive: true });
      await writeFile(join(workspace, ".gemini/settings.json"), JSON.stringify(settings), { mode: 0o600 });
      await persist("workspace-ready", { workspace: "[redacted]", settings: { hooksConfig: settings.hooksConfig, tools: settings.tools, allowedServer: "gptqueue" } });
      redis = new Redis(owned.url);
      const prompt = `Use only the gptqueue MCP tools. Register yourself as ${agent}, role both. Send yourself a status message with content ${nonce}, idempotency_key ${nonce}. Receive it and verify the exact message id and content. Do not use shell, files, or contact another agent. Return the original tool results.`;
      childResult = await runCli(workspace, owned.url, prompt);
      const parsedEvents = childResult.stdout.split("\n").flatMap(line => { try { return [JSON.parse(line) as unknown]; } catch { return []; } });
      const events = parsedEvents.map(safe);
      const tools = parsedEvents.filter(value => object(value)?.type === "tool_use");
      const results = parsedEvents.filter(value => object(value)?.type === "tool_result");
      const decoded = (call: Json | undefined): Json | undefined => {
        const id = typeof call?.tool_id === "string" ? call.tool_id : undefined;
        const result = results.find(event => object(event)?.tool_id === id);
        const output = object(result)?.output;
        if (typeof output === "string") { try { return object(JSON.parse(output)); } catch { return undefined; } }
        return object(output);
      };
      const sent = tools.find(value => { const row = object(value); return String(row?.tool_name).endsWith("send_message") && object(row?.parameters)?.content === nonce; });
      const received = tools.find(value => String(object(value)?.tool_name).endsWith("receive_message"));
      const sentResult = decoded(sent as Json | undefined), receivedResult = decoded(received as Json | undefined);
      const message = object(receivedResult?.message) ?? receivedResult;
      const registered = Boolean(await redis.hget(SESSION_KEYS.registry, agent));
      const exactSelfMessage = Boolean(sentResult?.message_id && message?.id === sentResult.message_id && message.from === agent && message.to === agent && object(message.payload)?.content === nonce);
      receipt.events = events;
      receipt.process = { code: childResult.code, timed_out: childResult.timedOut, error: childResult.error };
      receipt.tool_call_joins = { send_call_id: object(sent)?.tool_id, receive_call_id: object(received)?.tool_id, send_result_joined: Boolean(sentResult), receive_result_joined: Boolean(receivedResult), registered, exact_self_message: exactSelfMessage, expected_nonce: nonce };
      receipt.category = classify(childResult, registered, exactSelfMessage);
      receipt.passed = receipt.category === "SUCCESSFUL_MECHANICAL_REGISTRATION";
      await persist("sanitized-cli-observation", { process: receipt.process, stdout: safe(childResult.stdout), stderr: safe(childResult.stderr), events, tool_call_joins: receipt.tool_call_joins, category: receipt.category });
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.category = "OTHER_ERROR";
      receipt.passed = false;
      await persist("failure", { error: String(error) }).catch(() => undefined);
    } finally {
      const cleanup: Json[] = [];
      for (const [name, close] of [["gemini-process", async () => { if (childResult === undefined) return; }], ["redis", async () => { await redis?.quit(); }], ["owned-redis", async () => { await owned?.close(); }], ["workspace", async () => { await rm(workspace, { recursive: true, force: true }); }]] as const) {
        try { await close(); cleanup.push({ name, status: "fulfilled" }); } catch (error) { cleanup.push({ name, status: "rejected", error: String(error) }); failure ??= error; }
      }
      receipt.cleanup = cleanup;
      try { receipt.source_hashes_after = await sourceHashes(); receipt.gemini_cli_sha256_after = await cliHash(); } catch (error) { receipt.provenance_error = String(error); failure ??= error; }
      if (JSON.stringify(receipt.source_hashes) !== JSON.stringify(receipt.source_hashes_after) || receipt.gemini_cli_sha256_before !== receipt.gemini_cli_sha256_after) { receipt.passed = false; receipt.category = "OTHER_ERROR"; failure ??= new Error("source or Gemini executable changed during probe"); }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(["UNSUPPORTED_CLIENT", "OTHER_ERROR", "SUCCESSFUL_MECHANICAL_REGISTRATION"]).toContain(receipt.category);
  }, 210_000);
});
