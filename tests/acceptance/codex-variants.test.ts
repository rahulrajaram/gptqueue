import { describe, expect, it, vi } from "vitest";
import { mkdir, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { publicEvidence } from "./pi-support.js";
import { mkdtemp, rm } from "node:fs/promises";
import * as pty from 'node-pty';
import { CodexSocketClient } from '../../src/registered-shell/codex-socket.js';
import { startThread, readThread, toolCalls } from './codex-support.js';
import { appConfig, repo } from "./codex-support.js";

const enabled = process.env.GPTQUEUE_ACCEPTANCE_CODEX_VARIANTS === "1";
const codex = process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex";
const timeout = 180_000;
vi.setConfig({ testTimeout: timeout * 5, hookTimeout: 30_000 });

type Run = { code: number | null; signal: string | null; stdout: string; stderr: string };
const run = (args: string[], cwd: string): Promise<Run> => new Promise((resolve) => {
  const child = spawn(codex, args, { cwd, env: { ...process.env }, stdio: ["ignore", "pipe", "pipe"], detached: true });
  let stdout = "", stderr = "";
  child.stdout.on("data", (b) => { stdout = (stdout + String(b)).slice(-1_000_000); });
  child.stderr.on("data", (b) => { stderr = (stderr + String(b)).slice(-50_000); });
  const timer = setTimeout(() => { try { if (child.pid) process.kill(-child.pid, "SIGTERM"); } catch {} }, timeout);
  child.once("close", (code, signal) => { clearTimeout(timer); resolve({ code, signal, stdout, stderr }); });
});

const events = (text: string): Record<string, unknown>[] => text.split("\n").flatMap((line) => {
  try { const value = JSON.parse(line); return value && typeof value === "object" ? [value] : []; } catch { return []; }
});
const threadOf = (rows: Record<string, unknown>[]) => rows.find((e) => typeof e.thread_id === "string" || typeof e.threadId === "string");

type Json = Record<string, any>;
const mechanics = (history: Json, nonce: string): boolean => {
  const calls = toolCalls(history);
  const decode = (call: Json | undefined): Json => (call?.result as Json)?.structuredContent ?? (call?.result as Json)?.structured_content ?? {};
  const sent = calls.find(call => call.tool === 'send_message' && (call.arguments as Json)?.content === nonce);
  const id = decode(sent).message_id;
  const received = calls.filter(call => call.tool === 'receive_message').map(decode).map(result => result.message);
  return typeof id === 'string' && received.some(message => message?.id === id && message.payload?.content === nonce);
};

async function terminalObservation(rpc: CodexSocketClient, socket: string, cwd: string, prompt: string, nonce: string, config: Json, fork?: string): Promise<Json> {
  const overrides = Object.entries(config).flatMap(([key, value]) => ['-c', `${key}=${JSON.stringify(value)}`]);
  const automaticReview = process.env.GPTQUEUE_CODEX_TUI_AUTOREVIEW === '1';
  const args = [...(fork ? ['fork', fork] : []), '--remote', `unix://${socket}`, '--no-alt-screen', '--model', 'gpt-5.6-luna', '-C', cwd,
    ...(automaticReview ? ['--approve-for-me'] : []), ...overrides, prompt];
  const terminal = pty.spawn(codex, args, { cwd, env: { ...process.env, TERM: 'xterm-256color' }, cols: 140, rows: 45, name: 'xterm-256color' });
  let output = '', exited = false;
  const closed = new Promise<void>(resolve => terminal.onExit(() => { exited = true; resolve(); }));
  terminal.onData(data => {
    output = (output + data).slice(-300_000);
    if (data.includes('\x1b[6n')) terminal.write('\x1b[1;1R');
  });
  let history: Json | undefined, failure: string | undefined;
  try {
    const end = Date.now() + 180_000;
    while (!exited && Date.now() < end) {
      const listed = await rpc.request('thread/loaded/list', {}, AbortSignal.timeout(15_000));
      for (const id of (listed.data ?? []) as unknown[]) {
        if (typeof id !== 'string') continue;
        try {
          const candidate = await readThread(rpc, id);
          if (candidate.cwd !== cwd) continue;
          if (mechanics(candidate, nonce)) { history = candidate; break; }
        } catch (error) { failure = String(error); }
      }
      if (history) break;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  } catch (error) { failure = String(error); }
  finally {
    if (!exited) { terminal.kill('SIGTERM'); await Promise.race([closed, new Promise(resolve => setTimeout(resolve, 3000))]); }
    if (!exited) { terminal.kill('SIGKILL'); await closed; }
  }
  return { command: [codex, ...args], passed: Boolean(history), history, failure: history ? undefined : failure,
    approval_mode: automaticReview ? 'per-invocation automatic review' : 'default native terminal policy', terminal: output };
}

describe.skipIf(!enabled)("Codex native variants acceptance", () => {
  it("records persistent exec, explicit resume, fork, and owned app-server observations", async () => {
    const id = randomUUID(), directory = join(repo, ".gptqueue/acceptance/20260912-evaluation/codex-variants", id);
    // The native frontend requires persisted directory trust. Reuse this already
    // trusted repository while keeping processes, threads, and evidence owned.
    const cwd = process.env.GPTQUEUE_CODEX_VARIANTS_NATIVE_ONLY === '1' ? repo : directory;
    await mkdir(directory, { recursive: true });
    const receipt: Record<string, unknown> = { schema_version: 1, database: 15, run_id: id, routes: {} };
    const prompt = `Use only the gptqueue-shared MCP tools. Get your runtime status to learn your automatically registered agent name. Send yourself one status message with content ${id} and idempotency_key ${id}, then receive that exact message. Do not use shell or files.`;
    try {
      const allConfig = { ...await appConfig(), [`projects.${JSON.stringify(cwd)}.trust_level`]: 'trusted' };
      const config = Object.fromEntries(Object.entries(allConfig).filter(([key]) => key === "model_reasoning_effort" || key.startsWith("mcp_servers.gptqueue-shared.")));
      const overrides = Object.entries(config).flatMap(([key, value]) => ["-c", `${key}=${JSON.stringify(value)}`]);
      const nativeOnly = process.env.GPTQUEUE_CODEX_VARIANTS_NATIVE_ONLY === '1';
      const first = nativeOnly ? undefined : await run(["exec", "--json", "--ignore-user-config", ...overrides, "--skip-git-repo-check", "--approve-for-me", "--model", "gpt-5.6-luna", "-C", cwd, prompt], cwd);
      const firstEvents = events(first?.stdout ?? '');
      const thread = threadOf(firstEvents);
      const threadId = typeof thread?.thread_id === "string" ? thread.thread_id : typeof thread?.threadId === "string" ? thread.threadId : undefined;
      if (first) (receipt.routes as Record<string, unknown>).exec = { code: first.code, thread_id: threadId, stderr: first.stderr, events: firstEvents };
      if (threadId) {
        const resumed = await run(["exec", "--json", "--ignore-user-config", ...overrides, "--skip-git-repo-check", "--approve-for-me", "--model", "gpt-5.6-luna", "resume", threadId, `Use only gptqueue-shared MCP. Get your runtime status to learn your automatically registered identity, then send yourself one status message with content resume-${id} and receive that exact envelope.`], cwd);
        (receipt.routes as Record<string, unknown>).resume = { code: resumed.code, stderr: resumed.stderr, events: events(resumed.stdout) };
        (receipt.routes as Record<string, unknown>).fork = { status: "not_run", reason: "Native fork requires the TUI or app-server route; no exec fork substitution" };
      }
      const owned = await mkdtemp(join(tmpdir(), "gq-as-"));
      const socket = join(owned, "control.sock");
      const serverOverrides = Object.entries(allConfig).flatMap(([key, value]) => ["-c", `${key}=${JSON.stringify(value)}`]);
      const server = spawn(codex, ["app-server", "--listen", `unix://${socket}`, ...serverOverrides], { cwd, env: { ...process.env }, stdio: ["ignore", "pipe", "pipe"], detached: true });
      let serverErr = ""; server.stderr.on("data", (b) => { serverErr = (serverErr + String(b)).slice(-20_000); });
      try {
        const end = Date.now() + 15_000;
        while (!existsSync(socket) && server.exitCode === null && Date.now() < end) await new Promise(r => setTimeout(r, 100));
        (receipt.routes as Record<string, unknown>).app_server = { socket, started: existsSync(socket), stderr: serverErr, code: server.exitCode, configuration: "only db15 GPTQueue MCP enabled" };
        (receipt.routes as Record<string, unknown>).tui = { status: "not_run", reason: existsSync(socket) ? "Owned app-server available; native terminal exchange pending" : "Owned server could not start; see retained process error" };
        if (existsSync(socket)) {
          const rpc = new CodexSocketClient(socket, 20_000);
          try {
            const appNonce = `appserver-${id}`;
            const appThread = await startThread(rpc, cwd, 'Use only GPTQueue MCP tools. Do not use shell or filesystem tools.', allConfig);
            await rpc.request('turn/start', { threadId: appThread, input: [{ type: 'text', text: prompt.replaceAll(id, appNonce) }] }, AbortSignal.timeout(30_000));
            let history: Json = {};
            const deadline = Date.now() + 180_000;
            while (Date.now() < deadline) {
              try { history = await readThread(rpc, appThread); }
              catch (error) { history = { read_error: String(error) }; }
              if (mechanics(history, appNonce) || (history.turns as Json[] | undefined)?.at(-1)?.status === 'failed') break;
              await new Promise(resolve => setTimeout(resolve, 1000));
            }
            (receipt.routes as Record<string, unknown>).app_server = { started: true, passed: mechanics(history, appNonce), history, automatic_activation_claimed: false };
            const tuiNonce = `tui-${id}`;
            (receipt.routes as Record<string, unknown>).tui = await terminalObservation(rpc, socket, cwd, prompt.replaceAll(id, tuiNonce), tuiNonce, allConfig);
            if ((threadId ?? appThread) && process.env.GPTQUEUE_CODEX_VARIANTS_SKIP_FORK !== '1') {
              const forkNonce = `fork-${id}`;
              (receipt.routes as Record<string, unknown>).fork = await terminalObservation(rpc, socket, cwd, prompt.replaceAll(id, forkNonce), forkNonce, allConfig, threadId ?? appThread);
            }
          } finally { await rpc.close(); }
        }
      } finally {
        if (server.exitCode === null && server.signalCode === null) {
          const closed = new Promise<void>(resolve => server.once("close", () => resolve()));
          try { process.kill(-server.pid!, "SIGTERM"); } catch { /* exited */ }
          await Promise.race([closed, new Promise<void>(resolve => setTimeout(resolve, 3000))]);
          if (server.exitCode === null && server.signalCode === null) { try { process.kill(-server.pid!, "SIGKILL"); } catch { /* exited */ } await closed; }
        }
        await rm(owned, { recursive: true, force: true });
      }
      receipt.observation_completed = nativeOnly ? (receipt.routes as Json).app_server?.passed === true : first?.code === 0 && Boolean(threadId) && firstEvents.some((e) => JSON.stringify(e).includes("gptqueue-shared"));
      // A completed observation may include a failed native route. Individual
      // route predicates, rather than this test's exit code, decide acceptance.
      expect(receipt.observation_completed).toBe(true);
    } finally {
      await writeFile(join(directory, "receipt.json"), JSON.stringify(publicEvidence(receipt), null, 2) + "\n", { mode: 0o600 });
    }
  });
});
