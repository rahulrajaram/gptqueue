/**
 * Live Claude Code acceptance (operator decision D5: the session may write its own transcript
 * under ~/.claude/projects and update ~/.claude.json; nothing else under ~/.claude changes).
 *
 * Interactive `claude` under a PTY, wired only through generated files in the receipt dir:
 * --mcp-config (this build's bin/gptqueue-claude-mcp) with --strict-mcp-config, --settings with
 * SessionStart/Stop hooks (this build's bin/gptqueue-claude-hook, wrapped to log stdin/stdout),
 * and --setting-sources local so the user's settings and hooks are not loaded. The model starts
 * the event-driven watcher (`gptqueue-inbox-peek --watch`) with run_in_background as the hook says.
 *
 * Criteria: ID-2, AC-4, WK-1/DL-5 (+ send -> watcher exit, send -> turn start), ID-1/ID-5.
 * WK-9 is not covered here (needs the PTY wrapper).
 */
import { privateRedisEnv } from "../helpers/test-redis.js";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import * as pty from "node-pty";
import { afterAll, describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../../src/core/keys.js";
import {
  LIVE, SKIP_REASON, TURN_TIMEOUT_MS, awaitReply, consumption, exchangeVerdict, liveTask, now,
  ownedRedisUrl, receiptDir, redisFor, sleep, startLiveSender, until, writeReceipt,
} from "./support.js";

type Json = Record<string, unknown>;
const MODEL = "claude-opus-5-5";
const repo = resolve(import.meta.dirname, "../..");
const receipt: Json = { runtime: "claude", model: MODEL, criteria: {}, model_turns: 0, decision: "D5" };
const criteria = receipt.criteria as Record<string, Json>;
const cleanup: Array<() => Promise<void> | void> = [];
afterAll(async () => { for (const step of cleanup.splice(0).reverse()) await Promise.resolve(step()).catch(() => undefined); });

const shq = (v: string) => `'${v.replace(/'/g, `'\\''`)}'`;
type HookCall = Readonly<{ at: number; event: string; input: Json; output: string }>;
/** Hook calls logged by the wrapper: one `<ms>\t<event>\t<stdin json>\t<stdout>` line per call. */
const hookCalls = (log: string): HookCall[] => existsSync(log) ? readFileSync(log, "utf8").split("\u001e").filter((l) => l.trim()).map((line) => {
  const [at, event, input, output] = line.replace(/^\n/u, "").split("\u001f");
  let parsed: Json = {}; try { parsed = JSON.parse(input ?? "{}") as Json; } catch { /* keep empty */ }
  return { at: Number(at) * 1000 / 1e6, event: event ?? "", input: parsed, output: output ?? "" };
}) : [];

/** A fresh top-level session: drop the parent agent's CLAUDE* markers (child-session, session id). */
const freshEnv = (): Record<string, string> => privateRedisEnv(Object.fromEntries(Object.entries(process.env)
  .filter((entry): entry is [string, string] => entry[1] !== undefined && !entry[0].startsWith("CLAUDE"))));
type Session = Readonly<{ term: pty.IPty; screen(): string; hookLog: string }>;
const startClaude = (dir: string, label: string, work: string, env: Record<string, string>): Session => {
  const hookLog = join(dir, `hooks-${label}.log`);
  const wrapper = join(dir, "hook-wrapper.sh");
  writeFileSync(wrapper, `#!/bin/sh\n# $1 = event; logs stdin and stdout of the real hook, passes stdout through.\nin=$(cat)\nout=$(printf '%s' "$in" | ${shq(process.execPath)} ${shq(join(repo, "bin/gptqueue-claude-hook"))} "$1")\ncode=$?\n` +
    `printf '%s\\037%s\\037%s\\037%s\\036\\n' "$(date +%s%6N)" "$1" "$in" "$out" >> "$GPTQ_LIVE_HOOK_LOG"\nprintf '%s' "$out"\nexit $code\n`, { mode: 0o700 });
  const settings = join(dir, "settings.json");
  writeFileSync(settings, JSON.stringify({
    hooks: {
      SessionStart: [{ matcher: "", hooks: [{ type: "command", command: `${shq(wrapper)} session-start`, timeout: 15 }] }],
      Stop: [{ matcher: "", hooks: [{ type: "command", command: `${shq(wrapper)} stop`, timeout: 15 }] }],
    },
    permissions: { allow: ["mcp__gptqueue", `Bash(${join(repo, "bin/gptqueue-inbox-peek")}:*)`, `Bash('${join(repo, "bin/gptqueue-inbox-peek")}':*)`] },
  }, null, 2));
  const mcp = join(dir, "mcp.json");
  writeFileSync(mcp, JSON.stringify({ mcpServers: { gptqueue: { command: process.execPath, args: [join(repo, "bin/gptqueue-claude-mcp")], env } } }, null, 2));
  let screen = "";
  const term = pty.spawn("claude", ["--model", MODEL, "--setting-sources", "local", "--settings", settings, "--mcp-config", mcp, "--strict-mcp-config"],
    { cwd: work, cols: 160, rows: 50, name: "xterm-256color", env: { ...freshEnv(), ...env, GPTQ_LIVE_HOOK_LOG: hookLog, TERM: "xterm-256color" } as Record<string, string> });
  term.onData((d) => { screen = (screen + d).slice(-200_000); });
  cleanup.push(async () => {
    writeFileSync(join(dir, `pty-${label}.log`), screen);
    try { term.write("\u0003"); await sleep(300); term.write("\u0003"); await sleep(1_500); term.kill(); } catch { /* exited */ }
  });
  return { term, screen: () => screen, hookLog };
};
/** Accept the folder-trust dialog if shown, then wait for the SessionStart hook. */
const ready = async (s: Session): Promise<HookCall> => {
  let trusted = false;
  return until(() => {
    // The trust dialog defaults to "No, exit": move to "Yes, I trust this folder" first.
    if (!trusted && /Itrustthisfolder/iu.test(plain(s.screen()))) {
      trusted = true;
      setTimeout(() => s.term.write("\u001b[B"), 1_500);
      setTimeout(() => s.term.write("\r"), 2_500);
    }
    return hookCalls(s.hookLog).find((c) => c.event === "session-start");
  }, 60_000, "SessionStart hook");
};
const submit = async (s: Session, text: string) => { s.term.write(`\u001b[200~${text}\u001b[201~`); await sleep(400); s.term.write("\r"); };
// eslint-disable-next-line no-control-regex
const plain = (screen: string) => screen.replace(/\u001b\[[0-9;?>]*[a-zA-Z]/gu, "").replace(/\s/gu, "");
/** The MCP server's session record for one Claude pid (file name session-<pid>-<start>.json). */
const readRecord = (runtimeDir: string, claudePid: number): { file: string; record: Json; seen_at: number } | undefined => {
  const file = readdirSync(runtimeDir).find((f) => f.startsWith(`session-${claudePid}-`) && f.endsWith(".json"));
  return file ? { file, record: JSON.parse(readFileSync(join(runtimeDir, file), "utf8")) as Json, seen_at: now() } : undefined;
};
/** Claim and ack of one message, from MCP tool_use/tool_result pairs in a Claude transcript. */
const transcriptConsumption = (path: string, messageId: string): { claim_id: string; acknowledged: boolean; claim_tool_use: string; ack_tool_use?: string } | undefined => {
  if (!path || !existsSync(path)) return undefined;
  const uses = new Map<string, { name: string; input: Json }>();
  const results: Array<{ id: string; text: string }> = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    let entry: Json; try { entry = JSON.parse(line) as Json; } catch { continue; }
    const content = (entry.message as Json | undefined)?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content as Json[]) {
      if (block.type === "tool_use") uses.set(String(block.id), { name: String(block.name), input: (block.input ?? {}) as Json });
      if (block.type === "tool_result") results.push({ id: String(block.tool_use_id), text: JSON.stringify(block.content) });
    }
  }
  const claim = results.find((r) => uses.get(r.id)?.name === "mcp__gptqueue__claim_tasks" && r.text.includes(messageId));
  const claimId = claim && /claim_id\\?":\\?"([0-9a-f-]{36})/u.exec(claim.text)?.[1];
  if (!claim || !claimId) return undefined;
  const ack = results.find((r) => { const use = uses.get(r.id); return use?.name === "mcp__gptqueue__acknowledge_tasks" && use.input.claim_id === claimId && /acknowledged\\?":[1-9]/u.test(r.text); });
  return ack ? { claim_id: claimId, acknowledged: true, claim_tool_use: claim.id, ack_tool_use: ack.id } : undefined;
};
const sessionStartOutputs: string[] = [];
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };

describe.skipIf(!LIVE)(`live Claude Code (${LIVE ? "enabled" : SKIP_REASON})`, () => {
  it("ID-2, AC-4, WK-1/DL-5, ID-1/ID-5 with interactive Claude Code under a PTY", async () => {
    const redisUrl = ownedRedisUrl();
    const dir = receiptDir("claude");
    receipt.receipt_dir = dir;
    cleanup.push(() => writeReceipt(dir, receipt));
    const work = join(dir, "work"); mkdirSync(work, { recursive: true });
    const runtimeDir = join(dir, "rt"); mkdirSync(runtimeDir, { recursive: true, mode: 0o700 });
    const env = { REDIS_URL: redisUrl, GPTQ_CLAUDE_RUNTIME_DIR: runtimeDir, GPTQ_CLAUDE_NAMES_FILE: join(dir, "names.tsv"),
      GPTQ_LOG_DIR: join(dir, "gptq-logs"), XDG_STATE_HOME: join(dir, "xdg-state") };
    writeFileSync(env.GPTQ_CLAUDE_NAMES_FILE, "");
    const redis = redisFor(redisUrl); cleanup.push(() => { redis.disconnect(); });
    const runHook = (event: string, input: Json, claudePid: number): string => execFileSync(process.execPath, [join(repo, "bin/gptqueue-claude-hook"), event],
      { input: JSON.stringify(input), env: { ...freshEnv(), ...env, GPTQ_CLAUDE_PID: String(claudePid) }, encoding: "utf8" });
    const registry = async () => Object.keys(await redis.hgetall(SESSION_KEYS.registry));

    // ---- Session A: ID-2 from the real SessionStart hook output vs the MCP server's record.
    const a = startClaude(dir, "a", work, env);
    const startA = await ready(a);
    const sessionA = String(startA.input.session_id), transcriptA = String(startA.input.transcript_path ?? "");
    const realStartA = String(/registered as agent '([^']+)'/u.exec(startA.output)?.[1] ?? "");
    sessionStartOutputs.push(startA.output);
    const claudePidA = a.term.pid;
    const recordA = await until(() => readRecord(runtimeDir, claudePidA), 30_000, "session record A");
    const agentA = String(recordA.record.agent);
    // The hook as a descendant of this Claude process (GPTQ_CLAUDE_PID names the ancestor, as documented for launchers).
    const directA = runHook("session-start", { session_id: sessionA, cwd: work }, claudePidA);
    const id2 = !directA.includes("UNCONFIRMED") && directA.includes(`registered as agent '${agentA}'`) && (await registry()).includes(agentA);
    criteria["ID-2"] = { outcome: id2 ? "pass" : "fail", claude_session: sessionA, record: recordA, hook_output: directA,
      real_session_start: { agent: realStartA, unconfirmed: startA.output.includes("UNCONFIRMED"), hook_at: startA.at, record_seen_at: recordA.seen_at } };
    expect(id2).toBe(true);

    // ---- Turn 1: the model starts the watcher as the hook instructs.
    await sleep(3_000);
    await submit(a, "Start the gptqueue inbox watcher exactly as the gptqueue session context instructs (run_in_background). Whenever it wakes you, handle the gptqueue tasks as the gptqueue hooks describe. For now, just start it and stop.");
    receipt.model_turns = Number(receipt.model_turns) + 1;
    const pidPath = join(runtimeDir, `watch-${sessionA.replace(/[^A-Za-z0-9._-]/g, "_")}.pid`);
    const watcher = await until(() => {
      if (!existsSync(pidPath)) return undefined;
      const value = JSON.parse(readFileSync(pidPath, "utf8")) as { pid: number; agent: string };
      return alive(value.pid) ? value : undefined;
    }, TURN_TIMEOUT_MS, "watcher pid file");
    const t1 = now();
    await until(() => hookCalls(a.hookLog).find((c) => c.event === "stop" && c.at >= t1 - 60_000 && c.output === ""), TURN_TIMEOUT_MS, "turn 1 allowed stop");
    await sleep(3_000);
    receipt.watcher = watcher;

    // ---- WK-1 / DL-5: idle wake through watcher exit.
    const sender = await startLiveSender("claude"); cleanup.push(() => sender.close());
    receipt.sender = sender.name;
    const linesBefore = transcriptA && existsSync(transcriptA) ? readFileSync(transcriptA, "utf8").split("\n").length : 0;
    const task = liveTask();
    const sent = await sender.send(agentA, task.content);
    const watcherExit = await until(() => (alive(watcher.pid) ? undefined : now()), 30_000, "watcher exit");
    const turnStart = await until(() => (transcriptA && existsSync(transcriptA) && readFileSync(transcriptA, "utf8").split("\n").length > linesBefore ? now() : undefined), TURN_TIMEOUT_MS, "transcript growth (turn start)");
    receipt.model_turns = Number(receipt.model_turns) + 1;
    const reply = await awaitReply(sender, sent.message_id).catch(() => undefined);
    // The plain MCP server writes no inbox-trace stages (only registered-shell runtime tools do),
    // so consumption evidence comes from the session's own transcript: the claim_tasks result that
    // carries this message id, and the acknowledge_tasks result for that exact claim_id.
    const redisTrace = await consumption(redis, agentA, sent.message_id, 2_000);
    const native = await until(() => transcriptConsumption(transcriptA, sent.message_id), 60_000, "transcript claim+ack").catch(() => undefined);
    const trace = { claim: native ? { message_id: sent.message_id, claim_id: native.claim_id } : redisTrace.claim, ack: native?.acknowledged ? native : redisTrace.ack, rows: redisTrace.rows };
    receipt.native_consumption = native ?? null;
    const verdict = exchangeVerdict({ sender: sender.name, recipient: agentA, requestId: sent.message_id, reply,
      claim: trace.claim, acked: trace.ack !== undefined, expected: task.expected });
    criteria["WK-1"] = { outcome: verdict.outcome === "meets" ? "pass" : "fail", request_id: sent.message_id, reply_id: reply?.message.id,
      reply_content: reply?.message.payload.content, claim_id: trace.claim?.claim_id, watcher_pid: watcher.pid };
    criteria["DL-5"] = { outcome: verdict.outcome === "meets" ? "pass" : "fail", verdict };
    criteria["LT-3"] = { send_to_watcher_exit_ms: watcherExit - sent.sent_at, send_to_turn_start_ms: turnStart - sent.sent_at,
      turn_start_evidence: "first new transcript line after send (polled 25 ms)", target_p95_ms: 1000, samples: 1 };
    receipt.trace_wk1 = trace.rows;
    const settled = now();
    await until(() => hookCalls(a.hookLog).find((c) => c.event === "stop" && c.at >= settled - 120_000 && c.output === ""), TURN_TIMEOUT_MS, "wake turn allowed stop").catch(() => undefined);

    // ---- ID-1 / ID-5: a second session in the same directory gets the AGENT_NAME_HELD fallback.
    const b = startClaude(dir, "b", work, env);
    const startB = await ready(b);
    const sessionB = String(startB.input.session_id);
    sessionStartOutputs.push(startB.output);
    const recordB = await until(() => readRecord(runtimeDir, b.term.pid), 30_000, "session record B");
    const agentB = String(recordB.record.agent);
    const names = await registry();
    const id5 = agentB !== agentA && agentB.startsWith(`${agentA}-`) && names.includes(agentA) && names.includes(agentB);
    criteria["ID-1"] = { outcome: id5 ? "pass" : "fail", agentA, agentB, claude_session_b: sessionB, hook_output_b: startB.output };
    criteria["ID-5"] = { outcome: id5 ? "pass" : "fail", cwd: work, agentA, agentB, registry: names.filter((n) => n.startsWith(agentA)) };

    // ---- AC-4: the real Stop hook for session B (no model turn) blocks with exact counts.
    const pending = await sender.send(agentB, liveTask().content);
    const claudePidB = String(b.term.pid);
    const hookOut = runHook("stop", { session_id: sessionB, cwd: work, stop_hook_active: false }, b.term.pid);
    const decision = JSON.parse(hookOut || "{}") as Json;
    const ac4 = decision.decision === "block" && String(decision.reason).includes(`'${agentB}'`) &&
      String(decision.reason).includes("1 unclaimed; claimed by this session but not acknowledged: 0; claimed by other sessions: 0");
    criteria["AC-4"] = { outcome: ac4 ? "pass" : "fail", pending_id: pending.message_id, claude_pid_b: claudePidB, decision };
    criteria["WK-9"] = { outcome: "not_covered", reason: "needs the PTY wrapper (bin/gptqueue-pty); not exercised in this live run" };

    expect(verdict.reasons).toEqual([]);
    expect(id5).toBe(true);
    expect(ac4).toBe(true);
  }, 20 * 60_000);

  // ID-2 at the real SessionStart: Claude Code runs the SessionStart hook before the MCP server
  // (bin/gptqueue-claude-mcp) has written its session record. The hook now waits (bounded) for
  // the record, and the watcher command it gives is name-free (the watcher resolves the
  // registered name itself), so every session's context must be confirmed.
  it("ID-2: the real SessionStart hook output is confirmed for every session", () => {
    expect(sessionStartOutputs.length).toBeGreaterThan(0);
    for (const output of sessionStartOutputs) expect(output).not.toContain("UNCONFIRMED");
  });
});
