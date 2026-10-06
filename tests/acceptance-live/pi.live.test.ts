/**
 * Live Pi acceptance: WK-1/DL-5, WK-2, ID-5, LT-3, LT-4 with the real `pi` CLI in RPC mode.
 * Each Pi process loads only a generated extension that wraps this worktree's
 * createRegisteredPiExtension (dist/registered-shell/pi-extension.js), which starts the
 * registered sidecar (bin/gptqueue-session --client pi) against the private Redis.
 * Sessions are written under the receipt directory; no user extensions, skills or context files.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../../src/core/keys.js";
import {
  LIVE, SKIP_REASON, TURN_TIMEOUT_MS, awaitReply, consumption, exchangeVerdict, isolatedGptqueueEnv, liveTask, now,
  ownedRedisUrl, readTrace, receiptDir, redisFor, sleep, startLiveSender, until, writeReceipt,
} from "./support.js";

type Json = Record<string, unknown>;
/** text: the final assistant text of a run, on agent_end only. */
type PiEvent = Readonly<{ at: number; type: string; text?: string }>;
/** The last assistant message's text in an agent_end event's messages. */
const finalAssistantText = (event: Json): string | undefined => {
  const messages = Array.isArray(event.messages) ? event.messages as Json[] : [];
  const last = messages.filter((m) => m && m.role === "assistant").at(-1);
  if (!last) return undefined;
  const content = last.content;
  return typeof content === "string" ? content
    : Array.isArray(content) ? (content as Json[]).filter((c) => c?.type === "text").map((c) => String(c.text ?? "")).join("") : undefined;
};
const MODEL = "zai/glm-5.3-flash";
const repo = resolve(import.meta.dirname, "../..");
const receipt: Json = { runtime: "pi", model: MODEL, criteria: {}, model_runs: 0 };
const criteria = receipt.criteria as Record<string, Json>;
const cleanup: Array<() => Promise<void> | void> = [];
afterAll(async () => { for (const step of cleanup.splice(0).reverse()) await Promise.resolve(step()).catch(() => undefined); });

type PiProcess = Readonly<{ child: ChildProcess; events: PiEvent[]; send(command: Json): void; log(): string }>;
const startPi = (dir: string, label: string, redisUrl: string, work: string): PiProcess => {
  const extension = join(dir, `gptqueue-live-${label}.ts`);
  writeFileSync(extension,
    `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(repo, "dist/registered-shell/pi-extension.js")).href)};\n` +
    `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl, nodePath: process.execPath, sidecarPath: join(repo, "bin/gptqueue-session") })});\n`, { mode: 0o600 });
  const sessions = join(dir, `sessions-${label}`); mkdirSync(sessions, { recursive: true });
  const child = spawn("pi", ["--mode", "rpc", "--model", MODEL, "--session-dir", sessions, "-ne", "-e", extension,
    "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-builtin-tools",
    "--append-system-prompt", "Use only the gptqueue tools for gptqueue work."],
  { cwd: work, env: { ...process.env, ...isolatedGptqueueEnv(dir), REDIS_URL: redisUrl }, detached: true, stdio: ["pipe", "pipe", "pipe"] });
  const events: PiEvent[] = [];
  let buffer = "", raw = "";
  child.stdout!.on("data", (chunk: Buffer) => {
    const at = now();
    raw = (raw + String(chunk)).slice(-400_000);
    buffer += String(chunk);
    const lines = buffer.split("\n"); buffer = lines.pop() ?? "";
    for (const line of lines) {
      try { const event = JSON.parse(line) as Json; if (typeof event.type === "string" && !String(event.type).endsWith("_delta") && event.type !== "message_update") events.push(event.type === "agent_end" ? { at, type: event.type, text: finalAssistantText(event) } : { at, type: event.type }); } catch { /* non-JSON */ }
    }
  });
  child.stderr!.on("data", (chunk: Buffer) => { raw = (raw + String(chunk)).slice(-400_000); });
  const stop = async () => {
    try { process.kill(-child.pid!, "SIGTERM"); } catch { return; }
    await sleep(1_500);
    try { process.kill(-child.pid!, "SIGKILL"); } catch { /* exited */ }
  };
  cleanup.push(stop, () => writeFileSync(join(dir, `pi-${label}.log`), raw));
  return { child, events, send: (command) => child.stdin!.write(`${JSON.stringify(command)}\n`), log: () => raw };
};

describe.skipIf(!LIVE)(`live Pi (${LIVE ? "enabled" : SKIP_REASON})`, () => {
  it("WK-1/DL-5, ID-5, WK-2, LT-3, LT-4 with a real Pi RPC session", async () => {
    const redisUrl = ownedRedisUrl();
    const dir = receiptDir("pi");
    receipt.receipt_dir = dir;
    cleanup.push(() => writeReceipt(dir, receipt));
    const work = join(dir, "work"); mkdirSync(work, { recursive: true });
    const redis = redisFor(redisUrl); cleanup.push(() => { redis.disconnect(); });

    // ID-5: two Pi sessions in one directory, identified from the registry by working directory.
    const piA = startPi(dir, "a", redisUrl, work);
    const piB = startPi(dir, "b", redisUrl, work);
    const agents = await until(async () => {
      const rows = Object.entries(await redis.hgetall(SESSION_KEYS.registry)).flatMap(([name, raw]) => {
        try { const value = JSON.parse(raw) as { metadata?: Json }; return resolve(String(value.metadata?.working_directory ?? "")) === resolve(work) ? [{ name, pid: value.metadata?.pid, metadata: value.metadata }] : []; } catch { return []; }
      });
      return rows.length >= 2 ? rows : undefined;
    }, 60_000, "two Pi registrations");
    receipt.registrations = agents;
    criteria["ID-5"] = { outcome: new Set(agents.map((a) => a.name)).size === agents.length ? "pass" : "fail", cwd: work, agents: agents.map((a) => a.name) };
    expect(new Set(agents.map((a) => a.name)).size).toBe(agents.length);

    // Map registration -> process: the bound runtime proves which Pi owns which mailbox.
    await sleep(3_000);
    const sender = await startLiveSender("pi"); cleanup.push(() => sender.close());
    receipt.sender = sender.name;
    // Pick agent A by waking whichever registration Pi A owns: probe with the first and check events.
    const task1 = liveTask();
    const target = agents[0]!.name;
    const sent1 = await sender.send(target, task1.content);
    const started1 = await until(() => [piA, piB].map((p, i) => ({ i, e: p.events.find((e) => e.type === "agent_start" && e.at >= sent1.sent_at) })).find((x) => x.e),
      TURN_TIMEOUT_MS, "agent_start after send");
    const pi = started1.i === 0 ? piA : piB;
    receipt.recipient = { agent: target, process: started1.i === 0 ? "a" : "b" };
    receipt.model_runs = Number(receipt.model_runs) + 1;
    await until(() => pi.events.find((e) => e.type === "agent_settled" && e.at >= started1.e!.at) ?? pi.events.find((e) => e.type === "agent_end" && e.at >= started1.e!.at), TURN_TIMEOUT_MS, "agent_end");
    const reply1 = await awaitReply(sender, sent1.message_id, 60_000).catch(() => undefined);
    const trace1 = await consumption(redis, target, sent1.message_id);
    const verdict1 = exchangeVerdict({ sender: sender.name, recipient: target, requestId: sent1.message_id, reply: reply1,
      claim: trace1.claim, acked: trace1.ack !== undefined, expected: task1.expected });
    criteria["WK-1"] = { outcome: verdict1.outcome === "meets" ? "pass" : "fail", request_id: sent1.message_id, reply_id: reply1?.message.id,
      reply_content: reply1?.message.payload.content, claim_id: trace1.claim?.claim_id, verdict_reasons: verdict1.reasons };
    criteria["DL-5"] = { outcome: verdict1.outcome === "meets" ? "pass" : "fail", verdict: verdict1 };
    criteria["LT-3"] = { send_to_agent_start_ms: started1.e!.at - sent1.sent_at, target_p95_ms: 300, samples: 1 };
    receipt.trace_wk1 = trace1.rows;
    expect(verdict1.reasons).toEqual([]);
    await sleep(2_000);

    // WK-2 / LT-4: a task arriving mid-run is handled in the next turn.
    const mark = pi.events.length;
    pi.send({ type: "prompt", message: "Do not call any tools. Write the integers from 1 to 300 separated by single spaces, then stop." });
    receipt.model_runs = Number(receipt.model_runs) + 1;
    const busyStart = await until(() => pi.events.slice(mark).find((e) => e.type === "turn_start"), 30_000, "busy turn_start");
    const task2 = liveTask();
    const sent2 = await sender.send(target, task2.content);
    const reply2 = await awaitReply(sender, sent2.message_id).catch(() => undefined);
    await until(() => pi.events.slice(mark).find((e) => e.type === "agent_settled"), 60_000, "agent_settled").catch(() => undefined);
    const trace2 = await consumption(redis, target, sent2.message_id);
    const claimedAt = trace2.claim ? Number(String(trace2.claim.stream_id).split("-")[0]) : Number.NaN;
    const after = pi.events.slice(mark);
    // The busy run ends at its own agent_end (Pi runs a queued follow-up as a new run after it,
    // docs/extensions.md); steering would claim inside the busy run, before this agent_end.
    const busyEnd = after.find((e) => e.type === "agent_end" && e.at >= busyStart.at);
    const busyCompleted = /(?:^|\D)300\W*$/u.test(busyEnd?.text?.trim() ?? "");
    const nextStart = busyEnd && after.filter((e) => e.type === "turn_start" && e.at >= busyEnd.at && e.at <= claimedAt).at(-1);
    const verdict2 = exchangeVerdict({ sender: sender.name, recipient: target, requestId: sent2.message_id, reply: reply2,
      claim: trace2.claim, acked: trace2.ack !== undefined, expected: task2.expected });
    const midTurn = sent2.sent_at >= busyStart.at && busyEnd !== undefined && sent2.sent_at < busyEnd.at;
    const claimedAfterBusyRun = busyEnd !== undefined && claimedAt > busyEnd.at;
    const wk2 = midTurn && busyCompleted && claimedAfterBusyRun && nextStart !== undefined && verdict2.outcome === "meets";
    criteria["WK-2"] = { outcome: wk2 ? "pass" : "fail", request_id: sent2.message_id, sent_mid_turn: midTurn, busy_output_completed: busyCompleted,
      claimed_after_busy_run: claimedAfterBusyRun, busy_turn_start: busyStart.at,
      busy_turn_end: busyEnd?.at, next_turn_start: nextStart?.at, claim_stream_id: trace2.claim?.stream_id, verdict: verdict2 };
    criteria["LT-4"] = { turn_end_to_next_turn_start_ms: nextStart && busyEnd ? nextStart.at - busyEnd.at : null, target_p95_ms: 300, samples: 1 };
    receipt.events_after_busy = after.map((e) => [e.type, Math.round(e.at)]);
    receipt.trace_wk2 = trace2.rows;
    receipt.trace_all = await readTrace(redis, target);
    expect(verdict2.reasons).toEqual([]);
    expect(wk2).toBe(true);
  }, 15 * 60_000);
});
