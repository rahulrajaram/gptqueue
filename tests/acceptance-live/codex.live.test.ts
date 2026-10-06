/**
 * Live Codex acceptance: WK-1/DL-5, WK-2, ID-5, LT-3, LT-4 with the real `codex` CLI.
 * A private `codex app-server --listen unix://<receipt>/app.sock` hosts two threads; each
 * thread starts this worktree's sidecar (bin/gptqueue-session --client codex) against the
 * private Redis, and the sidecar binds itself through GPTQUEUE_CODEX_APP_SERVER_SOCKET.
 * User hooks are disabled and every other user MCP server is disabled per invocation.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { CodexSocketClient } from "../../src/registered-shell/codex-socket.js";
import { appConfig } from "../acceptance/codex-support.js";
import { codexEffectiveConfig } from "../acceptance/qualification-codex.js";
import {
  LIVE, SKIP_REASON, TURN_TIMEOUT_MS, awaitReply, consumption, exchangeVerdict, isolatedGptqueueEnv, liveTask, now,
  ownedRedisUrl, readTrace, receiptDir, redisFor, sleep, startLiveSender, until, writeReceipt,
} from "./support.js";

type Json = Record<string, unknown>;
const MODEL = "gpt-6-luna";
const receipt: Json = { runtime: "codex", model: MODEL, criteria: {}, model_turns: 0 };
const criteria = receipt.criteria as Record<string, Json>;
let shared: { sender: Awaited<ReturnType<typeof startLiveSender>>; redis: ReturnType<typeof redisFor>; agentB: string; threadB: string } | undefined;
const events: Array<Readonly<{ at: number; method: string; threadId?: string; turnId?: string }>> = [];
const cleanup: Array<() => Promise<void> | void> = [];
afterAll(async () => { for (const step of cleanup.splice(0).reverse()) await Promise.resolve(step()).catch(() => undefined); });

const stop = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { process.kill(-child.pid!, "SIGTERM"); } catch { return; }
  await sleep(1_500);
  try { process.kill(-child.pid!, "SIGKILL"); } catch { /* exited */ }
};

describe.skipIf(!LIVE)(`live Codex (${LIVE ? "enabled" : SKIP_REASON})`, () => {
  it("WK-1/DL-5, ID-5, WK-2, LT-3, LT-4 with a real Codex app-server", async () => {
    const redisUrl = ownedRedisUrl();
    const dir = receiptDir("codex");
    receipt.receipt_dir = dir;
    const work = join(dir, "work"); mkdirSync(work, { recursive: true });
    const socket = join(dir, "app.sock");
    const env = isolatedGptqueueEnv(dir);
    const config: Json = {
      ...codexEffectiveConfig(await appConfig(), redisUrl, socket),
      ...Object.fromEntries(Object.entries(env).map(([key, value]) => [`mcp_servers.gptqueue-shared.env.${key}`, value])),
    };
    const overrides = Object.entries(config).flatMap(([key, value]) => ["-c", `${key}=${JSON.stringify(value)}`]);
    const server = spawn("codex", ["app-server", "--listen", `unix://${socket}`, "--disable", "hooks", ...overrides],
      { cwd: work, env: { ...process.env, ...env, REDIS_URL: redisUrl, GPTQUEUE_CODEX_APP_SERVER_SOCKET: socket }, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    let serverLog = "";
    server.stdout?.on("data", (c) => { serverLog = (serverLog + String(c)).slice(-200_000); });
    server.stderr?.on("data", (c) => { serverLog = (serverLog + String(c)).slice(-200_000); });
    cleanup.push(() => stop(server), () => writeFileSync(join(dir, "app-server.log"), serverLog));
    cleanup.push(() => writeReceipt(dir, receipt));
    await until(() => (existsSync(socket) ? true : server.exitCode !== null ? (() => { throw new Error(`app-server exited: ${serverLog.slice(-2000)}`); })() : undefined), 20_000, "app-server socket");

    const rpc = new CodexSocketClient(socket, 30_000);
    cleanup.push(() => rpc.close());
    rpc.onNotification(({ method, params }) => {
      if (!method.startsWith("turn/") && method !== "thread/status/changed") return;
      const turn = params.turn as Json | undefined;
      events.push({ at: now(), method, threadId: params.threadId as string | undefined, turnId: (turn?.id ?? params.turnId) as string | undefined });
    });
    cleanup.push(() => { receipt.notifications = events; });
    const start = async (): Promise<string> => {
      const result = await rpc.request("thread/start", { cwd: work, model: MODEL, approvalPolicy: "never", sandbox: "read-only",
        config, developerInstructions: "Use only the gptqueue-shared MCP tools. Do not use shell commands or files." }, AbortSignal.timeout(60_000));
      const id = ((result.thread ?? result) as Json).id;
      if (typeof id !== "string") throw new Error(`thread/start returned no id: ${JSON.stringify(result).slice(0, 500)}`);
      return id;
    };
    const status = async (threadId: string): Promise<Json> => {
      const r = await rpc.request("mcpServer/tool/call", { threadId, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {} }, AbortSignal.timeout(30_000));
      return (r.structuredContent ?? {}) as Json;
    };
    const bound = (threadId: string) => until(async () => {
      const s = await status(threadId).catch(() => undefined);
      return (s?.runtime as Json | null | undefined)?.runtime_id === threadId ? s : undefined;
    }, 60_000, `sidecar binding for ${threadId}`);

    // ID-5: two threads of the same runtime in one directory.
    const threadA = await start();
    const threadB = await start();
    const [statusA, statusB] = await Promise.all([bound(threadA), bound(threadB)]);
    const agentA = String(statusA.agent), agentB = String(statusB.agent);
    criteria["ID-5"] = { outcome: agentA !== agentB ? "pass" : "fail", cwd: work, threadA, threadB, agentA, agentB };
    expect(agentA).not.toBe(agentB);

    const redis = redisFor(redisUrl); cleanup.push(() => { redis.disconnect(); });
    // Diagnostics on every exit: the recipient's full inbox trace and the thread's native turns.
    cleanup.push(async () => {
      receipt.trace_agentA = await readTrace(redis, agentA).catch((e: unknown) => String(e));
      const thread = await rpc.request("thread/read", { threadId: threadA, includeTurns: true }, AbortSignal.timeout(15_000)).catch((e: unknown): Json => ({ error: String(e) }));
      const turns = ((thread.thread ?? thread) as Json).turns;
      receipt.native_turns_A = Array.isArray(turns) ? turns.map((t: Json) => ({ id: t.id, status: t.status, items: Array.isArray(t.items) ? t.items.length : undefined })) : thread;
    });
    const sender = await startLiveSender("codex"); cleanup.push(() => sender.close());
    receipt.sender = sender.name;
    const turnStarted = (after: number) => until(() => events.find((e) => e.method === "turn/started" && e.threadId === threadA && e.at >= after), TURN_TIMEOUT_MS, "turn/started");
    const turnCompleted = (turnId: string | undefined) => until(() => events.find((e) => e.method === "turn/completed" && e.threadId === threadA && (!turnId || e.turnId === turnId)), TURN_TIMEOUT_MS, "turn/completed");

    shared = { sender, redis, agentB, threadB };
    // Thread A gets one operator turn first so this test covers an already-materialized thread;
    // the never-prompted case is covered by thread B in the regression guard below.
    const warm = await rpc.request("turn/start", { threadId: threadA, input: [{ type: "text", text: "Reply with the single word READY. Do not call tools." }] }, AbortSignal.timeout(30_000));
    receipt.model_turns = Number(receipt.model_turns) + 1;
    await turnCompleted((warm.turn as Json | undefined)?.id as string | undefined);
    await sleep(2_000);

    // WK-1 / DL-5 / LT-3: idle wake with no human input.
    const task1 = liveTask();
    const sent1 = await sender.send(agentA, task1.content);
    const wake1 = await turnStarted(sent1.sent_at); receipt.model_turns = Number(receipt.model_turns) + 1;
    await turnCompleted(wake1.turnId);
    const reply1 = await awaitReply(sender, sent1.message_id).catch(() => undefined);
    const trace1 = await consumption(redis, agentA, sent1.message_id);
    const verdict1 = exchangeVerdict({ sender: sender.name, recipient: agentA, requestId: sent1.message_id, reply: reply1,
      claim: trace1.claim, acked: trace1.ack !== undefined, expected: task1.expected });
    criteria["WK-1"] = { outcome: verdict1.outcome === "meets" ? "pass" : "fail", request_id: sent1.message_id, turn_id: wake1.turnId,
      reply_id: reply1?.message.id, reply_content: reply1?.message.payload.content, claim_id: trace1.claim?.claim_id };
    criteria["DL-5"] = { outcome: verdict1.outcome === "meets" ? "pass" : "fail", verdict: verdict1 };
    criteria["LT-3"] = { send_to_turn_started_ms: wake1.at - sent1.sent_at, target_p95_ms: 300, samples: 1 };
    receipt.trace_wk1 = trace1.rows;
    expect(verdict1.reasons).toEqual([]);

    // WK-2 / LT-4: a task arriving mid-turn is handled in the next turn.
    const before = now();
    const busy = await rpc.request("turn/start", { threadId: threadA, input: [{ type: "text",
      text: "Do not call any tools. Write the integers from 1 to 400 separated by single spaces, then stop." }] }, AbortSignal.timeout(30_000));
    receipt.model_turns = Number(receipt.model_turns) + 1;
    const busyTurnId = (busy.turn as Json | undefined)?.id as string | undefined;
    await turnStarted(before);
    const task2 = liveTask();
    const sent2 = await sender.send(agentA, task2.content);
    const busyEnd = await turnCompleted(busyTurnId);
    const handledDuringBusy = (await consumption(redis, agentA, sent2.message_id, 1)).claim;
    const wake2 = await turnStarted(busyEnd.at); receipt.model_turns = Number(receipt.model_turns) + 1;
    await turnCompleted(wake2.turnId);
    const reply2 = await awaitReply(sender, sent2.message_id).catch(() => undefined);
    const trace2 = await consumption(redis, agentA, sent2.message_id);
    const claimedAt = trace2.claim ? Number(String(trace2.claim.stream_id).split("-")[0]) : Number.NaN;
    const verdict2 = exchangeVerdict({ sender: sender.name, recipient: agentA, requestId: sent2.message_id, reply: reply2,
      claim: trace2.claim, acked: trace2.ack !== undefined, expected: task2.expected });
    const wk2 = sent2.sent_at < busyEnd.at && handledDuringBusy === undefined && claimedAt >= Math.floor(busyEnd.at) - 1 && verdict2.outcome === "meets";
    criteria["WK-2"] = { outcome: wk2 ? "pass" : "fail", busy_turn_id: busyTurnId, request_id: sent2.message_id, sent_before_busy_end: sent2.sent_at < busyEnd.at,
      claimed_during_busy: handledDuringBusy !== undefined, next_turn_id: wake2.turnId, claim_stream_id: trace2.claim?.stream_id, verdict: verdict2 };
    criteria["LT-4"] = { turn_end_to_next_turn_started_ms: wake2.at - busyEnd.at, target_p95_ms: 300, samples: 1 };
    receipt.trace_wk2 = trace2.rows;
    expect(verdict2.reasons).toEqual([]);
    expect(wk2).toBe(true);
  }, 15 * 60_000);

  // Regression guard (WK-1, before the first prompt): real Codex rejects thread/turns/list with
  // -32600 "thread ... is not materialized yet; thread/turns/list is unavailable before first user
  // message". Before abf9432, lookupRecent rethrew that, so a bound but never-prompted thread was
  // never woken; it now treats that exact rejection as an empty history and starts the turn.
  it("WK-1: an idle, bound thread that has never been prompted is woken", async () => {
    if (!shared) throw new Error("setup test did not reach ID-5");
    const { sender, redis, agentB, threadB } = shared;
    const task = liveTask();
    const sent = await sender.send(agentB, task.content);
    const started = await until(() => events.find((e) => e.method === "turn/started" && e.threadId === threadB), 30_000, "turn/started on thread B")
      .catch((error: unknown) => { throw error; })
      .finally(async () => {
        const rows = await readTrace(redis, agentB);
        criteria["WK-1-before-first-prompt"] = { outcome: events.some((e) => e.threadId === threadB && e.method === "turn/started") ? "pass" : "fail",
          regression_guard: "abf9432 (never-materialized threads)", request_id: sent.message_id,
          trace_stages: rows.map((r) => [r.stage, r.code ?? ""].join(":")) };
        if (rows.some((r) => r.stage === "task_claimed")) receipt.model_turns = Number(receipt.model_turns) + 1;
      });
    expect(started.threadId).toBe(threadB);
  }, 60_000);
});
