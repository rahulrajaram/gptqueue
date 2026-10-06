/**
 * LT-0..LT-4 latency driver, outside the agent loop (the fake model answers instantly).
 *
 * Hops per idle iteration (ms):
 *   send_to_persisted      LT-1  send_message call -> success (reported only after durable store)
 *   persisted_to_notified  LT-2  -> dispatcher activation_requested trace (Claude: watcher exit)
 *   send_to_injected       LT-3  -> native host receipt of the turn (turn/start, sendMessage, promptAsync; Claude: watcher exit)
 * Per busy iteration:
 *   turn_end_to_next_turn  LT-4  busy turn ends -> next model turn starts (Claude: the turn the
 *                                busy turn's Stop-hook block started; a watcher wake fails the run)
 *
 * Env: GPTQUEUE_LATENCY_ITERATIONS (default 50), GPTQUEUE_LATENCY_OUT (merge JSON results
 * there; default: no file), GPTQUEUE_LATENCY_ASSERT=1 (enforce provisional targets).
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { expect } from "vitest";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import { percentile, readTrace, REDIS_URL, sleep, startRecipient, startSender, taskContent, waitFor, type Recipient, type Runtime, type Sender } from "./harness.js";

export const ITERATIONS = Number(process.env.GPTQUEUE_LATENCY_ITERATIONS ?? 50);
export const ASSERT = process.env.GPTQUEUE_LATENCY_ASSERT === "1";

type Summary = Readonly<{ n: number; p50: number; p95: number; p99: number; max: number }>;
const summarize = (values: readonly number[]): Summary => {
  const round = (v: number) => Math.round(v * 10) / 10;
  return { n: values.length, p50: round(percentile(values, 50)), p95: round(percentile(values, 95)), p99: round(percentile(values, 99)), max: round(Math.max(...values)) };
};

export type LatencyResult = Readonly<{ runtime: Runtime; iterations: number; measured_at: string; hops: Readonly<Record<string, Summary>> }>;

const awaitReply = async (sender: Sender, requestId: string) => {
  await waitFor(async () => { await sender.drain(); return sender.received.some(({ message }) => message.payload.in_reply_to === requestId); }, 15_000, `reply to ${requestId}`);
};
const firstAfter = (times: readonly number[], from: number, since: number): number | undefined => times.slice(since).find((t) => t >= from);

export const measureRuntime = async (runtime: Runtime): Promise<LatencyResult> => {
  const sender = await startSender(`lt-${runtime}`);
  const recipient: Recipient = await startRecipient(runtime);
  const observer = new RedisClient(null, REDIS_URL);
  const hops: Record<string, number[]> = { send_to_persisted: [], persisted_to_notified: [], send_to_injected: [], notified_to_injected: [], turn_end_to_next_turn: [] };
  try {
    const settle = () => waitFor(() => !recipient.turnRunning(), 5_000, "model turn to end");
    for (let i = 0; i < ITERATIONS; i += 1) {
      await settle();
      const injectionsBefore = recipient.injections.length;
      const sent = await sender.send(recipient.name(), taskContent(i, 7));
      await awaitReply(sender, sent.message_id);
      const injected = firstAfter(recipient.injections, sent.sent_at, injectionsBefore);
      if (injected === undefined) throw new Error(`${runtime}: no injection recorded for iteration ${i}`);
      let notified = injected;
      if (runtime !== "claude") {
        const requested = (await readTrace(observer, recipient.name())).find((e) => e.stage === "activation_requested" && e.message_id === sent.message_id);
        if (!requested) throw new Error(`${runtime}: no activation_requested trace for ${sent.message_id} (LT-0)`);
        notified = Date.parse(requested.timestamp!);
      }
      hops.send_to_persisted!.push(sent.persisted_at - sent.sent_at);
      hops.persisted_to_notified!.push(notified - sent.persisted_at);
      hops.send_to_injected!.push(injected - sent.sent_at);
      hops.notified_to_injected!.push(injected - notified);
    }
    for (let i = 0; i < ITERATIONS; i += 1) {
      await settle();
      recipient.hold();
      const turnsBefore = recipient.turnStarts.length;
      const injectionsBefore = recipient.injections.length;
      const sent = await sender.send(recipient.name(), taskContent(i, 9));
      // Let the busy outcome register: a deferral trace (Codex/OpenCode), a queued follow-up (Pi).
      if (runtime === "codex" || runtime === "opencode") {
        await waitFor(async () => (await readTrace(observer, recipient.name())).some((e) => e.stage === "activation_deferred" && e.message_id === sent.message_id), 5_000, "busy deferral")
          .catch(async (error: unknown) => {
            const tail = (await readTrace(observer, recipient.name())).slice(-8).map((e) => `${e.timestamp} ${e.stage} ${e.code ?? ""} ${e.message_id === sent.message_id ? "THIS" : e.message_id ?? ""}`);
            throw new Error(`${String(error)} in busy iteration ${i}; trace tail:\n${tail.join("\n")}`);
          });
      } else if (runtime === "pi") {
        await waitFor(() => recipient.injections.some((t) => t >= sent.sent_at), 5_000, "follow-up queued");
      } else {
        await sleep(10);
      }
      const ended = await recipient.release();
      await awaitReply(sender, sent.message_id);
      const next = firstAfter(recipient.turnStarts, ended, turnsBefore);
      if (next === undefined) throw new Error(`${runtime}: no turn after busy end in iteration ${i}`);
      if (runtime === "claude" && recipient.injectionVia!.slice(injectionsBefore)[0] !== "stop_hook") {
        throw new Error(`claude: busy iteration ${i} continued via ${recipient.injectionVia!.slice(injectionsBefore)[0] ?? "nothing"}, not a Stop-hook block`);
      }
      hops.turn_end_to_next_turn!.push(next - ended);
    }
  } finally {
    await recipient.close().catch(() => undefined);
    await sender.close().catch(() => undefined);
    await observer.shutdown().catch(() => undefined);
  }
  const result: LatencyResult = { runtime, iterations: ITERATIONS, measured_at: new Date().toISOString(),
    hops: Object.fromEntries(Object.entries(hops).map(([hop, values]) => [hop, summarize(values)])) };
  report(result);
  return result;
};

const report = (result: LatencyResult): void => {
  const lines = Object.entries(result.hops).map(([hop, s]) => `  ${hop.padEnd(24)} n=${s.n} p50=${s.p50} p95=${s.p95} p99=${s.p99} max=${s.max} ms`);
  process.stdout.write(`[gptqueue latency] ${result.runtime} (${result.iterations} idle + ${result.iterations} busy iterations)\n${lines.join("\n")}\n`);
  const out = process.env.GPTQUEUE_LATENCY_OUT;
  if (!out) return;
  let merged: Record<string, unknown> = {};
  try { if (existsSync(out)) merged = JSON.parse(readFileSync(out, "utf8")) as Record<string, unknown>; } catch { merged = {}; }
  merged[result.runtime] = result;
  writeFileSync(out, `${JSON.stringify(merged, null, 2)}\n`);
};

/** Provisional targets from ACCEPTANCE-CRITERIA (LT-2, LT-3, LT-4). Enforced only with GPTQUEUE_LATENCY_ASSERT=1. */
export const assertTargets = (result: LatencyResult): void => {
  if (!ASSERT) return;
  const h = result.hops;
  if (result.runtime === "claude") {
    expect(h.send_to_injected!.p95, "LT-3 Claude watcher p95 <= 1 s").toBeLessThanOrEqual(1_000);
  } else {
    expect(h.persisted_to_notified!.p95, "LT-2 p95 <= 50 ms").toBeLessThanOrEqual(50);
    expect(h.send_to_injected!.p95, "LT-3 p95 <= 300 ms").toBeLessThanOrEqual(300);
  }
  expect(h.turn_end_to_next_turn!.p95, "LT-4 p95 <= 300 ms").toBeLessThanOrEqual(300);
};
