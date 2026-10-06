/**
 * Shared helpers for the live acceptance tests (real runtime CLIs, private Redis).
 * See README.md. Nothing here touches the shared Redis or user runtime config.
 */
import { randomInt, randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { checkExchangeEvidence, type ExchangeVerdict } from "../acceptance/oracle.js";

export const LIVE = process.env.GPTQUEUE_LIVE_ACCEPTANCE === "1";
export const SKIP_REASON = "live acceptance needs GPTQUEUE_LIVE_ACCEPTANCE=1, real runtime CLIs and their existing auth";
export const RECEIPT_ROOT = process.env.GPTQUEUE_LIVE_RECEIPT_DIR ??
  join(tmpdir(), "gptqueue-live-receipts");
export const TURN_TIMEOUT_MS = 180_000;

/** Fail closed: only a private Redis (scripts/with-owned-redis.mjs) is acceptable, never the shared 6379. */
export const ownedRedisUrl = (): string => {
  const raw = process.env.REDIS_URL;
  if (!raw) throw new Error("REDIS_URL unset: run through node scripts/with-owned-redis.mjs");
  const url = new URL(raw);
  if (url.port === "" || url.port === "6379" || url.pathname !== "/15" || !["127.0.0.1", "localhost"].includes(url.hostname)) {
    throw new Error(`refusing non-private Redis ${url.host}${url.pathname}`);
  }
  return raw;
};

export const now = (): number => performance.timeOrigin + performance.now();
export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
export const until = async <T>(probe: () => Promise<T | undefined> | T | undefined, timeoutMs: number, label: string): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for ${label}`);
    await sleep(25);
  }
};

/** A task with an independently computable answer; the expected reply content is the decimal sum. */
export type LiveTask = Readonly<{ content: string; expected: string; nonce: string }>;
export const liveTask = (): LiveTask => {
  const a = randomInt(1000, 9000), b = randomInt(1000, 9000), nonce = randomUUID().slice(0, 8);
  return Object.freeze({
    nonce, expected: String(a + b),
    content: `Task ${nonce}: compute ${a} + ${b}. Reply with send_message to this task's sender, type "result", ` +
      `in_reply_to set to this task's id, and content exactly the decimal sum (digits only, nothing else). Then acknowledge the claim.`,
  });
};

/** Per-run receipt directory outside version control. */
export const receiptDir = (runtime: string): string => {
  const dir = join(RECEIPT_ROOT, `${runtime}-${new Date().toISOString().replace(/[:.]/gu, "-")}`);
  mkdirSync(dir, { recursive: true });
  return dir;
};
export const writeReceipt = (dir: string, receipt: unknown): void =>
  writeFileSync(join(dir, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });

/** Environment that keeps gptqueue logs/state for spawned sidecars inside the receipt dir. */
export const isolatedGptqueueEnv = (dir: string): Readonly<Record<string, string>> => Object.freeze({
  GPTQ_LOG_DIR: join(dir, "gptq-logs"), XDG_STATE_HOME: join(dir, "xdg-state"),
  GPTQ_CLAUDE_NAMES_FILE: join(dir, "claude-agent-names.tsv"),
});

export type TraceEntry = Readonly<Record<string, string>>;
export const readTrace = async (redis: Redis, agent: string): Promise<readonly TraceEntry[]> =>
  (await redis.xrange(SESSION_KEYS.inboxTrace(agent), "-", "+")).map(([id, fields]) => {
    const out: Record<string, string> = { stream_id: id };
    for (let i = 0; i < fields.length; i += 2) out[fields[i]!] = fields[i + 1]!;
    return Object.freeze(out);
  });

type Sent = Readonly<{ message_id: string; sent_at: number }>;
type Received = Readonly<{ message: { id: string; from: string; to: string; payload: { content: string; in_reply_to?: string } }; claim_id: string }>;
export type LiveSender = Readonly<{
  name: string; send(to: string, content: string): Promise<Sent>; drain(): Promise<readonly Received[]>;
  received: Received[]; close(): Promise<void>;
}>;

/** The e2e harness sender (real MCP tool layer); imported after REDIS_URL is validated. */
export const startLiveSender = async (label: string): Promise<LiveSender> => {
  ownedRedisUrl();
  const harness = await import("../acceptance-e2e/harness.js");
  const sender = await harness.startSender(`live-${label}`);
  return sender as unknown as LiveSender;
};

/** Wait for a correlated reply, draining (claim + ack) the sender's inbox. */
export const awaitReply = (sender: LiveSender, requestId: string, timeoutMs = TURN_TIMEOUT_MS): Promise<Received> =>
  until(async () => {
    await sender.drain();
    return sender.received.find(({ message }) => message.payload.in_reply_to === requestId);
  }, timeoutMs, `reply to ${requestId}`);

/** Claim and ack trace rows for one message in the recipient's inbox trace. */
export const consumption = async (redis: Redis, agent: string, messageId: string, timeoutMs = 15_000) =>
  until(async () => {
    const rows = await readTrace(redis, agent);
    const claim = rows.find((row) => row.stage === "task_claimed" && row.message_id === messageId);
    const ack = claim && rows.find((row) => row.stage === "task_acknowledged" && row.claim_id === claim.claim_id);
    return claim && ack ? { claim, ack, rows } : undefined;
  }, timeoutMs, `claim+ack trace for ${messageId}`).catch(async () => {
    const rows = await readTrace(redis, agent);
    return { claim: rows.find((row) => row.stage === "task_claimed" && row.message_id === messageId), ack: undefined, rows };
  });

/** The exact-exchange oracle over live evidence (DL-5). */
export const exchangeVerdict = (input: Readonly<{
  sender: string; recipient: string; requestId: string; reply?: Received; claim?: TraceEntry; acked: boolean; expected: string;
}>): ExchangeVerdict => checkExchangeEvidence({
  sender: input.sender, recipient: input.recipient, expected_reply_content: input.expected, execution: { status: "completed" },
  request: { id: input.requestId, from: input.sender, to: input.recipient },
  reply: input.reply ? { id: input.reply.message.id, from: input.reply.message.from, to: input.reply.message.to,
    in_reply_to: input.reply.message.payload.in_reply_to, content: input.reply.message.payload.content } : undefined,
  request_consumption: input.claim ? { message_id: input.claim.message_id!, actor: input.recipient, consumed: true,
    claim_id: input.claim.claim_id, acknowledged: input.acked } : undefined,
  reply_consumption: input.reply ? { message_id: input.reply.message.id, actor: input.sender, consumed: true,
    claim_id: input.reply.claim_id, acknowledged: true } : undefined,
});

export const redisFor = (url: string): Redis => new Redis(url, { maxRetriesPerRequest: 3 });
