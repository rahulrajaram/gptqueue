import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { describe, expect, it } from "vitest";
import { collectNativeExchange, extractNativeTraces } from "./qualification-evidence.js";
import { checkExchangeEvidence } from "./oracle.js";
import type { ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";

const enabled = process.env.GPTQUEUE_RETAINED_CROSS_ADJUDICATION === "1";
const retained = resolve(".gptqueue/repair-qualification/20260912/qualification-cross-model/8a903b8f-0638-46d6-b44a-269dfccad35a");
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
type RecordedActor = Readonly<{ participant_id: string; route: ParticipantIdentity["route"]; host_runtime_id: string; agent: string; cwd_hash: string; profile_hash: string; epoch_hash: string }>;
type RetainedReceipt = Readonly<{
  run_id: string; started_at: string; ended_at: string; execution: unknown;
  source_hashes: Record<string, string>; source_hashes_after: Record<string, string>;
  cleanup: readonly Readonly<{ name: string; status: string }>[];
  phases: readonly Readonly<{ label: string; value: { sender: RecordedActor; receiver: RecordedActor } }>[];
}>;
const actor = (row: RecordedActor): ParticipantIdentity => ({
  participantId: row.participant_id, route: row.route, hostRuntimeId: row.host_runtime_id,
  agent: row.agent, cwdHash: row.cwd_hash, profileHash: row.profile_hash, epochHash: row.epoch_hash,
});

describe.skipIf(!enabled)("retained cross-model generated adjudication", () => {
  it("recomputes the verdict from immutable histories and actual recorded identities", async () => {
    const receiptText = await readFile(join(retained, "receipt-sanitized.json"), "utf8");
    const receipt = JSON.parse(receiptText) as RetainedReceipt;
    expect(receipt.source_hashes).toEqual(receipt.source_hashes_after);
    expect(receipt.cleanup.map(({ name }) => name)).toEqual(["sender.close", "receiver.close", "codex-factory.close", "pi-factory.close", "redis.close", "workspace.remove"]);
    expect(receipt.cleanup.every(({ status }) => status === "fulfilled")).toBe(true);
    const launch = receipt.phases.find(({ label }) => label === "launched")?.value;
    if (!launch) throw new Error("retained receipt lacks native launch identities");
    const sender = actor(launch.sender);
    const receiver = actor(launch.receiver);
    expect(sender.route).toBe("codex-appserver");
    expect(receiver.route).toBe("pi-rpc-cli");
    const oracleFiles = ["tests/acceptance/qualification-evidence.ts", "tests/acceptance/oracle.ts", "tests/acceptance/qualification-retained-cross.test.ts"];
    const oracleHashes = Object.fromEntries(await Promise.all(oracleFiles.map(async (path) => [path, hash(await readFile(path, "utf8"))])));
    const oracleRevision = hash(JSON.stringify(oracleHashes));
    const sourceRevision = hash(JSON.stringify(receipt.source_hashes));
    const histories = await Promise.all(["sender", "receiver"].map(async (side) => {
      const path = join(retained, `history-${side}-failure-sanitized.json`);
      const text = await readFile(path, "utf8");
      return { history: JSON.parse(text) as unknown, raw: { path, sha256: hash(text), sourceRevision, oracleRevision } satisfies RawEvidenceRef };
    }));
    const senderHistory = histories[0]!;
    const receiverHistory = histories[1]!;
    const traces = {
      sender: extractNativeTraces(senderHistory.history, sender, senderHistory.raw),
      receiver: extractNativeTraces(receiverHistory.history, receiver, receiverHistory.raw),
    };
    const nonce = `cross-${receipt.run_id}`;
    const collected = collectNativeExchange({
      sender: { actor: sender, traces: traces.sender }, receiver: { actor: receiver, traces: traces.receiver },
      nonce, requestContent: `qualification ${nonce}: calculate 17+25`, expectedReplyContent: `answer ${nonce}: ${17 + 25}`,
    });
    const verdict = checkExchangeEvidence(collected.evidence);
    expect(verdict.outcome).toBe("meets");
    expect(collected.evidence.request?.id).toBe("786a39a3-2b42-4d5e-8c49-d8dcc0baf679");
    expect(collected.evidence.reply?.id).toBe("c32adcbd-9856-4b16-b0a6-16026f35c1ee");
    expect(traces.sender.some((trace) => trace.successful === false)).toBe(true);
    const after = Object.fromEntries(await Promise.all(oracleFiles.map(async (path) => [path, hash(await readFile(path, "utf8"))])));
    expect(after).toEqual(oracleHashes);
    const dir = resolve(".gptqueue/repair-qualification/20260912/cross-adjudications", randomUUID());
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "adjudication.json"), `${JSON.stringify({
      kind: "offline-generated-adjudication", generatedAt: new Date().toISOString(),
      originalReceipt: { path: join(retained, "receipt-sanitized.json"), sha256: hash(receiptText), execution: receipt.execution, startedAt: receipt.started_at, endedAt: receipt.ended_at },
      oracleDecision: "Successful recovered chains qualify; failed calls remain visible and cannot satisfy any join. Automatic activation and initiative are outside this communication verdict.",
      oracleHashes, sourceHashes: receipt.source_hashes, sender, receiver, cleanup: receipt.cleanup,
      raw: histories.map(({ raw }) => raw), evidence: collected.evidence, verdict,
      failedCalls: traces.sender.filter((trace) => trace.successful === false),
      nativeExecutionPerformed: false,
    }, null, 2)}\n`);
    console.log(`Generated adjudication: ${dir}`);
  });
});
