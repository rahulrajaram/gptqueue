import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { ModelParticipant, Participant } from "./qualification-types.js";
import { createPiSdkAdapter } from "./qualification-pi-sdk.js";
import { hasPiAssistantRuntimeEvidence } from "./qualification-pi.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_PI_SDK === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-pi-sdk");
const sourceFiles = [
  "tests/acceptance/qualification-pi.ts", "tests/acceptance/qualification-pi-sdk.ts", "tests/acceptance/qualification-pi-sdk-conformance.test.ts",
  "tests/acceptance/qualification-types.ts", "tests/acceptance/owned-redis.ts", "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js",
] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });

type Json = Record<string, unknown>;
type Cleanup = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(join(repo, path))).digest("hex");
const redactUrl = (url: string): Json => {
  const parsed = new URL(url);
  return { protocol: parsed.protocol, hostname: parsed.hostname, port: parsed.port, database: parsed.pathname.slice(1) };
};
const identity = (participant: Participant): Json => ({
  participant_id: participant.identity.participantId, route: participant.identity.route, host_runtime_id: participant.identity.hostRuntimeId,
  agent: participant.identity.agent, cwd_hash: participant.identity.cwdHash, profile_hash: participant.identity.profileHash,
  epoch_hash: participant.identity.epochHash,
});

describe.skipIf(!enabled)("Pi SDK qualification owned pair", () => {
  it("keeps two actual SDK sessions distinct and records native histories", async () => {
    const provider = process.env.GPTQUEUE_PI_PROVIDER;
    const model = process.env.GPTQUEUE_PI_MODEL;
    if (!provider || !model) throw new Error("GPTQUEUE_PI_PROVIDER and GPTQUEUE_PI_MODEL are required for the gated Pi SDK run");
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = {
      schema_version: 1, run_id: runId, route: "pi-sdk", execution: { status: "running" }, passed: false,
      started_at: new Date().toISOString(), provider, model, source_hashes: {}, identities: {}, phases: [], cleanup: [],
    };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let sender: Participant | undefined;
    let receiver: Participant | undefined;
    let failure: unknown;
    try {
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      await persist("source-hashes", receipt.source_hashes);
      redis = await startOwnedRedis();
      receipt.redis = redactUrl(redis.url);
      workspace = await mkdtemp(join(artifactDir, "workspace-"));
      const adapter = createPiSdkAdapter({ workspaceRoot: workspace, provider, model });
      const signal = AbortSignal.timeout(420_000);
      const availability = await adapter.preflight(signal);
      await persist("preflight", availability);
      expect(availability.kind).toBe("available");
      sender = await adapter.launch({ role: "sender", pairId: "pi-sdk-conformance", nonce: `${runId}-sender`, redisUrl: redis.url }, signal);
      receiver = await adapter.launch({ role: "receiver", pairId: "pi-sdk-conformance", nonce: `${runId}-receiver`, redisUrl: redis.url }, signal);
      if (sender.kind !== "model" || receiver.kind !== "model") throw new Error("Pi SDK route did not launch model participants");
      const senderModel: ModelParticipant = sender;
      const receiverModel: ModelParticipant = receiver;
      receipt.identities = { sender: identity(sender), receiver: identity(receiver) };
      await persist("launched", receipt.identities);
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      const initial = await Promise.all([senderModel.status(signal), receiverModel.status(signal)]);
      await persist("initial-status", initial);
      expect(initial).toEqual([{ kind: "idle", runtimeId: sender.identity.hostRuntimeId }, { kind: "idle", runtimeId: receiver.identity.hostRuntimeId }]);
      await Promise.all([
        senderModel.prompt("Call get_runtime_status, then reply PI_SDK_SENDER_READY.", signal),
        receiverModel.prompt("Call get_runtime_status, then reply PI_SDK_RECEIVER_READY.", signal),
      ]);
      const [senderHistory, receiverHistory] = await Promise.all([senderModel.history(signal), receiverModel.history(signal)]);
      await writeFile(join(artifactDir, "history-sender-sanitized.json"), `${JSON.stringify(sanitizeEvidence(senderHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "history-receiver-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receiverHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await persist("history", { sender: senderHistory, receiver: receiverHistory });
      expect(hasPiAssistantRuntimeEvidence(senderHistory, { marker: "PI_SDK_SENDER_READY", agent: sender.identity.agent, runtimeId: sender.identity.hostRuntimeId })).toBe(true);
      expect(hasPiAssistantRuntimeEvidence(receiverHistory, { marker: "PI_SDK_RECEIVER_READY", agent: receiver.identity.agent, runtimeId: receiver.identity.hostRuntimeId })).toBe(true);
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
      const failureSignal = AbortSignal.timeout(10_000);
      const histories = await Promise.all([
        sender?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
        receiver?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
      ]);
      if (histories[0] !== undefined) await writeFile(join(artifactDir, "history-sender-failure-sanitized.json"), `${JSON.stringify(sanitizeEvidence(histories[0], { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 }).catch(() => undefined);
      if (histories[1] !== undefined) await writeFile(join(artifactDir, "history-receiver-failure-sanitized.json"), `${JSON.stringify(sanitizeEvidence(histories[1], { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 }).catch(() => undefined);
      await persist("failure", { error: String(error) }).catch(() => undefined);
    } finally {
      const cleanup: Cleanup[] = [];
      const actions: readonly Readonly<{ name: string; run: () => Promise<void> }>[] = [
        { name: "sender.close", run: async () => { await sender?.close(); } },
        { name: "receiver.close", run: async () => { await receiver?.close(); } },
        { name: "redis.close", run: async () => { await redis?.close(); } },
        { name: "workspace.remove", run: async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); } },
      ];
      for (const action of actions) {
        try { await action.run(); cleanup.push({ name: action.name, status: "fulfilled" }); }
        catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      const sourceHashesAfter = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      receipt.source_hashes_after = sourceHashesAfter;
      receipt.source_hashes_match = JSON.stringify(receipt.source_hashes) === JSON.stringify(sourceHashesAfter);
      if (receipt.source_hashes_match !== true || cleanup.some(result => result.status === "rejected")) {
        receipt.passed = false;
        receipt.execution = { status: "failed", detail: "Pi SDK source or cleanup verification failed" };
        failure ??= new Error("Pi SDK source or cleanup verification failed");
      }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
