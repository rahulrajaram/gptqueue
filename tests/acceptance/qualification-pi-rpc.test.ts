import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it, vi } from "vitest";
import { createPiAdapters, hasPiAssistantRuntimeEvidence, type PiRpcParticipant } from "./qualification-pi.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_PI_RPC === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-pi-rpc");
const sourceFiles = [
  "tests/acceptance/qualification-pi.ts", "tests/acceptance/qualification-pi-rpc.test.ts",
  "tests/acceptance/qualification-types.ts", "tests/acceptance/owned-redis.ts",
  "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js",
] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });

type Json = Record<string, unknown>;
type CleanupResult = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const redactUrl = (url: string): Json => {
  const parsed = new URL(url);
  return { protocol: parsed.protocol, hostname: parsed.hostname, port: parsed.port, database: parsed.pathname.slice(1) };
};
const sessionIdentity = (participant: PiRpcParticipant): Json => ({
  participant_id: participant.identity.participantId,
  route: participant.identity.route,
  runtime_id: participant.identity.hostRuntimeId,
  agent: participant.identity.agent,
  cwd_hash: participant.identity.cwdHash,
  profile_hash: participant.identity.profileHash,
  epoch_hash: participant.identity.epochHash,
  provenance: participant.provenance,
});

describe.skipIf(!enabled)("Pi RPC qualification owned pair", () => {
  it("keeps exact RPC sessions independent and records actual provider/model", async () => {
    const provider = process.env.GPTQUEUE_PI_PROVIDER;
    const model = process.env.GPTQUEUE_PI_MODEL;
    if (!provider || !model) throw new Error("GPTQUEUE_PI_PROVIDER and GPTQUEUE_PI_MODEL are required for the gated Pi run");
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = {
      schema_version: 1, run_id: runId, route: "pi-rpc-cli", execution: { status: "running" }, passed: false,
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
    let set: ReturnType<typeof createPiAdapters> | undefined;
    let sender: PiRpcParticipant | undefined;
    let receiver: PiRpcParticipant | undefined;
    let failure: unknown;
    try {
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      await persist("source-hashes", receipt.source_hashes);
      redis = await startOwnedRedis();
      receipt.redis = redactUrl(redis.url);
      workspace = await mkdtemp(join(tmpdir(), "gptq-qualification-pi-rpc-"));
      set = createPiAdapters({ workspaceRoot: workspace, provider, model });
      const adapter = set.adapters.find(({ spec }) => spec.id === "pi-rpc-cli");
      if (!adapter) throw new Error("pi-rpc-cli adapter missing");
      const signal = AbortSignal.timeout(420_000);
      const availability = await adapter.preflight(signal);
      await persist("preflight", availability);
      expect(availability.kind).toBe("available");
      sender = await adapter.launch({ role: "sender", pairId: "pi-rpc-conformance", nonce: "sender", redisUrl: redis.url }, signal) as PiRpcParticipant;
      await persist("sender-launched", sessionIdentity(sender));
      receiver = await adapter.launch({ role: "receiver", pairId: "pi-rpc-conformance", nonce: "receiver", redisUrl: redis.url }, signal) as PiRpcParticipant;
      await persist("receiver-launched", sessionIdentity(receiver));
      receipt.identities = { sender: sessionIdentity(sender), receiver: sessionIdentity(receiver) };
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      expect(sender.provenance.provider).toBe(provider);
      expect(sender.provenance.model).toBe(model);
      expect(receiver.provenance.provider).toBe(provider);
      expect(receiver.provenance.model).toBe(model);
      const initial = await Promise.all([sender.status(signal), receiver.status(signal)]);
      await persist("initial-status", initial);
      expect(initial).toEqual([{ kind: "idle", runtimeId: sender.identity.hostRuntimeId }, { kind: "idle", runtimeId: receiver.identity.hostRuntimeId }]);
      await Promise.all([
        sender.prompt("Call get_runtime_status, then reply PI_RPC_SENDER_READY.", signal),
        receiver.prompt("Call get_runtime_status, then reply PI_RPC_RECEIVER_READY.", signal),
      ]);
      const [senderHistory, receiverHistory] = await Promise.all([sender.history(signal), receiver.history(signal)]);
      await writeFile(join(artifactDir, "history-sender-sanitized.json"), `${JSON.stringify(sanitizeEvidence(senderHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "history-receiver-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receiverHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await persist("history", { sender: senderHistory, receiver: receiverHistory });
      expect(hasPiAssistantRuntimeEvidence(senderHistory, { marker: "PI_RPC_SENDER_READY", agent: sender.identity.agent, runtimeId: sender.identity.hostRuntimeId })).toBe(true);
      expect(hasPiAssistantRuntimeEvidence(receiverHistory, { marker: "PI_RPC_RECEIVER_READY", agent: receiver.identity.agent, runtimeId: receiver.identity.hostRuntimeId })).toBe(true);
      const final = await Promise.all([sender.status(signal), receiver.status(signal)]);
      await persist("final-status", final);
      expect(final).toEqual([{ kind: "idle", runtimeId: sender.identity.hostRuntimeId }, { kind: "idle", runtimeId: receiver.identity.hostRuntimeId }]);
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
      await persist("failure-histories", histories).catch(() => undefined);
    } finally {
      await persist("cleanup-before", { sender: Boolean(sender), receiver: Boolean(receiver), factory: Boolean(set), redis: Boolean(redis) }).catch(() => undefined);
      const cleanup: CleanupResult[] = [];
      const actions: readonly Readonly<{ name: string; run: () => Promise<void> }>[] = [
        { name: "sender.close", run: async () => { await sender?.close(); } },
        { name: "receiver.close", run: async () => { await receiver?.close(); } },
        { name: "pi-factory.close", run: async () => { await set?.close(); } },
        { name: "redis.close", run: async () => { await redis?.close(); } },
        { name: "workspace.remove", run: async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); } },
      ];
      for (const action of actions) {
        try { await action.run(); cleanup.push({ name: action.name, status: "fulfilled" }); }
        catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      const sourceHashesAfter = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      receipt.source_hashes_after = sourceHashesAfter;
      const sourceHashesBefore = receipt.source_hashes as Record<string, string>;
      const sourceHashesMatch = JSON.stringify(sourceHashesBefore) === JSON.stringify(sourceHashesAfter);
      receipt.source_hashes_match = sourceHashesMatch;
      if (!sourceHashesMatch) {
        receipt.passed = false;
        receipt.execution = { status: "failed", detail: "source hash changed during Pi RPC qualification" };
        failure ??= new Error("source hash changed during Pi RPC qualification");
      }
      if (cleanup.some(result => result.status === "rejected")) {
        receipt.passed = false;
        receipt.execution = { status: "failed", detail: "Pi RPC cleanup rejected" };
        failure ??= new Error("Pi RPC cleanup rejected");
      }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup-after", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
