import { describe, expect, it, vi } from "vitest";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_CODEX_APPSERVER === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-codex-appserver");
const sourceFiles = [
  "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-codex-appserver.test.ts",
  "tests/acceptance/qualification-types.ts", "tests/acceptance/qualification-routes.ts",
  "tests/acceptance/codex-support.ts", "src/registered-shell/codex-socket.ts", "src/registered-shell/codex-history.ts",
] as const;
vi.setConfig({ testTimeout: 360_000, hookTimeout: 60_000 });

type Json = Record<string, unknown>;
type CleanupResult = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;

const hasRuntimeStatusCall = (history: unknown): boolean => {
  if (!history || typeof history !== "object") return false;
  const turns = (history as { turns?: unknown }).turns;
  if (!Array.isArray(turns)) return false;
  return turns.some(turn => {
    if (!turn || typeof turn !== "object" || !Array.isArray((turn as { items?: unknown }).items)) return false;
    return (turn as { items: unknown[] }).items.some(item => item && typeof item === "object" &&
      (item as { type?: unknown }).type === "mcpToolCall" && (item as { tool?: unknown }).tool === "get_runtime_status");
  });
};
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const redactUrl = (url: string): Json => {
  const parsed = new URL(url);
  return { protocol: parsed.protocol, hostname: parsed.hostname, database: parsed.pathname.slice(1) };
};
const waitForIdle = async (participant: CodexParticipant, signal: AbortSignal): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const status = await participant.status(signal);
    if (status.kind === "idle") return;
    if (status.kind === "terminated" || status.kind === "unknown") throw new Error(`Codex participant did not return to idle: ${JSON.stringify(status)}`);
    await new Promise<void>(resolveDelay => setTimeout(resolveDelay, 250));
  }
  throw new Error("Codex participant remained busy past the idle wait deadline");
};

describe.skipIf(!enabled)("Codex qualification owned app-server", () => {
  it("keeps two exact runtime identities independent through prompt, history, and close", async () => {
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    const phasesDir = join(artifactDir, "phases");
    await mkdir(phasesDir, { recursive: true, mode: 0o700 });
    const receipt: Json = {
      schema_version: 1, run_id: runId, route: "codex-appserver", execution: { status: "running" }, passed: false,
      started_at: new Date().toISOString(), command: [process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex", "app-server", "--listen", "unix://<owned>"],
      model: "gpt-5.6-luna", source_hashes: {}, identities: {}, phases: [], cleanup: [],
    };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true }) };
      const phases = receipt.phases as Json[];
      phases.push(phase);
      await writeFile(join(phasesDir, `${String(phases.length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined;
    let root: string | undefined;
    let set: ReturnType<typeof createCodexAdapters> | undefined;
    let sender: CodexParticipant | undefined;
    let receiver: CodexParticipant | undefined;
    let failure: unknown;
    try {
      await persist("source-hashes", Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))]))));
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      redis = await startOwnedRedis();
      receipt.redis = redactUrl(redis.url);
      root = await mkdtemp(join(tmpdir(), "gptqueue-qualification-codex-appserver-"));
      set = createCodexAdapters({ workspaceRoot: root, model: "gpt-5.6-luna" });
      const adapter = set.adapters.find(({ spec }) => spec.id === "codex-appserver");
      if (!adapter) throw new Error("codex-appserver adapter missing");
      const signal = AbortSignal.timeout(300_000);
      await persist("launch-before", { pair_id: "appserver-conformance", roles: ["sender", "receiver"] });
      sender = await adapter.launch({ role: "sender", pairId: "appserver-conformance", nonce: "sender-ready", redisUrl: redis.url }, signal) as CodexParticipant;
      await persist("sender-launched", { identity: sender.identity, provenance: sender.provenance });
      receiver = await adapter.launch({ role: "receiver", pairId: "appserver-conformance", nonce: "receiver-ready", redisUrl: redis.url }, signal) as CodexParticipant;
      await persist("receiver-launched", { identity: receiver.identity, provenance: receiver.provenance });
      receipt.config_hash = sender.provenance.configHash;
      receipt.model = sender.provenance.model;
      receipt.socket_hash = sender.provenance.socketHash;
      receipt.identities = { sender: sender.identity, receiver: receiver.identity };
      expect(sender.identity.route).toBe("codex-appserver");
      expect(receiver.identity.route).toBe("codex-appserver");
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      expect(sender.identity.cwdHash).not.toBe(receiver.identity.cwdHash);
      expect(sender.identity.profileHash).toBe(receiver.identity.profileHash);
      const initial = await Promise.all([sender.status(signal), receiver.status(signal)]);
      await persist("initial-status", initial);
      expect(initial).toEqual([{ kind: "idle", runtimeId: sender.identity.hostRuntimeId }, { kind: "idle", runtimeId: receiver.identity.hostRuntimeId }]);
      await persist("prompt-before", { sender: sender.identity.hostRuntimeId, receiver: receiver.identity.hostRuntimeId });
      await Promise.all([
        sender.prompt("Call get_runtime_status now, then reply APP_SERVER_SENDER_READY.", signal),
        receiver.prompt("Call get_runtime_status now, then reply APP_SERVER_RECEIVER_READY.", signal),
      ]);
      await persist("prompt-after", { sender: sender.identity.hostRuntimeId, receiver: receiver.identity.hostRuntimeId });
      const [senderHistory, receiverHistory] = await Promise.all([sender.history(signal), receiver.history(signal)]);
      await writeFile(join(artifactDir, "history-sender-sanitized.json"), `${JSON.stringify(sanitizeEvidence(senderHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "history-receiver-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receiverHistory, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await persist("history-status", { sender: { id: (senderHistory as { id?: unknown }).id, has_runtime_status: hasRuntimeStatusCall(senderHistory) }, receiver: { id: (receiverHistory as { id?: unknown }).id, has_runtime_status: hasRuntimeStatusCall(receiverHistory) } });
      expect((senderHistory as { id?: unknown }).id).toBe(sender.identity.hostRuntimeId);
      expect((receiverHistory as { id?: unknown }).id).toBe(receiver.identity.hostRuntimeId);
      expect(hasRuntimeStatusCall(senderHistory)).toBe(true);
      expect(hasRuntimeStatusCall(receiverHistory)).toBe(true);
      await Promise.all([waitForIdle(sender, signal), waitForIdle(receiver, signal)]);
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
      const evidence = error && typeof error === "object" ? (error as { evidence?: unknown }).evidence : undefined;
      if (evidence !== undefined) await persist("failure-evidence", evidence).catch(() => undefined);
    } finally {
      await persist("cleanup-before", { sender: Boolean(sender), receiver: Boolean(receiver), host: Boolean(set), redis: Boolean(redis) }).catch(() => undefined);
      const cleanup: CleanupResult[] = [];
      const actions: readonly Readonly<{ name: string; run: () => Promise<void> }>[] = [
        { name: "sender.close", run: async () => { await sender?.close(); } },
        { name: "receiver.close", run: async () => { await receiver?.close(); } },
        { name: "codex-host.close", run: async () => { await set?.close(); } },
        { name: "redis.close", run: async () => { await redis?.close(); } },
        { name: "workspace.remove", run: async () => { if (root) await rm(root, { recursive: true, force: true }); } },
      ];
      for (const action of actions) {
        try { await action.run(); cleanup.push({ name: action.name, status: "fulfilled" }); }
        catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      const cleanupFailures = cleanup.filter(item => item.status === "rejected");
      if (cleanupFailures.length > 0) {
        receipt.passed = false;
        failure ??= new Error(`Codex conformance cleanup failed: ${cleanupFailures.map(item => item.name).join(", ")}`);
      }
      receipt.source_hashes_after = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      if (JSON.stringify(receipt.source_hashes) !== JSON.stringify(receipt.source_hashes_after)) {
        receipt.passed = false;
        failure ??= new Error("Codex conformance source changed during execution");
      }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup-after", cleanup);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
