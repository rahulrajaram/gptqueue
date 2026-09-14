import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_CODEX_HEADLESS === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/codex-headless");
const sourceFiles = ["tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-codex-headless.test.ts", "dist/registered-shell/codex-socket.js"] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });

type Json = Record<string, unknown>;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const redactedUrl = (value: string): Json => { const url = new URL(value); return { protocol: url.protocol, hostname: url.hostname, port: url.port, database: url.pathname.slice(1) }; };

describe.skipIf(!enabled)("Codex headless qualification", () => {
  it("keeps a live native thread identity through one GPTQueue control task", async () => {
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, route: "codex-headless", passed: false, execution: { status: "running" }, started_at: new Date().toISOString(), phases: [], cleanup: [] };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let set: ReturnType<typeof createCodexAdapters> | undefined;
    let participant: CodexParticipant | undefined;
    let failure: unknown;
    try {
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      await persist("source-hashes", receipt.source_hashes);
      redis = await startOwnedRedis();
      receipt.redis = redactedUrl(redis.url);
      workspace = await mkdtemp(join(tmpdir(), "gptq-qualification-codex-headless-"));
      set = createCodexAdapters({ workspaceRoot: workspace, model: "gpt-5.6-luna" });
      const adapter = set.adapters.find(({ spec }) => spec.id === "codex-headless");
      if (!adapter) throw new Error("codex-headless adapter missing");
      const signal = AbortSignal.timeout(420_000);
      participant = await adapter.launch({ role: "sender", pairId: "codex-headless-conformance", nonce: randomUUID(), redisUrl: redis.url }, signal) as CodexParticipant;
      receipt.identity = participant.identity;
      await persist("launched", { identity: participant.identity, provenance: participant.provenance, status: await participant.status(signal) });
      expect(participant.identity.hostRuntimeId).toMatch(/\S/u);
      expect((await participant.status(signal)).kind).toBe("unknown");
      const reply = await participant.prompt("Return the exact marker HEADLESS_CONTROL_READY to the task sender using a result message.", signal) as Json;
      await persist("control-reply", reply);
      expect(reply.from).toBe(participant.identity.agent);
      expect(reply.type).toBe("result");
      expect(reply.payload && typeof reply.payload === "object").toBe(true);
      expect(typeof (reply.payload as Json).in_reply_to).toBe("string");
      expect((reply.payload as Json).content).toBe("HEADLESS_CONTROL_READY");
      const history = await participant.history(signal);
      await writeFile(join(artifactDir, "history-sanitized.json"), `${JSON.stringify(sanitizeEvidence(history, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      await persist("history", history);
      expect(JSON.stringify(history)).toContain("HEADLESS_CONTROL_READY");
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
    } finally {
      if (participant) {
        try { await persist("final-history", await participant.history(AbortSignal.timeout(10_000))); }
        catch (error) { receipt.history_capture_error = String(error); }
      }
      const cleanup: Json[] = [];
      for (const [name, run] of [
        ["participant.close", async () => { await participant?.close(); }],
        ["codex-factory.close", async () => { await set?.close(); }],
        ["redis.close", async () => { await redis?.close(); }],
        ["workspace.remove", async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); }],
      ] as const) {
        try { await run(); cleanup.push({ name, status: "fulfilled" }); }
        catch (error) { cleanup.push({ name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      receipt.source_hashes_after = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(join(repo, file))])));
      if (JSON.stringify(receipt.source_hashes) !== JSON.stringify(receipt.source_hashes_after)) {
        receipt.passed = false;
        receipt.execution = { status: "failed", detail: "source changed during headless conformance" };
        failure ??= new Error("source changed during headless conformance");
      }
      if (cleanup.some(value => value.status === "rejected")) {
        receipt.passed = false;
        receipt.execution = { status: "failed", detail: "headless cleanup rejected" };
        failure ??= new Error("headless cleanup rejected");
      }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
