import { describe, expect, it } from "vitest";
import { assertPrivateRedisUrl, createGenericAdapters, sanitizeGenericValue } from "./qualification-generic.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import { extractGenericTraces } from "./qualification-evidence.js";
import { startOwnedRedis } from "./owned-redis.js";
import { checkExchangeEvidence } from "./oracle.js";
import { randomUUID } from "node:crypto";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { GenericParticipant, RawEvidenceRef } from "./qualification-types.js";

describe("generic qualification adapters", () => {
  it("requires an owned loopback Redis DB15 URL", () => {
    expect(assertPrivateRedisUrl("redis://127.0.0.1:6379/15").pathname).toBe("/15");
    expect(() => assertPrivateRedisUrl("redis://127.0.0.1:6379/0")).toThrow(/private loopback/);
    expect(() => assertPrivateRedisUrl("redis://example.test:6379/15")).toThrow(/private loopback/);
    expect(() => assertPrivateRedisUrl("redis://:secret@127.0.0.1:6379/15")).toThrow(/without credentials/);
  });

  it("redacts session and credential fields recursively", () => {
    expect(sanitizeGenericValue({ session_id: "private", nested: { token: "secret", value: 4 } })).toEqual({ session_id: "[redacted]", nested: { token: "[redacted]", value: 4 } });
  });

  it("keeps the call result separate from the history envelope", () => {
    const history = sanitizeGenericValue({ isError: false, result: { message_id: "m1" } }) as Record<string, unknown>;
    expect(history).toEqual({ isError: false, result: { message_id: "m1" } });
    expect((history.result as Record<string, unknown>).message_id).toBe("m1");
  });

  it("exposes three non-model generic routes and reports setup gaps", async () => {
    const factory = createGenericAdapters({ repo: "/definitely/missing/gptqueue" });
    expect(factory.adapters.map(({ spec }) => spec.id)).toEqual(["generic-stdio", "generic-http", "generic-stateless"]);
    expect(factory.adapters.every(({ spec }) => spec.modelBacked === false)).toBe(true);
    await expect(factory.adapters[0]!.preflight(new AbortController().signal)).resolves.toMatchObject({ kind: "setup_gap" });
    await factory.close();
  });

  it("uses the HTTP transport script for both HTTP-backed preflights", async () => {
    const root = await mkdtemp(join(tmpdir(), "generic-preflight-"));
    try {
      await mkdir(join(root, "dist/transports"), { recursive: true });
      await writeFile(join(root, "dist/transports/http.js"), "fixture");
      const factory = createGenericAdapters({ repo: root });
      await expect(factory.adapters.find(({ spec }) => spec.id === "generic-stdio")!.preflight(new AbortController().signal)).resolves.toMatchObject({ kind: "setup_gap", detail: "missing dist/mcp-server/index.js" });
      await expect(factory.adapters.find(({ spec }) => spec.id === "generic-http")!.preflight(new AbortController().signal)).resolves.toEqual({ kind: "available" });
      await expect(factory.adapters.find(({ spec }) => spec.id === "generic-stateless")!.preflight(new AbortController().signal)).resolves.toEqual({ kind: "available" });
      expect(factory.transportEvidence()).toBeNull();
      await factory.close();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe.skipIf(process.env.GPTQUEUE_GENERIC_QUALIFICATION !== "1")("generic nine-pair conformance", () => {
  it("runs all ordered pairs across stdio, HTTP, and explicit-session routes", async () => {
    const run = randomUUID();
    const evidenceRoot = join(process.cwd(), ".gptqueue/repair-qualification/20260912/generic-qualification", run);
    await mkdir(evidenceRoot, { recursive: true });
    const hashedFiles = [
      "tests/acceptance/qualification-generic.ts", "tests/acceptance/qualification-generic.test.ts",
      "tests/acceptance/qualification-discovery.ts",
      "tests/acceptance/qualification-types.ts", "tests/acceptance/oracle.ts", "tests/acceptance/owned-redis.ts",
      "dist/mcp-server/index.js", "dist/transports/http.js",
    ] as const;
    const sourceHashes = Object.fromEntries(await Promise.all(hashedFiles.map(async (file) => [file, createHash("sha256").update(await readFile(join(process.cwd(), file))).digest("hex")] as const)));
    const ownedRedis = await startOwnedRedis();
    const factory = createGenericAdapters({ repo: process.cwd() });
    const participants: GenericParticipant[] = [];
    const byRoute = new Map<string, typeof participants>();
    const rows: Record<string, unknown>[] = [];
    const discoveryHistoryRefs: RawEvidenceRef[] = [];
    const discoveryTraces = new Map<string, ReturnType<typeof extractGenericTraces>>();
    let primary: unknown;
    try {
      for (const adapter of factory.adapters) {
        const firstCandidate = await adapter.launch({ role: "sender", pairId: `${adapter.spec.id}-1`, nonce: randomUUID(), redisUrl: ownedRedis.url }, AbortSignal.timeout(45_000));
        if (firstCandidate.kind !== "generic") throw new Error("generic adapter returned a model participant");
        participants.push(firstCandidate);
        const secondCandidate = await adapter.launch({ role: "receiver", pairId: `${adapter.spec.id}-2`, nonce: randomUUID(), redisUrl: ownedRedis.url }, AbortSignal.timeout(45_000));
        if (secondCandidate.kind !== "generic") throw new Error("generic adapter returned a model participant");
        participants.push(secondCandidate);
        const first = firstCandidate, second = secondCandidate;
        byRoute.set(adapter.spec.id, [first, second]);
      }
      const discoveryHistoryRoot = join(evidenceRoot, "participant-histories");
      await mkdir(discoveryHistoryRoot, { recursive: true });
      for (const participant of participants) {
        await participant.call("list_agents", {}, AbortSignal.timeout(30_000));
      }
      for (const participant of participants) {
        const calls = await participant.history(AbortSignal.timeout(10_000));
        const serialized = JSON.stringify(calls, null, 2) + "\n";
        const fileName = `${participant.identity.agent.replace(/[^A-Za-z0-9._-]+/g, "_")}-${participant.identity.participantId.replace(/[^A-Za-z0-9._-]+/g, "_")}.json`;
        const path = join(discoveryHistoryRoot, fileName);
        await writeFile(path, serialized, { mode: 0o600 });
        const rawHistoryRef: RawEvidenceRef = Object.freeze({
          path,
          sha256: createHash("sha256").update(serialized).digest("hex"),
          sourceRevision: sourceHashes["tests/acceptance/qualification-generic.ts"]!,
          oracleRevision: sourceHashes["tests/acceptance/oracle.ts"]!,
        });
        discoveryHistoryRefs.push(rawHistoryRef);
        discoveryTraces.set(participant.identity.agent, extractGenericTraces(calls, participant.identity, rawHistoryRef));
      }
      for (const senderRoute of factory.adapters.map(({ spec }) => spec.id)) for (const receiverRoute of factory.adapters.map(({ spec }) => spec.id)) {
        const sender = byRoute.get(senderRoute)![0]!;
        const receiver = byRoute.get(receiverRoute)![senderRoute === receiverRoute ? 1 : 0]!;
        const requestContent = `generic-request-${senderRoute}-${receiverRoute}-${randomUUID()}`;
        const sent = (await sender.call("send_message", { to: receiver.identity.agent, type: "task", content: requestContent, idempotency_key: randomUUID() }, AbortSignal.timeout(30_000))) as Record<string, unknown>;
        const receivedResult = (await receiver.call("receive_message", { timeout: 5 }, AbortSignal.timeout(30_000))) as Record<string, unknown>;
        const received = receivedResult.message as Record<string, unknown>;
        expect(sent.message_id).toBe(received.id);
        expect(received.type).toBe("task");
        expect((received.payload as Record<string, unknown>).content).toBe(requestContent);
        const expected = `reply:${requestContent}`;
        const replied = (await receiver.call("send_message", { to: sender.identity.agent, type: "result", content: expected, in_reply_to: received.id, idempotency_key: randomUUID() }, AbortSignal.timeout(30_000))) as Record<string, unknown>;
        const returnedResult = (await sender.call("receive_message", { timeout: 5 }, AbortSignal.timeout(30_000))) as Record<string, unknown>;
        const returned = returnedResult.message as Record<string, unknown>;
        expect(replied.message_id).toBe(returned.id);
        expect(returned.type).toBe("result");
        const verdict = checkExchangeEvidence({
          sender: sender.identity.agent, recipient: receiver.identity.agent, expected_reply_content: expected,
          request: { id: String(received.id), from: String(received.from), to: String(received.to), content: String((received.payload as Record<string, unknown>).content) },
          reply: { id: String(returned.id), from: String(returned.from), to: String(returned.to), in_reply_to: String((returned.payload as Record<string, unknown>).in_reply_to), content: String((returned.payload as Record<string, unknown>).content) },
          request_consumption: { message_id: String(received.id), actor: receiver.identity.agent, consumed: true, acknowledged: false },
          reply_consumption: { message_id: String(returned.id), actor: sender.identity.agent, consumed: true, acknowledged: false },
          request_requires_ack: false, reply_requires_ack: false, execution: { status: "completed" },
        });
        expect(verdict.outcome).toBe("meets");
        const senderDiscovery = collectPeerDiscovery(sender.identity, receiver.identity, discoveryTraces.get(sender.identity.agent)!);
        const receiverDiscovery = collectPeerDiscovery(receiver.identity, sender.identity, discoveryTraces.get(receiver.identity.agent)!);
        rows.push({ sender: sender.identity, receiver: receiver.identity, sent, received, replied, returned, verdict, discovery: { sender_to_receiver: senderDiscovery, receiver_to_sender: receiverDiscovery } });
      }
    } catch (error) {
      primary = error;
      throw error;
    } finally {
      const transportBeforeCleanup = factory.transportEvidence();
      const historyResults = await Promise.allSettled(participants.map((participant) => participant.history(AbortSignal.timeout(10_000))));
      const histories = historyResults.map((result, index) => result.status === "fulfilled" ? { participant: participants[index]!.identity, calls: result.value } : { participant: participants[index]!.identity, error: String(result.reason) });
      const participantCleanup = await Promise.allSettled(participants.reverse().map((participant) => participant.close()));
      const factoryCleanup = await Promise.allSettled([factory.close()]);
      const transportAfterCleanup = factory.transportEvidence();
      const redisCleanup = await Promise.allSettled([ownedRedis.close()]);
      const cleanup = [...participantCleanup, ...factoryCleanup, ...redisCleanup];
      const historyFailures = historyResults.filter((result): result is PromiseRejectedResult => result.status === "rejected").map(({ reason }) => reason);
      const failures = cleanup.filter((result): result is PromiseRejectedResult => result.status === "rejected").map(({ reason }) => reason);
      const sourceHashesAfter = Object.fromEntries(await Promise.all(hashedFiles.map(async (file) => [file, createHash("sha256").update(await readFile(join(process.cwd(), file))).digest("hex")] as const)));
      const sourceHashesMatch = JSON.stringify(sourceHashes) === JSON.stringify(sourceHashesAfter);
      const transportTerminated = transportAfterCleanup?.terminated === true;
      await writeFile(join(evidenceRoot, "history.json"), JSON.stringify(histories, null, 2) + "\n", { mode: 0o600 });
      await writeFile(join(evidenceRoot, "receipt.json"), JSON.stringify({ run, rows, discovery_history_refs: Object.freeze([...discoveryHistoryRefs]), cleanup: failures.length === 0, history_complete: historyFailures.length === 0, redis_url: "[redacted]", source_hashes: sourceHashes, source_hashes_after: sourceHashesAfter, source_hashes_match: sourceHashesMatch, transport_evidence_before_cleanup: transportBeforeCleanup, transport_evidence_after_cleanup: transportAfterCleanup, transport_terminated_after_cleanup: transportTerminated }, null, 2) + "\n", { mode: 0o600 });
      await writeFile(join(evidenceRoot, "hashes.json"), JSON.stringify({ source_hashes: sourceHashes, rows_hash: createHash("sha256").update(JSON.stringify(rows)).digest("hex"), history: createHash("sha256").update(JSON.stringify(histories)).digest("hex"), discovery_history: createHash("sha256").update(JSON.stringify(discoveryHistoryRefs)).digest("hex") }, null, 2) + "\n", { mode: 0o600 });
      expect(sourceHashesAfter).toEqual(sourceHashes);
      expect(transportBeforeCleanup).not.toBeNull();
      expect(transportAfterCleanup).not.toBeNull();
      expect(transportTerminated).toBe(true);
      if (failures.length > 0 || historyFailures.length > 0) {
        const errors = primary === undefined ? [...historyFailures, ...failures] : [primary, ...historyFailures, ...failures];
        throw new AggregateError(errors, "generic qualification evidence or cleanup failed");
      }
    }
  }, 180_000);
});
