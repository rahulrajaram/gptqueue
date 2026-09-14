import { describe, expect, it } from "vitest";
import { CohortLeasePool, withPairLease, type CohortMember } from "./qualification-scheduler.js";
import type { PairLeasePool, PairSpec, Participant } from "./qualification-types.js";

const participant = (route: "codex-headless" | "pi-sdk", id: string): Participant => ({
  kind: "model",
  identity: { participantId: id, route, hostRuntimeId: `runtime-${id}`, agent: `agent-${id}`, cwdHash: `cwd-${id}`, profileHash: `profile-${id}`, epochHash: `epoch-${id}` },
  prompt: async () => undefined,
  status: async () => ({ kind: "idle", runtimeId: `runtime-${id}` }),
  history: async () => undefined,
  close: async () => undefined,
});
const member = (route: "codex-headless" | "pi-sdk", id: string): CohortMember => {
  const value = participant(route, id);
  return { participant: value, identity: value.identity };
};
const members: readonly CohortMember[] = [member("codex-headless", "a1"), member("codex-headless", "a2"), member("pi-sdk", "b1"), member("pi-sdk", "b2")];
const pair = (pairId: string, sender: PairSpec["sender"], receiver: PairSpec["receiver"], nonce = pairId): PairSpec => ({ pairId, sender, receiver, nonce });

describe("qualification pair lease scheduler", () => {
  it("reserves reverse pairs through one shared pool without partial holds", async () => {
    const pool = new CohortLeasePool(members);
    const first = await pool.acquirePair(pair("a->b", "codex-headless", "pi-sdk"), new AbortController().signal);
    const second = await pool.acquirePair(pair("b->a", "pi-sdk", "codex-headless"), new AbortController().signal);
    expect(new Set([second.lease.sender.participantId, second.lease.receiver.participantId]).size).toBe(2);
    expect(second.lease.sender.participantId).not.toBe(first.lease.receiver.participantId);
    expect(second.lease.receiver.participantId).not.toBe(first.lease.sender.participantId);
    await first.release();
    await second.release();
  });

  it("requires two distinct participants for same-route pairs", async () => {
    const pool = new CohortLeasePool(members);
    const lease = await pool.acquirePair(pair("same", "codex-headless", "codex-headless"), new AbortController().signal);
    expect(lease.lease.sender.participantId).not.toBe(lease.lease.receiver.participantId);
    await lease.release();
  });

  it("queues contention without reusing held participants", async () => {
    const pool = new CohortLeasePool(members);
    const first = await pool.acquirePair(pair("first", "codex-headless", "pi-sdk"), new AbortController().signal);
    const held = await pool.acquirePair(pair("held", "codex-headless", "pi-sdk"), new AbortController().signal);
    const secondPromise = pool.acquirePair(pair("third", "codex-headless", "pi-sdk"), new AbortController().signal);
    let settled = false;
    void secondPromise.then(() => { settled = true; });
    await Promise.resolve();
    expect(settled).toBe(false);
    await first.release();
    const second = await secondPromise;
    expect(second.lease.sender.participantId).toBe(first.lease.sender.participantId);
    expect(second.lease.receiver.participantId).toBe(first.lease.receiver.participantId);
    expect(second.lease.sender.participantId).not.toBe(held.lease.sender.participantId);
    expect(second.lease.receiver.participantId).not.toBe(held.lease.receiver.participantId);
    await held.release();
    await second.release();
  });

  it("removes an aborted queued lease without consuming a slot", async () => {
    const pool = new CohortLeasePool(members);
    const first = await pool.acquirePair(pair("held", "codex-headless", "pi-sdk"), new AbortController().signal);
    const second = await pool.acquirePair(pair("also-held", "codex-headless", "pi-sdk"), new AbortController().signal);
    const controller = new AbortController();
    const queued = pool.acquirePair(pair("cancel", "codex-headless", "pi-sdk"), controller.signal);
    controller.abort(new Error("cancelled"));
    await expect(queued).rejects.toThrow("cancelled");
    await first.release();
    await second.release();
    const next = await pool.acquirePair(pair("next", "codex-headless", "pi-sdk"), new AbortController().signal);
    await next.release();
  });

  it("releases both sides when work fails or aborts after acquisition", async () => {
    const pool = new CohortLeasePool(members);
    await expect(withPairLease(pool, pair("failure", "codex-headless", "pi-sdk"), new AbortController().signal, async () => {
      throw new Error("timeout");
    })).rejects.toThrow("timeout");
    const controller = new AbortController();
    await expect(withPairLease(pool, pair("abort", "codex-headless", "pi-sdk"), controller.signal, async () => {
      controller.abort(new Error("cancelled during work"));
      throw controller.signal.reason;
    })).rejects.toThrow("cancelled during work");
    const next = await pool.acquirePair(pair("reused", "codex-headless", "pi-sdk"), new AbortController().signal);
    await next.release();
  });

  it("releases a post-acquire abort before invoking work", async () => {
    const realPool = new CohortLeasePool(members);
    const acquired = await realPool.acquirePair(pair("post-abort", "codex-headless", "pi-sdk"), new AbortController().signal);
    const controller = new AbortController();
    const pool: PairLeasePool = { acquirePair: async () => { controller.abort(new Error("cancelled before work")); return acquired; } };
    let called = false;
    await expect(withPairLease(pool, pair("post-abort", "codex-headless", "pi-sdk"), controller.signal, async () => { called = true; })).rejects.toThrow("cancelled before work");
    expect(called).toBe(false);
    const reused = await realPool.acquirePair(pair("reused-after-abort", "codex-headless", "pi-sdk"), new AbortController().signal);
    await reused.release();
  });

  it("rejects wrong kind, duplicate agents, and same-host runtimes", () => {
    const bad = participant("codex-headless", "bad");
    expect(() => new CohortLeasePool([{ participant: bad, identity: { ...bad.identity, participantId: "other" } }])).toThrow(/identity\/kind/);
    const wrongKind = { ...bad, kind: "generic" as const } as Participant;
    expect(() => new CohortLeasePool([{ participant: wrongKind, identity: wrongKind.identity }])).toThrow(/identity\/kind/);
    const duplicateAgent = participant("codex-headless", "other");
    const duplicateAgentIdentity = { ...duplicateAgent.identity, agent: bad.identity.agent };
    expect(() => new CohortLeasePool([{ participant: bad, identity: bad.identity }, { participant: { ...duplicateAgent, identity: duplicateAgentIdentity }, identity: duplicateAgentIdentity }])).toThrow(/duplicate cohort agent/);
    const duplicateRuntime = participant("codex-headless", "other-runtime");
    const duplicateRuntimeIdentity = { ...duplicateRuntime.identity, hostRuntimeId: bad.identity.hostRuntimeId };
    expect(() => new CohortLeasePool([{ participant: bad, identity: bad.identity }, { participant: { ...duplicateRuntime, identity: duplicateRuntimeIdentity }, identity: duplicateRuntimeIdentity }])).toThrow(/duplicate cohort runtime/);
  });
});
