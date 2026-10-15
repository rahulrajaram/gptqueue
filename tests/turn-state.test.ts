import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { admitTurnState, applyTurnReport, TURN_STATE_TTL_SECONDS, TurnStateStore, type TurnReport, type TurnState } from "../src/core/turn-state.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { assertNotLiveDb } from "./helpers/redis-test-utils.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
assertNotLiveDb(REDIS_URL, "turn-state");

const T = (s: number): string => new Date(Date.parse("2030-01-01T00:00:00.000Z") + s * 1000).toISOString();
const report = (overrides: Partial<TurnReport> = {}): TurnReport => ({ phase: "running", at: T(0), runtime: "codex", session_id: "thread-1", ...overrides });

describe("turn state model", () => {
  it("starts from nothing with the reported phase and time", () => {
    const applied = applyTurnReport(null, "lead-a", report());
    expect(applied).toEqual({ kind: "changed", state: { agent: "lead-a", phase: "running", since: T(0), reported_at: T(0), runtime: "codex", session_id: "thread-1", turn_id: null } });
  });

  it("keeps the original since when the same session repeats a phase", () => {
    const idle = applyTurnReport(null, "lead-a", report({ phase: "idle", at: T(10) }));
    if (idle.kind !== "changed") throw new Error("expected change");
    // S3: a repeat advances reported_at (a write) but keeps the phase's since.
    expect(applyTurnReport(idle.state, "lead-a", report({ phase: "idle", at: T(3600) }))).toEqual({ kind: "refreshed", state: { ...idle.state, reported_at: T(3600) } });
  });

  it("moves to a new phase with a new since", () => {
    const running = applyTurnReport(null, "lead-a", report());
    if (running.kind !== "changed") throw new Error("expected change");
    const idle = applyTurnReport(running.state, "lead-a", report({ phase: "idle", at: T(30), turn_id: "turn-9" }));
    expect(idle).toEqual({ kind: "changed", state: { ...running.state, phase: "idle", since: T(30), reported_at: T(30), turn_id: "turn-9" } });
  });

  it("treats a new session in the same phase as a change (a successor took over the mailbox)", () => {
    const first = applyTurnReport(null, "lead-a", report({ phase: "idle" }));
    if (first.kind !== "changed") throw new Error("expected change");
    const next = applyTurnReport(first.state, "lead-a", report({ phase: "idle", session_id: "thread-2", at: T(5) }));
    expect(next.kind === "changed" && next.state.session_id).toBe("thread-2");
  });

  it("ignores a report older than the stored state (async hooks land out of order)", () => {
    const idle = applyTurnReport(null, "lead-a", report({ phase: "idle", at: T(60) }));
    if (idle.kind !== "changed") throw new Error("expected change");
    expect(applyTurnReport(idle.state, "lead-a", report({ phase: "running", at: T(59) }))).toEqual({ kind: "stale" });
  });

  it("S3: running, running, then a delayed older idle: the idle is stale", () => {
    const first = applyTurnReport(null, "lead-a", report({ phase: "running", at: T(0) }));
    if (first.kind !== "changed") throw new Error("expected change");
    const second = applyTurnReport(first.state, "lead-a", report({ phase: "running", at: T(20) }));
    if (second.kind !== "refreshed") throw new Error("expected refresh");
    expect(second.state).toMatchObject({ phase: "running", since: T(0), reported_at: T(20) });
    expect(applyTurnReport(second.state, "lead-a", report({ phase: "idle", at: T(10) }))).toEqual({ kind: "stale" });
  });

  it("S3: admits a legacy record without reported_at, treating it as since", () => {
    expect(admitTurnState({ agent: "a", phase: "idle", since: T(5), runtime: "codex", session_id: "s", turn_id: null }))
      .toEqual({ agent: "a", phase: "idle", since: T(5), reported_at: T(5), runtime: "codex", session_id: "s", turn_id: null });
  });

  it("rejects malformed reports", () => {
    expect(applyTurnReport(null, "lead-a", report({ at: "not a time" }))).toEqual({ kind: "invalid" });
    expect(applyTurnReport(null, "", report())).toEqual({ kind: "invalid" });
    expect(applyTurnReport(null, "lead-a", report({ runtime: "emacs" as never }))).toEqual({ kind: "invalid" });
  });
});

describe("turn state store (Redis)", () => {
  let redis: Redis;
  const agents: string[] = [];
  const agent = (): string => { const name = `turn-test-${randomUUID().slice(0, 8)}`; agents.push(name); return name; };
  beforeAll(() => { redis = new Redis(REDIS_URL); });
  afterAll(async () => {
    if (agents.length > 0) await redis.del(...agents.map((a) => SESSION_KEYS.turnState(a)));
    redis.disconnect();
  });

  it("writes a change with a TTL and reads it back", async () => {
    const store = new TurnStateStore(redis);
    const name = agent();
    const applied = await store.report(name, report({ phase: "idle", at: T(1) }));
    expect(applied.kind).toBe("changed");
    expect(await store.read(name)).toMatchObject<Partial<TurnState>>({ agent: name, phase: "idle", since: T(1) });
    const ttl = await redis.ttl(SESSION_KEYS.turnState(name));
    expect(ttl).toBeGreaterThan(TURN_STATE_TTL_SECONDS - 60);
  });

  it("does not let a stale report overwrite a newer state", async () => {
    const store = new TurnStateStore(redis);
    const name = agent();
    await store.report(name, report({ phase: "idle", at: T(100) }));
    expect(await store.report(name, report({ phase: "running", at: T(50) }))).toEqual({ kind: "stale" });
    expect((await store.read(name))?.phase).toBe("idle");
  });

  it("replaces a malformed stored record with the next valid report", async () => {
    const store = new TurnStateStore(redis);
    const name = agent();
    await redis.set(SESSION_KEYS.turnState(name), "{garbage");
    expect(await store.read(name)).toBeNull();
    expect((await store.report(name, report())).kind).toBe("changed");
    expect((await store.read(name))?.phase).toBe("running");
  });
});
