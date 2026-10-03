import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import {
  CustodyStore,
  type CustodyClaimInput,
  type CustodyReleaseInput,
} from "../src/core/custody-store.js";
import { CUSTODY_KEYS } from "../src/core/keys.js";
import type { HandoffRecordV1 } from "../src/core/custody-model.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";


const T0 = "2030-01-01T00:00:00.000Z";
const PATH = "/srv/worktrees/one";

const claim = (
  overrides: Partial<CustodyClaimInput> = {}
): CustodyClaimInput => ({
  worktree_path: PATH,
  repo_head: "abc123",
  tree_fingerprint: "fp-1",
  lease_seconds: 3600,
  actor_name: "agent-a",
  session_id: "session-a",
  now: T0,
  ...overrides,
});

const handoff = (
  overrides: Partial<HandoffRecordV1> = {}
): HandoffRecordV1 => ({
  schema_version: 1,
  authored_by: "origin",
  repo_head: "abc123",
  tracked_tree_state: "clean",
  untracked_inventory: [],
  unfinished_work: "wrap up the batch",
  hazards: [],
  next_step: "commit the batch",
  ...overrides,
});

const release = (
  overrides: Partial<CustodyReleaseInput> = {}
): CustodyReleaseInput => ({
  worktree_path: PATH,
  actor_name: "agent-a",
  session_id: "session-a",
  handoff: handoff(),
  now: T0,
  ...overrides,
});

const expectOk = <T extends { ok: boolean }>(result: T): Extract<T, { ok: true }> => {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("expected ok result");
  return result;
};

describe("CustodyStore", () => {
  let redis: Redis;
  let store: CustodyStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new CustodyStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("claims a worktree from unowned as an initial hold", async () => {
    const res = await store.claim(claim());
    const ok = expectOk(res);
    expect(ok.record.state).toBe("held");
    expect(ok.record.mode).toBe("initial");
    expect(ok.record.custodian).toEqual({
      actor_name: "agent-a",
      session_id: "session-a",
    });
    expect(ok.record.worktree).toEqual({
      worktree_path: PATH,
      repo_head: "abc123",
      tree_fingerprint: "fp-1",
    });
    // lease = now + 3600s (2030-01-01T00:00:00Z -> 2030-01-01T01:00:00Z)
    expect(ok.record.lease_expires_at).toBe("2030-01-01T01:00:00.000Z");
    expect(ok.record.handoff).toBeUndefined();
  });

  it("returns already_held naming the current custodian on a double claim", async () => {
    await store.claim(claim());
    const second = await store.claim(
      claim({ actor_name: "agent-b", session_id: "session-b" })
    );
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("expected already_held");
    expect(second.error.code).toBe("already_held");
    expect(second.error.message).toContain("agent-a");
  });

  it("requires inventory when claiming a forfeited worktree", async () => {
    await store.claim(claim({ lease_seconds: 1 }));
    const expired = await store.status({
      worktree_path: PATH,
      now: "2030-01-01T00:00:02.000Z",
    });
    expect(expired.ok && "record" in expired && expired.record?.state).toBe(
      "forfeited"
    );

    for (const inventory of [undefined, []] as const) {
      const res = await store.claim(
        claim({
          actor_name: "agent-b",
          session_id: "session-b",
          inventory,
        })
      );
      expect(res.ok).toBe(false);
      if (res.ok) throw new Error("expected takeover_requires_inventory");
      expect(res.error.code).toBe("takeover_requires_inventory");
    }
  });

  it("takes over a forfeited worktree with inventory and clears predecessor attribution", async () => {
    await store.claim(claim({ lease_seconds: 1 }));
    await store.status({ worktree_path: PATH, now: "2030-01-01T00:00:02.000Z" });

    const res = await store.claim(
      claim({
        actor_name: "agent-b",
        session_id: "session-b",
        inventory: ["recovered.json"],
      })
    );
    const ok = expectOk(res);
    expect(ok.record.state).toBe("held");
    expect(ok.record.mode).toBe("successor_takeover");
    expect(ok.record.custodian).toEqual({
      actor_name: "agent-b",
      session_id: "session-b",
    });
    // No predecessor handoff or stale custodian attribution is carried forward.
    expect(ok.record.handoff).toBeUndefined();
    expect(ok.record.worktree).toEqual({
      worktree_path: PATH,
      repo_head: "abc123",
      tree_fingerprint: "fp-1",
    });
  });

  it("gracefully reclaims a released worktree retaining handoff provenance", async () => {
    await store.claim(claim());
    const released = await store.release(release());
    expectOk(released);

    const reclaim = await store.claim(
      claim({ actor_name: "agent-c", session_id: "session-c" })
    );
    const ok = expectOk(reclaim);
    expect(ok.record.mode).toBe("graceful_handoff");
    expect(ok.record.handoff).toEqual(handoff());
    expect(ok.record.custodian).toEqual({
      actor_name: "agent-c",
      session_id: "session-c",
    });
  });

  it("rejects a release by a non-custodian session as not_custodian", async () => {
    await store.claim(claim()); // held by session-a
    const res = await store.release(
      release({ actor_name: "agent-b", session_id: "session-b" })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected not_custodian");
    expect(res.error.code).toBe("not_custodian");
  });

  it("rejects a release of a non-held worktree as not_held", async () => {
    const res = await store.release(release());
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected not_held");
    expect(res.error.code).toBe("not_held");
  });

  it("propagates handoff admission errors on release", async () => {
    await store.claim(claim());
    const res = await store.release(
      release({ handoff: handoff({ repo_head: "" }) })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected missing_head");
    expect(res.error.code).toBe("missing_head");
  });

  it("releases a held worktree, storing the handoff and clearing the custodian", async () => {
    await store.claim(claim());
    const res = await store.release(
      release({
        handoff: handoff({
          tracked_tree_state: "dirty",
          untracked_inventory: ["wip.sh"],
          unfinished_work: "finish refactor",
        }),
      })
    );
    const ok = expectOk(res);
    expect(ok.record.state).toBe("released");
    expect(ok.record.custodian).toBeUndefined();
    expect(ok.record.mode).toBeUndefined();
    expect(ok.record.lease_expires_at).toBeUndefined();
    expect(ok.record.handoff).toEqual(
      handoff({
        tracked_tree_state: "dirty",
        untracked_inventory: ["wip.sh"],
        unfinished_work: "finish refactor",
      })
    );
  });

  it("lazily forfeits an expired lease and persists the forfeited record", async () => {
    await store.claim(claim({ lease_seconds: 1 }));

    const status = await store.status({
      worktree_path: PATH,
      now: "2030-01-01T00:00:05.000Z",
    });
    expect(status.ok && "record" in status).toBe(true);
    if (!status.ok || !("record" in status))
      throw new Error("expected ok status");
    expect(status.record?.state).toBe("forfeited");
    expect(status.record?.custodian).toBeUndefined();
    expect(status.record?.lease_expires_at).toBeUndefined();

    // The forfeited result is persisted: a later read sees forfeited too.
    const again = await store.status({
      worktree_path: PATH,
      now: "2030-01-01T00:00:06.000Z",
    });
    if (!again.ok || !("record" in again))
      throw new Error("expected ok status");
    expect(again.record?.state).toBe("forfeited");
  });

  it("does not expire a held lease that is still valid", async () => {
    await store.claim(claim({ lease_seconds: 3600 }));
    const status = await store.status({ worktree_path: PATH, now: T0 });
    if (!status.ok || !("record" in status))
      throw new Error("expected ok status");
    expect(status.record?.state).toBe("held");
  });

  it("self-heals an expired-held lease on claim so a successor can take over", async () => {
    // session-a holds until 2030-01-01T00:00:01Z.
    await store.claim(claim({ lease_seconds: 1 }));

    // A successor claims AFTER the lease lapsed: the expired-but-held record
    // must be lazily forfeited first so the takeover path runs instead of a
    // dead-end already_held.
    const res = await store.claim(
      claim({
        actor_name: "agent-b",
        session_id: "session-b",
        inventory: ["recovered.txt"],
        now: "2030-01-01T00:00:02.000Z",
        lease_seconds: 3600,
      })
    );
    const ok = expectOk(res);
    expect(ok.record.state).toBe("held");
    expect(ok.record.mode).toBe("successor_takeover");
    expect(ok.record.custodian).toEqual({
      actor_name: "agent-b",
      session_id: "session-b",
    });
    // The forfeited state was persisted before the takeover, so the finished
    // hold is clean successor attribution.
    expect(ok.record.custodian?.actor_name).toBe("agent-b");
  });

  it("keeps already_held for a claim on an unexpired hold", async () => {
    // Hold is valid through 2030-01-01T01:00:00Z.
    await store.claim(claim({ lease_seconds: 3600 }));
    const res = await store.claim(
      claim({ actor_name: "agent-b", session_id: "session-b" })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected already_held");
    expect(res.error.code).toBe("already_held");
  });

  it("releases an expired-held lease as not_held after self-healing the lapsed hold", async () => {
    // The old custodian's lease lapsed before their release attempt.
    await store.claim(claim({ lease_seconds: 1 }));
    const res = await store.release(
      release({ now: "2030-01-01T00:00:02.000Z" })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected not_held");
    // Documented coherence choice (M4): releasing a lapsed (expired-but-held)
    // hold self-heals the record to forfeited, then resolves as not_held --
    // the worktree is no longer held by anyone -- rather than silently
    // releasing a stale hold.
    expect(res.error.code).toBe("not_held");
  });

  it("returns null for an absent worktree path", async () => {
    const res = await store.status({ worktree_path: "/no/such/path", now: T0 });
    expect(res.ok && "record" in res).toBe(true);
    if (!res.ok || !("record" in res)) throw new Error("expected ok status");
    expect(res.record).toBeNull();
  });

  it("lists all stored custody records", async () => {
    await store.claim(claim({ worktree_path: "/w/a" }));
    await store.claim(
      claim({ worktree_path: "/w/b", repo_head: "def456" })
    );

    const res = await store.status({ now: T0 });
    expect(res.ok && "records" in res).toBe(true);
    if (!res.ok || !("records" in res)) throw new Error("expected ok list");
    expect(res.records).toHaveLength(2);
    expect(res.records.map((r) => r.worktree.worktree_path).sort()).toEqual([
      "/w/a",
      "/w/b",
    ]);
  });

  it("returns store_corrupt for a malformed stored record", async () => {
    await redis.hset(CUSTODY_KEYS.records, PATH, "not-json{{{");
    const res = await store.claim(claim());
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected store_corrupt");
    expect(res.error.code).toBe("store_corrupt");
    expect(res.error.message).toContain(PATH);
  });

  it("returns store_corrupt for valid JSON that is missing required fields", async () => {
    for (const bad of [
      { state: "held", lease_expires_at: "2020-01-01T00:00:00Z" }, // no worktree
      { state: "bogus", worktree: { worktree_path: PATH, repo_head: "h", tree_fingerprint: "f" } },
      { state: "held", worktree: { worktree_path: PATH, repo_head: "h", tree_fingerprint: "f" } }, // held without custodian/lease
    ]) {
      await redis.hset(CUSTODY_KEYS.records, PATH, JSON.stringify(bad));
      const res = await store.claim(claim());
      if (res.ok) throw new Error(`expected store_corrupt for ${JSON.stringify(bad)}`);
      expect(res.error.code).toBe("store_corrupt");
    }
  });

  it("returns store_corrupt for a stored handoff the domain code cannot consume", async () => {
    const worktree = { worktree_path: PATH, repo_head: "abc123", tree_fingerprint: "fp-1" };
    const { hazards: _hazards, ...withoutHazards } = handoff();
    for (const bad of [
      { state: "released", worktree, handoff: {} }, // review finding RF9
      { state: "released", worktree, handoff: withoutHazards },
      { state: "released", worktree, handoff: { ...handoff(), untracked_inventory: "a.txt" } },
      { state: "released", worktree, handoff: { ...handoff(), schema_version: 2 } },
      { state: "released", worktree, handoff: null },
      { state: "released", worktree }, // a released record always carries its handoff
      { state: "forfeited", worktree, handoff: {} }, // carried-forward handoffs are consumed too
    ]) {
      await redis.hset(CUSTODY_KEYS.records, PATH, JSON.stringify(bad));
      const res = await store
        .claim(claim({ inventory: ["a.txt"] }))
        .catch((error: unknown) => ({ thrown: String(error) }));
      expect(res, JSON.stringify(bad)).toMatchObject({ ok: false, error: { code: "store_corrupt" } });
    }
  });

  it("still re-claims a released record whose stored handoff is valid", async () => {
    const worktree = { worktree_path: PATH, repo_head: "abc123", tree_fingerprint: "fp-1" };
    await redis.hset(CUSTODY_KEYS.records, PATH, JSON.stringify({ state: "released", worktree, handoff: handoff() }));
    const res = expectOk(await store.claim(claim({ session_id: "session-b" })));
    expect(res.record).toMatchObject({ state: "held", mode: "graceful_handoff", handoff: handoff() });
  });

  it("enforces the conditional-write precondition in the Lua script", async () => {
    const held = {
      state: "held",
      worktree: {
        worktree_path: PATH,
        repo_head: "abc123",
        tree_fingerprint: "fp-1",
      },
      custodian: { actor_name: "agent-a", session_id: "session-a" },
      lease_expires_at: "2030-01-01T01:00:00.000Z",
    };
    await redis.hset(CUSTODY_KEYS.records, PATH, JSON.stringify(held));

    const lua = readFileSync(
      fileURLToPath(
        new URL("../src/mcp-server/lua/custody-conditional-set.lua", import.meta.url)
      ),
      "utf-8"
    );

    // State mismatch: no write.
    const stateMismatch = await redis.eval(
      lua,
      1,
      CUSTODY_KEYS.records,
      PATH,
      JSON.stringify({ state: "released" }),
      "released",
      ""
    );
    expect(stateMismatch).toBe(0);
    expect(JSON.parse((await redis.hget(CUSTODY_KEYS.records, PATH))!).state).toBe(
      "held"
    );

    // Custodian session mismatch (release path): no write.
    const custodianMismatch = await redis.eval(
      lua,
      1,
      CUSTODY_KEYS.records,
      PATH,
      JSON.stringify({ state: "released", handoff: {} }),
      "held",
      "session-other"
    );
    expect(custodianMismatch).toBe(0);
    expect(JSON.parse((await redis.hget(CUSTODY_KEYS.records, PATH))!).state).toBe(
      "held"
    );

    // Correct state + custodian session: write succeeds.
    const okWrite = await redis.eval(
      lua,
      1,
      CUSTODY_KEYS.records,
      PATH,
      JSON.stringify({ state: "released", handoff: {} }),
      "held",
      "session-a"
    );
    expect(okWrite).toBe(1);
    expect(JSON.parse((await redis.hget(CUSTODY_KEYS.records, PATH))!).state).toBe(
      "released"
    );

    // ABSENT precondition against an existing field fails.
    const absentAgainstExisting = await redis.eval(
      lua,
      1,
      CUSTODY_KEYS.records,
      PATH,
      JSON.stringify({ state: "unowned" }),
      "ABSENT",
      ""
    );
    expect(absentAgainstExisting).toBe(0);

    // ABSENT precondition against a missing field succeeds.
    const absentOk = await redis.eval(
      lua,
      1,
      CUSTODY_KEYS.records,
      "/fresh/path",
      JSON.stringify({ state: "unowned" }),
      "ABSENT",
      ""
    );
    expect(absentOk).toBe(1);
  });
});