import { describe, expect, it } from "vitest";
import { verificationChecks } from "../scripts/wrapper-verification-checks.mjs";

const baseline = () => ({
  failure: null, proof: { sent_id: "message", reply_id: "reply" }, exits: [{ code: 0 }, { code: 0 }],
  remaining: [], retryId: "message", replyRetryId: "reply", newFiles: ["/workspace/run/cache.json"], piRuntime: "/workspace/run/",
  before: { pi_cache: { sha256: "cache", mtime_ms: 1 }, codex_config: { sha256: "config", mtime_ms: 1 } },
  after: { pi_cache: { sha256: "cache", mtime_ms: 1 }, codex_config: { sha256: "config", mtime_ms: 2 } },
});
const passed = (input: ReturnType<typeof baseline>) => Object.values(verificationChecks(input)).every(Boolean);

describe("live wrapper evidence acceptance", () => {
  it("records Codex metadata churn without confusing it with a content change", () => {
    expect(passed(baseline())).toBe(true);
  });
  it.each([
    ["missing exchange", { proof: null }],
    ["failed child", { exits: [{ code: 0 }, { code: 1 }] }],
    ["timeout despite zero exits", { failure: "timeout" }],
    ["Redis residue", { remaining: ["gptq:orphan"] }],
    ["wrong retry evidence", { retryId: "other-message" }],
    ["wrong reply retry evidence", { replyRetryId: "other-reply" }],
    ["sibling path escape", { newFiles: ["/workspace/run-other/cache.json"] }],
    ["no adapter state evidence", { newFiles: [] }],
  ])("rejects %s", (_name, delta) => {
    expect(passed({ ...baseline(), ...delta } as ReturnType<typeof baseline>)).toBe(false);
  });
  it("rejects Pi cache metadata changes even if its bytes remain the same", () => {
    const input = baseline();
    expect(passed({ ...input, after: { ...input.after, pi_cache: { sha256: "cache", mtime_ms: 2 } } })).toBe(false);
  });
  it("rejects changed Codex configuration contents", () => {
    const input = baseline();
    expect(passed({ ...input, after: { ...input.after, codex_config: { sha256: "changed", mtime_ms: 2 } } })).toBe(false);
  });
});
