import { describe, expect, it } from "vitest";
import { receiveMessageSchema } from "../src/mcp-server/tools/receive-message.js";
import { sendMessageSchema } from "../src/mcp-server/tools/send-message.js";
import { stableToolError } from "../src/mcp-server/tool-result.js";
import { GPTQUEUE_INSTRUCTIONS } from "../src/transports/setup-tools.js";
import { custodyClaimSchema } from "../src/mcp-server/tools/custody-claim.js";
import { custodyReleaseSchema } from "../src/mcp-server/tools/custody-release.js";
import { custodyStatusSchema } from "../src/mcp-server/tools/custody-status.js";

describe("MCP contract hardening", () => {
  it("bounds blocking receive timeouts", () => {
    expect(receiveMessageSchema.safeParse({ timeout: 0 }).success).toBe(true);
    expect(receiveMessageSchema.safeParse({ timeout: 60 }).success).toBe(true);
    expect(receiveMessageSchema.safeParse({ timeout: -1 }).success).toBe(false);
    expect(receiveMessageSchema.safeParse({ timeout: 61 }).success).toBe(false);
    expect(receiveMessageSchema.safeParse({ timeout: 1.5 }).success).toBe(false);
  });

  it("bounds retry idempotency keys", () => {
    const base = { to: "agent", content: "hello" };
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "retry-1" }).success).toBe(true);
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "" }).success).toBe(false);
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "x".repeat(129) }).success).toBe(false);
  });

  it("returns stable unavailable errors as structured content", () => {
    const result = stableToolError(new Error("Session abc not found in Redis."));
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      status: "error",
      error: { code: "SESSION_UNAVAILABLE", retryable: false },
    });
  });

  it("maps the session-bound prologue lost-session error to SESSION_UNAVAILABLE", () => {
    // M6: bindSession throws this exact message when a caller is registered but
    // has no bound session; it must map to SESSION_UNAVAILABLE, not
    // GPTQUEUE_ERROR.
    const result = stableToolError(
      new Error(
        "Session binding not found. Call register_agent first with a name and retain the session_id."
      )
    );
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      status: "error",
      error: { code: "SESSION_UNAVAILABLE", retryable: false },
    });
  });

  it("places coordination-plane policy in server instructions", () => {
    expect(GPTQUEUE_INSTRUCTIONS).toContain("instead of native");
    expect(GPTQUEUE_INSTRUCTIONS).toContain("idempotency_key");
  });

  it("bounds custody_claim lease_seconds", () => {
    const base = {
      worktree_path: "/w",
      repo_head: "abc",
      tree_fingerprint: "fp",
    };
    expect(
      custodyClaimSchema.safeParse({ ...base, lease_seconds: 1 }).success
    ).toBe(true);
    expect(
      custodyClaimSchema.safeParse({ ...base, lease_seconds: 86400 }).success
    ).toBe(true);
    expect(
      custodyClaimSchema.safeParse({ ...base, lease_seconds: 0 }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...base, lease_seconds: 86401 }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...base, lease_seconds: 1.5 }).success
    ).toBe(false);
  });

  it("rejects empty worktree identity fields for custody_claim", () => {
    const valid = {
      worktree_path: "/w",
      repo_head: "abc",
      tree_fingerprint: "fp",
      lease_seconds: 60,
    };
    expect(
      custodyClaimSchema.safeParse({ ...valid, worktree_path: "" }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...valid, repo_head: "" }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...valid, tree_fingerprint: "" }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...valid, inventory: ["ok", ""] }).success
    ).toBe(false);
  });

  it("rejects invalid custody_claim inventory entries", () => {
    const base = {
      worktree_path: "/w",
      repo_head: "abc",
      tree_fingerprint: "fp",
      lease_seconds: 60,
    };
    expect(custodyClaimSchema.safeParse(base).success).toBe(true);
    expect(
      custodyClaimSchema.safeParse({ ...base, inventory: ["ok"] }).success
    ).toBe(true);
    expect(
      custodyClaimSchema.safeParse({ ...base, inventory: [""] }).success
    ).toBe(false);
    expect(
      custodyClaimSchema.safeParse({ ...base, inventory: [5] }).success
    ).toBe(false);
  });

  it("defaults custody_release handoff fields", () => {
    const parsed = custodyReleaseSchema.safeParse({
      worktree_path: "/w",
      repo_head: "abc",
      tracked_tree_state: "clean",
      unfinished_work: "finish up",
      next_step: "commit",
    });
    expect(parsed.success).toBe(true);
    if (parsed.success) {
      expect(parsed.data.authored_by).toBe("origin");
      expect(parsed.data.untracked_inventory).toEqual([]);
      expect(parsed.data.hazards).toEqual([]);
    }
  });

  it("accepts a dirty custody_release with an inventory at the schema level", () => {
    const parsed = custodyReleaseSchema.safeParse({
      worktree_path: "/w",
      repo_head: "abc",
      tracked_tree_state: "dirty",
      untracked_inventory: ["wip.sh"],
      unfinished_work: "finish up",
      next_step: "commit",
    });
    expect(parsed.success).toBe(true);
  });

  it("rejects bad custody_release enums and empty structural fields", () => {
    const base = {
      worktree_path: "/w",
      repo_head: "abc",
      tracked_tree_state: "clean",
      unfinished_work: "x",
      next_step: "y",
    };
    expect(
      custodyReleaseSchema.safeParse({
        ...base,
        tracked_tree_state: "partially_dirty",
      }).success
    ).toBe(false);
    expect(
      custodyReleaseSchema.safeParse({
        ...base,
        authored_by: "god",
      }).success
    ).toBe(false);
    expect(
      custodyReleaseSchema.safeParse({ ...base, repo_head: "" }).success
    ).toBe(false);
    expect(
      custodyReleaseSchema.safeParse({ ...base, unfinished_work: "" }).success
    ).toBe(false);
    expect(
      custodyReleaseSchema.safeParse({ ...base, next_step: "" }).success
    ).toBe(false);
  });

  it("keeps custody_status.worktree_path optional", () => {
    expect(custodyStatusSchema.safeParse({}).success).toBe(true);
    expect(
      custodyStatusSchema.safeParse({ worktree_path: "/w" }).success
    ).toBe(true);
    expect(
      custodyStatusSchema.safeParse({ worktree_path: "" }).success
    ).toBe(false);
  });
});
