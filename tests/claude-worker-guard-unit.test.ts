/** Worker-mode predicate (f-1965) and the pure AskUserQuestion guard decision (f-1915). */
import { spawnSync } from "node:child_process";
import { describe, expect, it } from "vitest";
import { decideAsk, renderAsk } from "../src/claude/hook.js";
import type { Identity, ProbeResult } from "../src/claude/inbox-probe.js";
import { isExplicitWorker, isWorkerSession } from "../src/claude/worker-mode.js";

const ok = (unclaimed: number, mine: number | null = 0, latest: string | null = "1-0", expired = 0): ProbeResult =>
  ({ ok: true, counts: { unclaimed, claimed_by_session: mine, claimed_by_others: expired, expired_by_others: expired, latest_event_id: latest } });
const recorded: Identity = { agent: "claude-held", gptqueue_session_id: "s-1", source: "session_record", note: null, claude: { pid: 4242, start: "100" } };
const derived: Identity = { agent: "claude-guess", gptqueue_session_id: null, source: "derived", note: "no session record", claude: null };

describe("isWorkerSession truth table", () => {
  const cases: ReadonlyArray<[NodeJS.ProcessEnv, boolean]> = [
    [{}, false],
    [{ CLAUDE_CODE_ENTRYPOINT: "cli" }, false],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli" }, true],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQ_AGENT_NAME: "" }, true],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQ_AGENT_NAME: "named" }, false],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "0" }, false],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "1" }, true],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "yes" }, true],
    [{ GPTQUEUE_WORKER: "1" }, true],
    [{ GPTQUEUE_WORKER: "1", GPTQ_AGENT_NAME: "named" }, true],
    [{ GPTQUEUE_WORKER: "1", CLAUDE_CODE_ENTRYPOINT: "cli" }, true],
    [{ GPTQUEUE_WORKER: "0" }, false],
    [{ GPTQUEUE_WORKER: "true", CLAUDE_CODE_ENTRYPOINT: "cli" }, false],
  ];
  it.each(cases)("%j -> %s", (env, expected) => { expect(isWorkerSession(env)).toBe(expected); });
});

describe("isExplicitWorker truth table (sidecar: explicit GPTQUEUE_WORKER only)", () => {
  const cases: ReadonlyArray<[NodeJS.ProcessEnv, boolean]> = [
    [{}, false],
    [{ GPTQUEUE_WORKER: "1" }, true],
    [{ GPTQUEUE_WORKER: "1", GPTQ_AGENT_NAME: "named" }, true],
    [{ GPTQUEUE_WORKER: "0" }, false],
    [{ GPTQUEUE_WORKER: "" }, false],
    [{ GPTQUEUE_WORKER: "yes" }, false],
    [{ GPTQUEUE_WORKER: "true" }, false],
    // Not a Claude print-mode child: the sdk-cli inference does not apply.
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli" }, false],
    [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "0" }, false],
  ];
  it.each(cases)("%j -> %s", (env, expected) => { expect(isExplicitWorker(env)).toBe(expected); });
});

describe("test hermeticity (vitest.config.ts)", () => {
  // A wrapper exporting GPTQUEUE_WORKER=1, or a `claude -p` caller
  // (CLAUDE_CODE_ENTRYPOINT=sdk-cli), must not put children that tests spawn
  // with the inherited env into headless worker mode.
  it("drops the worker-mode env, so children spawned with the inherited env are not headless workers", () => {
    expect(process.env.GPTQUEUE_WORKER).toBeUndefined();
    expect(process.env.CLAUDE_CODE_ENTRYPOINT).toBeUndefined();
    const child = spawnSync(process.execPath, ["-e", "process.stdout.write(JSON.stringify([process.env.GPTQUEUE_WORKER ?? null, process.env.CLAUDE_CODE_ENTRYPOINT ?? null]))"],
      { env: process.env, encoding: "utf8" });
    expect(JSON.parse(child.stdout)).toEqual([null, null]);
    expect(isWorkerSession(process.env)).toBe(false);
  });
});

describe("decideAsk", () => {
  it("passes silently with no actionable mail and clears the record", () => {
    expect(decideAsk({ identity: recorded, probe: ok(0), prior: null })).toEqual({ kind: "pass", systemMessage: null, clearRecord: true });
    expect(renderAsk(decideAsk({ identity: recorded, probe: ok(0, 0, "9-0"), prior: { event_id: "9-0" } }))).toBeNull();
  });
  it("denies once per latest event while mail is actionable, naming agent and counts", () => {
    const first = decideAsk({ identity: recorded, probe: ok(2, 1, "5-0"), prior: null });
    expect(first.kind).toBe("deny");
    if (first.kind !== "deny") throw new Error("unreachable");
    expect(first.record).toEqual({ event_id: "5-0" });
    expect(first.reason).toContain('agent "claude-held"');
    expect(first.reason).toContain("2 unclaimed; claimed by this session but not acknowledged: 1");
    expect(first.reason).toMatch(/AskUserQuestion blocks this session until the operator answers/);
    expect(first.reason).toMatch(/claim_tasks/);
    expect(first.reason).toMatch(/does not repeat/);
    expect(JSON.parse(renderAsk(first)!)).toEqual({ hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: first.reason } });
    const retry = decideAsk({ identity: recorded, probe: ok(2, 1, "5-0"), prior: { event_id: "5-0" } });
    expect(retry).toEqual({ kind: "pass", systemMessage: null, clearRecord: false });
    expect(decideAsk({ identity: recorded, probe: ok(3, 1, "6-0"), prior: { event_id: "5-0" } }).kind).toBe("deny");
  });
  it("counts expired claims of other sessions as actionable", () => {
    expect(decideAsk({ identity: recorded, probe: ok(0, 0, "1-0", 1), prior: null }).kind).toBe("deny");
  });
  it("a null latest event still denies once", () => {
    expect(decideAsk({ identity: recorded, probe: ok(0, 1, null), prior: null }).kind).toBe("deny");
    expect(decideAsk({ identity: recorded, probe: ok(0, 1, null), prior: { event_id: null } }).kind).toBe("pass");
  });
  it("never denies an unconfirmed identity or on a failed probe", () => {
    expect(decideAsk({ identity: derived, probe: ok(5), prior: null })).toEqual({ kind: "pass", systemMessage: null, clearRecord: false });
    const failed = decideAsk({ identity: recorded, probe: { ok: false, code: "redis_unavailable", message: "down" }, prior: null });
    expect(failed.kind).toBe("pass");
    const out = JSON.parse(renderAsk(failed)!) as Record<string, unknown>;
    expect(out.systemMessage).toMatch(/FAILED \(redis_unavailable\)/);
    expect(out).not.toHaveProperty("hookSpecificOutput");
  });
});
