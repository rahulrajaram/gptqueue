/**
 * AT1 startup binding: a launcher-declared seat is held by the new session,
 * and its job card reaches startup context, with zero model tool calls.
 * The end-to-end case spawns the real built gptqueue-claude-hook.
 */
import { testRedisUrl } from "../helpers/test-redis.js";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import {
  createMemorySeatStore,
  createNodeProcessProbe,
  createRedisSeatStore,
  createSeatService,
  parseSeatFile,
  readProcessStart,
  SEAT_CARD_MAX_CHARS,
  SEAT_ENV,
  SEAT_FILE_ENV,
  startSeat,
  type SeatStartupInput,
} from "../../src/agent-tracking/index.js";
import { SEAT_KEY_PREFIX } from "../../src/integrations/agent-tracking.js";
import { decideHook } from "../../src/claude/hook.js";
import { publishSessionRecord, readProcess } from "../../src/claude/session-record.js";
import { assertPrivateTestRedis } from "../helpers/redis-test-utils.js";
import { BINDING_TOOL_INSTRUCTION, seat, seatFileJson } from "./fixtures.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "at01-startup-binding");
const BIN = resolve(__dirname, "..", "..", "bin");
const SID = "44444444-4444-4444-8444-444444444444";

let home = "";
let env: NodeJS.ProcessEnv = {};
let redis: Redis;
const seatIds: string[] = [];
const children: ChildProcess[] = [];

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-seat-at1-"));
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off", GPTQ_CLAUDE_WATCH: "off" };
  redis = new Redis(REDIS_URL);
});
afterEach(() => { for (const c of children.splice(0)) c.kill("SIGKILL"); });
afterAll(async () => {
  for (const id of seatIds) await redis.del(`${SEAT_KEY_PREFIX}tenure:${id}`, `${SEAT_KEY_PREFIX}history:${id}`);
  redis.disconnect();
  publishSessionRecord(readProcess(process.pid)!, null, null, env);
  await rm(home, { recursive: true, force: true });
});

const self = () => ({ pid: process.pid, start: readProcessStart(process.pid)! });

const libraryInput = (overrides: Partial<SeatStartupInput> = {}): SeatStartupInput => {
  const probe = createNodeProcessProbe();
  const file = parseSeatFile(seatFileJson());
  return {
    env: { [SEAT_ENV]: "acme.supervisor" },
    runtime: "claude",
    session_id: SID,
    cwd: "/workspace/project-a",
    process: self(),
    service: createSeatService({ store: createMemorySeatStore(), clock: { now: () => new Date() }, probe }),
    probe,
    loadFile: () => file,
    ...overrides,
  };
};

describe("AT1 startup binding (library)", () => {
  it("binds the launcher-declared seat to the starting process and returns its job card", async () => {
    const input = libraryInput();
    const started = await startSeat(input);
    expect(started.kind).toBe("held");
    if (started.kind !== "held") return;
    expect(started.tenure).toMatchObject({ seat_id: "acme.supervisor", state: "held", epoch: 1 });
    expect(started.tenure.holder).toMatchObject({ runtime: "claude", session_id: SID, pid: process.pid });
    expect(started.context).toContain(seat().name);
    expect(started.context).toContain(seat().description);
    expect(started.context).toContain("epoch 1");
    for (const capability of seat().capabilities) expect(started.context).toContain(capability);
    expect(started.context.length).toBeLessThanOrEqual(SEAT_CARD_MAX_CHARS);
    const history = await input.service.history("acme.supervisor");
    expect(history.ok && history.value.map((e) => e.kind)).toEqual(["bind"]);
  });

  it("asks the model for no tool call: the card names no binding or registration step", async () => {
    const started = await startSeat(libraryInput());
    expect(started.kind).toBe("held");
    if (started.kind === "held") expect(started.context).not.toMatch(BINDING_TOOL_INSTRUCTION);
  });

  it("does nothing when the launcher names no seat", async () => {
    expect(await startSeat(libraryInput({ env: {} }))).toEqual({ kind: "none" });
    expect(await startSeat(libraryInput({ env: { [SEAT_ENV]: "" } }))).toEqual({ kind: "none" });
  });

  it("re-running startup in the same process keeps the same tenure", async () => {
    const input = libraryInput();
    await startSeat(input);
    const again = await startSeat(input);
    expect(again.kind === "held" && again.tenure.epoch).toBe(1);
  });

  it("puts a visible refusal, not a card, in context when the seat is held elsewhere", async () => {
    const input = libraryInput();
    const sleeper = spawn("sleep", ["60"]);
    children.push(sleeper);
    const other = { pid: sleeper.pid!, start: readProcessStart(sleeper.pid!)! };
    const first = await startSeat({ ...input, process: other, session_id: randomUUID() });
    expect(first.kind).toBe("held");
    const second = await startSeat(input);
    expect(second.kind).toBe("refused");
    if (second.kind !== "refused") return;
    expect(second.error.code).toBe("seat_held");
    expect(second.context).toMatch(/NOT/);
    expect(second.context).toContain("seat_held");
    expect(second.context).not.toContain(seat().description);
  });
});

describe("AT1 Claude SessionStart hook composes the seat context", () => {
  const probe = { ok: true as const, counts: { unclaimed: 0, claimed_by_session: 0, claimed_by_others: 0, expired_by_others: 0, latest_event_id: null } };
  const base = {
    event: "session-start" as const, probe, stopHookActive: false, prior: null, watcherRunning: false,
    watchPolicy: "off" as const, watchCommand: "peek",
  };
  it("appends the seat text after the inbox context, for confirmed and unconfirmed identities", () => {
    const confirmed = { agent: "claude-x", gptqueue_session_id: "sid", source: "session_record" as const, note: null, claude: null };
    const unconfirmed = { agent: "claude-y", gptqueue_session_id: null, source: "derived" as const, note: "n", claude: null };
    for (const identity of [confirmed, unconfirmed]) {
      const without = decideHook({ ...base, identity });
      const withSeat = decideHook({ ...base, identity, seat: "SEAT-CARD-TEXT" });
      expect(without.kind).toBe("context");
      expect(withSeat.kind).toBe("context");
      if (without.kind === "context" && withSeat.kind === "context") {
        expect(withSeat.text).toBe(`${without.text}\n\nSEAT-CARD-TEXT`);
      }
    }
  });
});

describe("AT1 end to end through the real gptqueue-claude-hook", { timeout: 120_000 }, () => {
  const hook = (extra: NodeJS.ProcessEnv) =>
    new Promise<{ stdout: string; code: number | null }>((done) => {
      const child = spawn(join(BIN, "gptqueue-claude-hook"), ["session-start"], { env: { ...env, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
      let stdout = "";
      child.stdout.on("data", (d) => { stdout += d; });
      child.on("close", (code) => done({ stdout: stdout.trim(), code }));
      child.stdin.end(JSON.stringify({ session_id: SID, cwd: "/workspace/project-a/sub" }));
    });

  const seatEnv = async (seatId: string): Promise<NodeJS.ProcessEnv> => {
    seatIds.push(seatId);
    const path = join(home, `${seatId}.json`);
    await writeFile(path, seatFileJson([seat({ seat_id: seatId })]));
    return { [SEAT_ENV]: seatId, [SEAT_FILE_ENV]: path };
  };

  it("binds the seat to this Claude process in Redis and injects the card, with no tool call", async () => {
    // This test process plays the Claude process; the spawned hook is its child.
    publishSessionRecord(readProcess(process.pid)!, "claude-at1", "sid-at1", env);
    const seatId = `at1.${randomUUID().slice(0, 8)}`;
    const result = await hook(await seatEnv(seatId));
    expect(result.code).toBe(0);
    const context = JSON.parse(result.stdout).hookSpecificOutput.additionalContext as string;
    // The inbox sentence before the seat text is pinned byte-for-byte by AT12; check what the seat adds.
    const seatText = context.slice(context.indexOf("\n\ngptqueue seat:") + 2);
    expect(context.indexOf("\n\ngptqueue seat:")).toBeGreaterThan(0);
    expect(seatText).toContain(seat().name);
    expect(seatText).toContain("epoch 1");
    expect(seatText).not.toMatch(BINDING_TOOL_INSTRUCTION);

    const store = createRedisSeatStore(redis, { prefix: SEAT_KEY_PREFIX });
    const read = await store.read(seatId);
    expect(read.ok && read.stored?.tenure).toMatchObject({
      state: "held", epoch: 1, holder: { runtime: "claude", session_id: SID, pid: process.pid },
    });

    const again = await hook(await seatEnv(seatId));
    expect(JSON.parse(again.stdout).hookSpecificOutput.additionalContext).toContain("epoch 1");
  });

  it("reports a seat outside the launcher policy visibly and binds nothing", async () => {
    publishSessionRecord(readProcess(process.pid)!, "claude-at1", "sid-at1", env);
    const seatId = `at1.${randomUUID().slice(0, 8)}`;
    const path = join(home, `${seatId}-policy.json`);
    seatIds.push(seatId);
    await writeFile(path, seatFileJson([seat({ seat_id: seatId, policy: { runtimes: ["codex"], cwd_roots: ["/workspace/project-a"] } })]));
    const result = await hook({ [SEAT_ENV]: seatId, [SEAT_FILE_ENV]: path });
    const context = JSON.parse(result.stdout).hookSpecificOutput.additionalContext as string;
    expect(context).toContain("runtime_not_allowed");
    expect(await redis.exists(`${SEAT_KEY_PREFIX}tenure:${seatId}`)).toBe(0);
  });
});
