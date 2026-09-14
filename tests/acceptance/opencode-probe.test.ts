/** Opt-in OpenCode mechanics probe; it never starts GPTQueue or flushes Redis. */
import { describe, expect, it, beforeAll, afterAll, vi } from "vitest";
import { randomUUID, createHash } from "node:crypto";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import {
  model, redisUrl, mcpEntry, opencodeBin, modelsPath, timeoutMs, repo, newConfigHome, newWorkDir,
  runOpenCode, type RunResult,
} from "./opencode-support.js";

const enabled = process.env.GPTQUEUE_OPENCODE_TESTS === "1";
const artifactRoot = join(repo, ".gptqueue/acceptance/20260912-evaluation/opencode");
const runId = randomUUID();
vi.setConfig({ testTimeout: timeoutMs * 3, hookTimeout: 30_000 });

const agentName = (route: string) => `oc-${route}-${Date.now()}-${randomUUID().slice(0, 8)}`;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function observe(redis: Redis, names: string[], waitMs = 20_000) {
  const end = Date.now() + waitMs;
  while (Date.now() < end) {
    const rows = await Promise.all(names.map(async (agent) => ({
      agent,
      registered: (await redis.hexists(SESSION_KEYS.registry, agent)) === 1,
      sessions: (await redis.smembers(SESSION_KEYS.agentSessions(agent))).length,
      session_hashes: (await redis.smembers(SESSION_KEYS.agentSessions(agent))).map((id) => createHash("sha256").update(id).digest("hex")),
    })));
    if (rows.every((row) => row.registered)) return rows;
    await sleep(500);
  }
  return Promise.all(names.map(async (agent) => ({
    agent,
    registered: (await redis.hexists(SESSION_KEYS.registry, agent)) === 1,
    sessions: (await redis.smembers(SESSION_KEYS.agentSessions(agent))).length,
    session_hashes: (await redis.smembers(SESSION_KEYS.agentSessions(agent))).map((id) => createHash("sha256").update(id).digest("hex")),
  })));
}

async function removeAgents(redis: Redis, names: string[]) {
  for (const agent of names) {
    const sessions = await redis.smembers(SESSION_KEYS.agentSessions(agent));
    await redis.hdel(SESSION_KEYS.registry, agent);
    await redis.del(
      SESSION_KEYS.agentSessions(agent), SESSION_KEYS.queue(agent),
      SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent),
      ...sessions.flatMap((session) => [SESSION_KEYS.session(session), SESSION_KEYS.lease(session)]),
    );
  }
}

const toolSeen = (run: RunResult) => run.traces.some((trace) =>
  trace.name.endsWith("register_agent") && trace.status === "completed" && trace.hasOutput && !trace.error);

function evidence(run: RunResult) {
  return { code: run.code, signal: run.signal, timed_out: run.timedOut, process_error: run.processError, json_events: run.jsonEvents, session_lineage_events: run.sessionLineageEvents, session_lineage_hashes: run.sessionLineageHashes, stderr: run.stderr, tool_calls: run.traces, events: run.events };
}

function trace(run: RunResult, suffix: string) {
  return run.traces.find((item) => item.name.endsWith(suffix) && item.status === "completed" && !item.error);
}

describe.skipIf(!enabled)("OpenCode MCP acceptance probe", () => {
  let redis: Redis;
  const created: string[] = [];

  beforeAll(async () => {
    if (!existsSync(opencodeBin)) throw new Error(`OpenCode binary unavailable: ${opencodeBin}`);
    if (!existsSync(mcpEntry)) throw new Error(`Build artifact unavailable: ${mcpEntry}`);
    if (!existsSync(modelsPath)) throw new Error(`Cached OpenCode model catalog unavailable: ${modelsPath}`);
    redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
    await redis.ping();
  });
  afterAll(async () => {
    try {
      if (created.length) await removeAgents(redis, created);
    } finally {
      await redis?.quit();
    }
  });

  it("records direct identity and native Task child mechanics", async () => {
    const configHome = newConfigHome();
    const directWork = newWorkDir();
    const nativeWork = newWorkDir();
    const direct = agentName("direct");
    const parent = agentName("parent");
    const child = agentName("child");
    created.push(direct, parent, child);
    const nonce = randomUUID();
    const idempotency = `oc-idem-${nonce}`;
    const directPrompt = [
      "Assisted mechanics probe. Use only the configured gptqueue MCP server; no shell, files, skills, or other tools.",
      `Call register_agent exactly once with name '${direct}', role 'both', description 'OpenCode direct ${nonce}'.`,
      `Then call send_message exactly once to '${direct}' with content '${nonce}' and idempotency_key '${idempotency}', then call receive_message with timeout 5. Finally call list_agents once. Report original tool results; do not infer success from prose.`,
    ].join(" ");
    const nativePrompt = [
      "Assisted native-child mechanics probe. Use only the configured gptqueue MCP server; no shell, files, another OpenCode process, or session fork.",
      `Call register_agent exactly once with name '${parent}', role 'both', description 'OpenCode native parent ${nonce}'.`,
      "Then invoke the native Task tool exactly once with subagent_type='general'. Its child prompt must call register_agent exactly once with",
      `name '${child}', role 'both', description 'OpenCode native child ${nonce}'. Report original MCP tool results, not prose inference.`,
    ].join(" ");
    let directRun!: RunResult;
    let nativeRun!: RunResult;
    try {
      directRun = await runOpenCode(directPrompt, configHome, directWork);
      nativeRun = await runOpenCode(nativePrompt, configHome, nativeWork);
    } finally {
      rmSync(configHome, { recursive: true, force: true });
      rmSync(directWork, { recursive: true, force: true });
      rmSync(nativeWork, { recursive: true, force: true });
    }
    const directObserved = await observe(redis, [direct]);
    const nativeObserved = await observe(redis, [parent, child]);
    const sendTrace = trace(directRun, "send_message");
    const receiveTrace = trace(directRun, "receive_message");
    const communicationText = JSON.stringify([sendTrace?.input, sendTrace?.output, receiveTrace?.input, receiveTrace?.output]);
    const communicationObserved = Boolean(
      sendTrace && receiveTrace && communicationText.includes(nonce) &&
      communicationText.includes(idempotency) && communicationText.includes(direct) &&
      /message[_-]?id|"id"/i.test(communicationText)
    );
    const sendOutput = sendTrace?.output && typeof sendTrace.output === "object" ? sendTrace.output as Record<string, unknown> : undefined;
    const directMessageType = typeof sendOutput?.type === "string" ? sendOutput.type : "defaulttask_implicit";
    const taskObserved = nativeRun.traces.some((item) => item.name === "task" && item.status === "completed" && item.hasOutput && !item.error);
    const parentUsable = nativeObserved[0]?.registered === true && nativeObserved[0].sessions === 1;
    const childUsable = nativeObserved[1]?.registered === true && nativeObserved[1].sessions === 1;
    const distinctSessions = parentUsable && childUsable &&
      nativeObserved[0]!.session_hashes[0] !== nativeObserved[1]!.session_hashes[0];
    // A completed native Task whose child displaced the parent is an observed
    // acceptance failure. "unsupported" is reserved for a route that did not
    // expose enough mechanics to evaluate the child independently.
    const nativeChildStatus = taskObserved && parentUsable && childUsable && distinctSessions && nativeRun.sessionLineageEvents > 0 ? "pass" :
      taskObserved ? "does_not_meet" : "unobserved";
    const artifactDir = join(artifactRoot, runId);
    mkdirSync(artifactDir, { recursive: true });
    writeFileSync(join(artifactDir, "probe.json"), JSON.stringify({
      schema_version: 1, host: "opencode", model, assisted: true,
      config: { source: "OPENCODE_CONFIG_CONTENT", global_config: "disabled", pure: true, mcp: "gptqueue", redis_database: 15 },
      surfaces: { run: "executed", serve: "unsupported_not_executed", acp: "unsupported_not_executed", native_child: nativeChildStatus },
      direct: { requested: direct, tool_result_observed: toolSeen(directRun), communication_observed: communicationObserved, communication_kind: "self_message", send_type: directMessageType, registry: directObserved, run: evidence(directRun) },
      native: { parent_requested: parent, child_requested: child, task_observed: taskObserved, parent_usable: parentUsable, child_usable: childUsable, distinct_sessions: distinctSessions, session_lineage_events: nativeRun.sessionLineageEvents, status_reason: nativeChildStatus === "does_not_meet" ? "native Task completed but parent and child did not remain independently discoverable/usable" : undefined, registry: nativeObserved, run: evidence(nativeRun) },
      prompt_sha256: [directPrompt, nativePrompt].map((text) => createHash("sha256").update(text).digest("hex")),
    }, null, 2) + "\n", { mode: 0o600 });
    expect(directObserved[0]?.registered).toBe(true);
    expect(toolSeen(directRun)).toBe(true);
    expect(communicationObserved).toBe(true);
  });
});
