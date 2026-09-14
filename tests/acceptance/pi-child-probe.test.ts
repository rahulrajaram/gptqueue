/**
 * Opt-in live acceptance probe for Pi's installed native Agent tool.
 *
 * This deliberately uses the Pi SDK and the installed pi-subagents extension;
 * it is not a standalone CLI or a mocked child. It is skipped unless the
 * parent explicitly dispatches it with GPTQUEUE_PI_CHILD_ACCEPTANCE=1.
 */
import { describe, expect, it } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../../src/core/keys.js";
import { createRegisteredPiExtension } from "../../src/registered-shell/pi-extension.js";

const enabled = process.env.GPTQUEUE_PI_CHILD_ACCEPTANCE === "1";
const root = resolve(import.meta.dirname, "../..");
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const piInstalled = "/home/rahul/nodeenv2251-311/lib/node_modules/@earendil-works/pi-coding-agent/dist";
const subagentsSource = "/home/rahul/.pi/agent/git/github.com/rahulrajaram/pi-subagents/src/index.ts";
const subagentsRoot = resolve(dirname(subagentsSource), "..");
const modelProvider = "openrouter";
const modelId = process.env.GPTQUEUE_PI_MODEL ?? "z-ai/glm-5.3-flash";

type Json = Record<string, any>;
type Session = any;
type ToolResult = { value: any; text: string; raw: any };

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const until = async <T>(read: () => Promise<T>, accept: (value: T) => boolean, timeout = 120_000): Promise<T> => {
  const end = Date.now() + timeout;
  let value = await read();
  while (!accept(value) && Date.now() < end) { await sleep(500); value = await read(); }
  if (!accept(value)) throw new Error(`Pi child observation timed out after ${timeout}ms`);
  return value;
};

const toolText = (result: any): string => (result?.content ?? [])
  .filter((item: any) => item?.type === "text").map((item: any) => String(item.text)).join("\n");
const toolValue = (result: any): any => result?.details?.structuredContent ?? (() => {
  const text = toolText(result);
  try { return JSON.parse(text); } catch { return text; }
})();
const call = async (session: Session, name: string, args: Json = {}): Promise<ToolResult> => {
  const definition = session.agent.state.tools.find((item: any) => item.name === name);
  if (!definition) throw new Error(`Pi tool ${name} is unavailable`);
  const raw = await definition.execute(`pi-child-probe-${randomUUID()}`, args, AbortSignal.timeout(300_000));
  if (raw?.isError) throw new Error(`${name} failed: ${toolText(raw)}`);
  return { value: toolValue(raw), text: toolText(raw), raw };
};

const streamRows = async (redis: Redis, key: string): Promise<Json[]> => {
  const rows = await redis.xrange(key, "-", "+");
  return rows.map(([, fields]) => Object.fromEntries(Array.from(
    { length: Math.floor(fields.length / 2) }, (_, index) => [fields[index * 2]!, fields[index * 2 + 1]!],
  )));
};
const taskOf = (value: any): Json | undefined => {
  const task = typeof value === "string" ? JSON.parse(value) : value;
  return task && typeof task === "object" ? task as Json : undefined;
};
const taskReplyingTo = (value: any, id: string): Json | undefined => {
  const tasks = value?.claim?.tasks;
  return Array.isArray(tasks) ? tasks.map(taskOf).find((task) => task?.payload?.in_reply_to === id) : undefined;
};
const childAgentsFrom = (registry: Json, before: ReadonlySet<string>, parent: string, cwd: string): string[] =>
  Object.entries(registry).filter(([name, raw]) => {
    if (before.has(name) || name === parent) return false;
    try {
      const metadata = JSON.parse(String(raw)).metadata;
      return metadata?.client === "pi" && metadata?.working_directory === cwd;
    } catch { return false; }
  }).map(([name]) => name);

const sanitized = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(sanitized);
  if (typeof value === "string") return value.replace(/("?(?:token|secret|password|authorization|api[_-]?key|access[_-]?token|session_id)"?\s*:\s*)"[^"]*"/giu, "$1\"[redacted]\"");
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => !/(?:token|secret|password|authorization|api[_-]?key|access[_-]?token|session_id)$/iu.test(key))
    .filter(([key]) => !/^(?:thinking|reasoning|thinkingSignature|signature)$/iu.test(key))
    .map(([key, item]) => [key, sanitized(item)]));
};
const sessionToolTrace = (session: Session | undefined): unknown[] =>
  (session?.agent?.state?.messages ?? []).filter((item: any) =>
    item?.toolName || item?.content?.some?.((part: any) => part?.type === "toolCall"),
  ).map((item: any) => sanitized({
    role: item.role, toolName: item.toolName, toolCallId: item.toolCallId,
    isError: item.isError, stopReason: item.stopReason, content: item.content,
    details: item.details,
  }));
const toolCallArguments = (session: Session | undefined, name: string): Json[] =>
  (session?.agent?.state?.messages ?? []).flatMap((item: any) =>
    (Array.isArray(item?.content) ? item.content : []).filter((part: any) => item.role === "assistant" && part?.type === "toolCall" && part.name === name)
      .map((part: any) => typeof part.arguments === "string" ? JSON.parse(part.arguments) : part.arguments),
  );

const cleanupAgent = async (redis: Redis, name: string | undefined): Promise<void> => {
  if (!name) return;
  const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
  const claims = await redis.zrange(CLAIM_KEYS.index(name), 0, -1);
  await redis.hdel(SESSION_KEYS.registry, name);
  await redis.del(
    SESSION_KEYS.agentSessions(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name),
    SESSION_KEYS.heartbeat(name), CLAIM_KEYS.index(name), `gptq:inbox-trace:${name}`,
    `gptq:inbox-events:${name}`, `gptq:runtime-binding:${name}`,
    ...sessions.flatMap((session) => [SESSION_KEYS.session(session), SESSION_KEYS.lease(session)]),
  );
  if (claims.length) await redis.hdel(CLAIM_KEYS.claims, ...claims);
};

describe.skipIf(!enabled)("Pi native Agent child GPTQueue acceptance", () => {
  it("creates a distinct native child that registers, consumes, replies, and acknowledges", async () => {
    const runId = randomUUID();
    const artifactDir = join(root, ".gptqueue/acceptance/20260912-evaluation/pi-child", runId);
    const profile = join(artifactDir, "profile");
    const cwd = join(artifactDir, "parent");
    const receiptPath = join(artifactDir, "receipt.json");
    const receipt: Json = {
      schema_version: 1, route: "pi-sdk-native-agent-child", database: 15, run_id: runId,
      model: { provider: modelProvider, id: modelId }, execution: { status: "not_run" }, passed: false,
      identities: {}, native_agent: {}, parent_tool_trace: [], child_tool_trace: [],
    };
    await mkdir(artifactDir, { recursive: true });
    await mkdir(join(profile, "extensions"), { recursive: true });
    await mkdir(cwd, { recursive: true });
    const priorAgentDir = process.env.PI_CODING_AGENT_DIR;
    const priorCwd = process.cwd();
    const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
    let parent: Session | undefined;
    let parentAgent: string | undefined;
    let childAgent: string | undefined;
    let childToolId: string | undefined;
    const extensionErrors: string[] = [];
    try {
      // The registered-shell sidecar inherits process.cwd(). Keep it aligned
      // with the SDK session's declared cwd so identity binding is exact.
      process.chdir(cwd);
      // The child loader must discover exactly this owned extension after the
      // parent's normal model runtime has already been initialized.
      await writeFile(join(profile, "extensions", "gptqueue.ts"),
        `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(root, "dist/registered-shell/pi-extension.js")).href)};\n` +
        `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl, nodePath: process.execPath, sidecarPath: join(root, "bin/gptqueue-session") })});\n`,
        { mode: 0o600 });

      const { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } =
        await import(pathToFileURL(join(piInstalled, "index.js")).href);
      const modelRuntime = await ModelRuntime.create({ allowModelNetwork: false });
      const model = modelRuntime.getModel(modelProvider, modelId);
      expect(model).toBeTruthy();
      expect(modelRuntime.hasConfiguredAuth(modelProvider)).toBe(true);

      // This is intentionally after ModelRuntime.create. It is restored even
      // when setup or the native child fails, so global Pi state is untouched.
      process.env.PI_CODING_AGENT_DIR = profile;
      // The installed extension is source TypeScript and its peer packages are
      // installed with the Pi distribution, not in this repository's module
      // graph. Jiti is already shipped by Pi; explicit aliases keep this
      // loader isolated without mutating the extension checkout or globals.
      const { createJiti } = await import(pathToFileURL(join(piInstalled, "../node_modules/jiti/lib/jiti.mjs")).href);
      const extensionLoader = createJiti(import.meta.url, {
        interopDefault: true,
        alias: {
          "@earendil-works/pi-coding-agent": join(piInstalled, "index.js"),
          "@earendil-works/pi-tui": "/home/rahul/nodeenv2251-311/lib/node_modules/@earendil-works/pi-tui/dist/index.js",
          "@sinclair/typebox": join(subagentsRoot, "node_modules/@sinclair/typebox/build/cjs/index.js"),
        },
      });
      const installedSubagentsModule = await extensionLoader.import(subagentsSource) as any;
      const installedSubagents = installedSubagentsModule.default ?? installedSubagentsModule;
      const profileExtension = async (pi: any) => {
        pi.on("session_start", () => { process.env.PI_CODING_AGENT_DIR = profile; });
      };
      const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
      const loader = new DefaultResourceLoader({
        cwd, agentDir: profile, settingsManager, noExtensions: true, noSkills: true,
        noPromptTemplates: true, noThemes: true, noContextFiles: true,
        systemPrompt: "You are an isolated Pi parent used only for a native child acceptance probe. When GPTQueue activates an inbox task, call claim_tasks, inspect the exact task, and acknowledge_tasks with that claim_id. For a correlated child result, do not send a reply; report PARENT_RECEIVED after acknowledging. Never use receive_message.",
        extensionFactories: [
          createRegisteredPiExtension({ redisUrl, nodePath: process.execPath, sidecarPath: join(root, "bin/gptqueue-session") }),
          installedSubagents, profileExtension,
        ],
      });
      await loader.reload();
      expect(loader.getExtensions().errors).toEqual([]);
      ({ session: parent } = await createAgentSession({
        cwd, agentDir: profile, modelRuntime, model, thinkingLevel: "low", settingsManager,
        sessionManager: SessionManager.inMemory(cwd), resourceLoader: loader, noTools: "builtin",
      }));
      await parent.bindExtensions({ onError: (error: unknown) => { extensionErrors.push(String(error)); } });
      expect(extensionErrors).toEqual([]);
      const status = (await call(parent, "get_runtime_status")).value;
      expect(status.activation_ready).toBe(true);
      parentAgent = String(status.agent);
      receipt.identities.parent = { agent: parentAgent, runtime: status.runtime, cwd };
      const before = new Set(Object.keys(await redis.hgetall(SESSION_KEYS.registry)));

      const marker = `native-child-${randomUUID()}`;
      const childPrompt = [
        "You are the actual native Pi child. Use only the available GPTQueue MCP tools; do not use shell, files, or other extensions.",
        "Call get_runtime_status once, then repeatedly call claim_tasks with max_batch=1 until a task arrives.",
        `When the task content is exactly ${marker}, send one result to task.from with content ${marker}-reply, type result, in_reply_to equal to task.id, and idempotency_key equal to task.id.`,
        "Acknowledge that exact claim with acknowledge_tasks, then report NATIVE_CHILD_DONE.",
      ].join(" ");
      const started = await call(parent, "Agent", {
        description: "native child probe", name: `native-child-${marker.slice(-8)}`,
        subagent_type: "general-purpose", model: `${modelProvider}/${modelId}`, run_in_background: true, max_turns: 12,
        prompt: childPrompt,
      });
      childToolId = typeof started.value?.agentId === "string"
        ? started.value.agentId : started.text.match(/Agent ID:\s*(\S+)/)?.[1];
      expect(childToolId).toBeTruthy();
      receipt.native_agent = { request: sanitized(started.value), output: sanitized(started.text), agent_id: childToolId };
      const candidates = await until(async () => childAgentsFrom(await redis.hgetall(SESSION_KEYS.registry), before, parentAgent!, cwd),
        (value) => value.length > 0);
      expect(candidates).toHaveLength(1);
      childAgent = candidates[0];
      expect(childAgent).not.toBe(parentAgent);
      receipt.identities.child = { agent: childAgent, cwd };

      const sent = await call(parent, "send_message", {
        to: childAgent, type: "task", content: marker, idempotency_key: `request-${marker}`,
      });
      const requestId = String(sent.value?.message_id);
      expect(requestId).not.toBe("undefined");
      receipt.request = { id: requestId, from: parentAgent, to: childAgent, marker };
      const childTrace = await until(() => streamRows(redis, `gptq:inbox-trace:${childAgent}`), (rows) => {
        const claim = rows.find((row) => row.stage === "task_claimed" && row.message_id === requestId);
        return Boolean(claim && rows.some((row) => row.stage === "task_acknowledged" && row.claim_id === claim.claim_id));
      });
      const childClaim = childTrace.find((row) => row.stage === "task_claimed" && row.message_id === requestId)!;
      expect(childTrace.some((row) => row.stage === "task_acknowledged" && row.claim_id === childClaim.claim_id)).toBe(true);
      receipt.child_trace = sanitized(childTrace);

      const parentClaim = await until(async () => {
        const claims = (parent!.agent.state.messages ?? []).filter((item: any) => item.role === "toolResult" && item.toolName === "claim_tasks" && !item.isError);
        return claims.map((item: any) => toolValue(item)).find((value: any) => taskReplyingTo(value, requestId));
      }, (value) => Boolean(value));
      const reply = taskReplyingTo(parentClaim, requestId)!;
      expect(reply.from).toBe(childAgent);
      expect(reply.to).toBe(parentAgent);
      expect(reply.payload?.in_reply_to).toBe(requestId);
      expect(reply.payload?.content).toBe(`${marker}-reply`);
      const replyId = String(reply.id);
      const parentClaimId = String(parentClaim.claim.claim_id);
      const parentActivity = await until(async () => ({
        calls: toolCallArguments(parent, "acknowledge_tasks"),
        results: (parent!.agent.state.messages ?? []).filter((item: any) => item.role === "toolResult" && item.toolName === "acknowledge_tasks" && !item.isError),
        traces: await streamRows(redis, `gptq:inbox-trace:${parentAgent}`),
      }), (activity) => activity.calls.some((args) => args.claim_id === parentClaimId) && activity.results.length > 0 &&
        activity.traces.some((row) => row.stage === "task_claimed" && row.message_id === replyId) &&
        activity.traces.some((row) => row.stage === "task_acknowledged" && row.claim_id === parentClaimId));
      receipt.reply = { id: replyId, from: childAgent, to: parentAgent, in_reply_to: requestId, content: reply.payload?.content };
      receipt.parent_claim = { claim_id: parentClaimId, message_id: replyId, tool_results: sanitized(parentActivity.results) };
      receipt.parent_trace = sanitized(parentActivity.traces);

      const childResult = await call(parent, "get_subagent_result", { agent_id: childToolId, wait: true, verbose: true });
      expect(childResult.text).toContain("NATIVE_CHILD_DONE");
      receipt.native_agent.output = sanitized(childResult.text);
      receipt.child_claim = { claim_id: childClaim.claim_id, message_id: requestId };
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: receipt.error };
      throw error;
    } finally {
      if (parent && childToolId) await call(parent, "abort_subagent", { agent_id: childToolId, reason: "acceptance cleanup" }).catch(() => undefined);
      receipt.parent_tool_trace = sessionToolTrace(parent);
      receipt.child_tool_trace = receipt.native_agent.output ?? [];
      if (!receipt.passed && parent) receipt.error ??= "Pi native child acceptance did not pass";
      await writeFile(receiptPath, `${JSON.stringify(sanitized(receipt), null, 2)}\n`, { mode: 0o600 });
      if (parent) {
        await parent.abort().catch(() => undefined);
        if (parent.extensionRunner) await parent.extensionRunner.emit({ type: "session_shutdown" }).catch(() => undefined);
        parent.dispose();
      }
      await cleanupAgent(redis, childAgent).catch(() => undefined);
      await cleanupAgent(redis, parentAgent).catch(() => undefined);
      await redis.quit();
      if (priorAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = priorAgentDir;
      process.chdir(priorCwd);
      await rm(profile, { recursive: true, force: true });
    }
  }, 420_000);
});
