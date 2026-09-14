import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { startOwnedRedis } from "./owned-redis.js";
import {
  mcpEntry,
  model,
  modelsPath,
  newConfigHome,
  newWorkDir,
  opencodeBin,
  publicEvidence,
  repo,
  type Json,
} from "./opencode-support.js";

export const repairEnabled = process.env.GPTQUEUE_OPENCODE_REPAIR_TESTS === "1";
export const repairPluginPath = join(repo, "dist/registered-shell/opencode-plugin.js");
export const repairArtifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/opencode-repair");

export type OwnedRedis = Readonly<{
  url: string;
  redis: Redis;
  close: () => Promise<void>;
}>;

export const openOwnedRedis = async (): Promise<OwnedRedis> => {
  const ownedProcess = await startOwnedRedis();
  const url = ownedProcess.url;
  const redis = new Redis(url, { maxRetriesPerRequest: 3 });
  try {
    await redis.ping();
    return Object.freeze({ url, redis, close: async () => { await redis.quit(); await ownedProcess.close(); } });
  } catch (error) {
    redis.disconnect();
    await ownedProcess.close();
    throw error;
  }
};

export const repairConfig = (pluginPath = repairPluginPath): Json => ({
  $schema: "https://opencode.ai/config.json",
  model,
  autoupdate: false,
  plugin: [pluginPath],
  // The local session adapter owns GPTQueue MCP clients. Do not load the
  // direct shared MCP server, whose process-local registration would re-create
  // the identity displacement this proof is designed to catch.
  mcp: {
    // Explicitly disable the inherited direct server entry. GPTQueue access
    // comes only from the local session plugin below.
    gptqueue: { type: "local", command: [process.execPath, mcpEntry], enabled: false },
  },
  provider: {
    "zai-coding-plan": {
      name: "Z.AI Coding Plan",
      npm: "@ai-sdk/openai-compatible",
      options: { baseURL: "https://api.z.ai/api/coding/paas/v4" },
      models: { "glm-5.3": { name: "GLM-5.3" } },
    },
  },
});

export const hashText = (value: string | Uint8Array): string => createHash("sha256").update(value).digest("hex");

export const hashFile = (path: string): string => {
  if (!existsSync(path)) throw new Error(`required evidence source is missing: ${path}`);
  return hashText(readFileSync(path));
};

export const ownedAgents = async (redis: Redis, before: ReadonlySet<string>, directory: string): Promise<readonly string[]> => {
  const registry = await redis.hgetall(SESSION_KEYS.registry);
  return Object.entries(registry)
    .filter(([name, raw]) => {
      if (before.has(name) || !name.startsWith("gptqueue-opencode-")) return false;
      try {
        const metadata = JSON.parse(raw).metadata as Record<string, unknown> | undefined;
        return metadata?.working_directory === directory;
      } catch { return false; }
    })
    .map(([name]) => name);
};

export const streamRows = async (redis: Redis, key: string): Promise<readonly Json[]> => {
  const rows = await redis.xrange(key, "-", "+");
  return rows.map(([, fields]) => Object.fromEntries(
    Array.from({ length: fields.length / 2 }, (_, index) => [fields[index * 2]!, fields[index * 2 + 1]!]),
  ));
};

export type RepairSession = Readonly<{
  id: string;
  record: () => Promise<Json>;
  status: () => Promise<Json>;
  history: () => Promise<unknown>;
  prompt: (text: string) => Promise<Json>;
  close: () => Promise<void>;
}>;

const nativePromptTimeoutMs = 150_000;

export type RepairOpenCodeServer = Readonly<{
  baseUrl: string;
  directory: string;
  profileHash: string;
  processId?: number;
  toolIds: readonly string[];
  session: (id?: string) => Promise<RepairSession>;
  sessions: () => Promise<readonly Json[]>;
  close: () => Promise<void>;
}>;

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
};

const request = async (base: string, path: string, init?: RequestInit): Promise<any> => {
  const response = await fetch(`${base}${path}`, { ...init, signal: init?.signal ?? AbortSignal.timeout(30_000) });
  if (!response.ok) throw new Error(`OpenCode repair HTTP ${response.status} ${path}`);
  return response.status === 204 ? undefined : response.json();
};

const stop = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  await Promise.race([exited, new Promise<void>((resolve) => setTimeout(resolve, 5_000))]);
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
};

/** Persistent serve harness; the parent and native child remain observable until proof teardown. */
export const startRepairOpenCodeServer = async (redisUrl: string): Promise<RepairOpenCodeServer> => {
  if (!existsSync(opencodeBin) || !existsSync(repairPluginPath) || !existsSync(modelsPath)) {
    throw new Error("OpenCode repair prerequisites unavailable");
  }
  const configHome = newConfigHome();
  const homeDir = newConfigHome();
  const dataHome = newConfigHome();
  const cacheHome = newConfigHome();
  const stateHome = newConfigHome();
  const directory = newWorkDir();
  const profileHash = hashText(JSON.stringify({
    config: repairConfig(),
    HOME: homeDir,
    XDG_CONFIG_HOME: configHome,
    XDG_DATA_HOME: dataHome,
    XDG_CACHE_HOME: cacheHome,
    XDG_STATE_HOME: stateHome,
    REDIS_URL: redisUrl,
    OPENCODE_PURE: undefined,
    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
    OPENCODE_MODELS_PATH: modelsPath,
  }));
  const port = await freePort();
  const child = spawn(opencodeBin, ["serve", "--port", String(port), "--hostname", "127.0.0.1"], {
    cwd: repo,
    env: {
      ...process.env,
      REDIS_URL: redisUrl,
      HOME: homeDir,
      XDG_CONFIG_HOME: configHome,
      XDG_DATA_HOME: dataHome,
      XDG_CACHE_HOME: cacheHome,
      XDG_STATE_HOME: stateHome,
      OPENCODE_CONFIG: undefined,
      OPENCODE_CONFIG_DIR: undefined,
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_CONFIG_CONTENT: JSON.stringify(repairConfig()),
      OPENCODE_PURE: undefined,
      OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
      OPENCODE_MODELS_PATH: modelsPath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-20_000); });
  const base = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 20_000;
  try {
    while (Date.now() < deadline) {
      if (child.exitCode !== null) throw new Error(`OpenCode serve exited: ${stderr}`);
      try { await request(base, "/config", { signal: AbortSignal.timeout(500) }); break; }
      catch { await new Promise((resolve) => setTimeout(resolve, 250)); }
    }
    if (Date.now() >= deadline) throw new Error(`OpenCode serve timed out: ${stderr}`);
    const toolIdsValue = await request(base, `/experimental/tool/ids?directory=${encodeURIComponent(directory)}`);
    if (!Array.isArray(toolIdsValue) || !toolIdsValue.every((id): id is string => typeof id === "string")) {
      throw new Error("OpenCode repair tool ID endpoint did not return string IDs");
    }
    const toolIds = toolIdsValue as readonly string[];
    const expectedToolIds = [
      "gptqueue_claim_tasks", "gptqueue_acknowledge_tasks", "gptqueue_renew_claim",
      "gptqueue_send_message", "gptqueue_receive_message", "gptqueue_get_queue_status",
      "gptqueue_list_agents", "gptqueue_get_runtime_status", "gptqueue_find_agents",
      "gptqueue_get_agent_details", "gptqueue_get_delivery_status",
    ];
    const missingToolIds = expectedToolIds.filter((id) => !toolIds.includes(id));
    if (missingToolIds.length > 0) throw new Error(`OpenCode repair missing native tool IDs: ${missingToolIds.join(",")}`);
    const session = async (id?: string): Promise<RepairSession> => {
      const created = id ? { id } : await request(base, `/session?directory=${encodeURIComponent(directory)}`, {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ title: "GPTQueue identity repair" }),
      });
      if (!created?.id) throw new Error("OpenCode repair session has no id");
      const sessionID = String(created.id);
      return Object.freeze({
        id: sessionID,
        record: () => request(base, `/session/${encodeURIComponent(sessionID)}?directory=${encodeURIComponent(directory)}`),
        status: () => request(base, `/session/status?directory=${encodeURIComponent(directory)}`),
        history: () => request(base, `/session/${encodeURIComponent(sessionID)}/message?directory=${encodeURIComponent(directory)}`),
        prompt: (text: string) => request(base, `/session/${encodeURIComponent(sessionID)}/message?directory=${encodeURIComponent(directory)}`, {
          method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ parts: [{ type: "text", text }] }),
          signal: AbortSignal.timeout(nativePromptTimeoutMs),
        }),
        close: async () => {
          await request(base, `/session/${encodeURIComponent(sessionID)}?directory=${encodeURIComponent(directory)}`, { method: "DELETE" });
        },
      });
    };
    return Object.freeze({
      baseUrl: base,
      directory,
      profileHash,
      processId: child.pid,
      toolIds,
      session,
      sessions: () => request(base, `/session?directory=${encodeURIComponent(directory)}`),
      close: async () => {
        await stop(child);
        for (const path of [configHome, homeDir, dataHome, cacheHome, stateHome, directory]) {
          rmSync(path, { recursive: true, force: true });
        }
      },
    });
  } catch (error) {
    await stop(child);
    for (const path of [configHome, homeDir, dataHome, cacheHome, stateHome, directory]) {
      rmSync(path, { recursive: true, force: true });
    }
    throw error;
  }
};

export const cleanupOwnedAgent = async (redis: Redis, name: string | undefined): Promise<void> => {
  if (!name) return;
  const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
  await redis.hdel(SESSION_KEYS.registry, name);
  await redis.del(
    SESSION_KEYS.agent(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name),
    SESSION_KEYS.heartbeat(name), SESSION_KEYS.agentSessions(name),
    `gptq:runtime-binding:${name}`, `gptq:activation:${name}`, `gptq:inbox-events:${name}`,
    `gptq:inbox-trace:${name}`, ...sessions.flatMap((session) => [SESSION_KEYS.session(session), SESSION_KEYS.lease(session)]),
  );
};

export const writeRepairReceipt = (runID: string, receipt: Json): string => {
  const directory = join(repairArtifactRoot, runID);
  mkdirSync(directory, { recursive: true });
  const path = join(directory, "receipt.json");
  writeFileSync(path, `${JSON.stringify(publicEvidence(receipt), null, 2)}\n`, { mode: 0o600 });
  return path;
};

export { mcpEntry, newConfigHome, newWorkDir, opencodeBin };
