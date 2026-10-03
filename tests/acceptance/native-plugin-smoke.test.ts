import { createHash } from "node:crypto";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { spawn, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { Redis } from "ioredis";
import { afterEach, describe, it } from "vitest";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { opencodeBin, modelsPath, newConfigHome, newWorkDir, opencodePrerequisites, repo } from "./opencode-support.js";

const pluginPath = join(repo, "dist/registered-shell/opencode-plugin.js");
const artifactDir = join(repo, ".gptqueue/repair-qualification/20260912/native-plugin-smoke");
const sleep = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("smoke port unavailable");
  const port = address.port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
};

const request = async (base: string, path: string, init?: RequestInit): Promise<any> => {
  const response = await fetch(`${base}${path}`, { ...init, signal: init?.signal ?? AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`HTTP ${response.status} ${path}`);
  return response.status === 204 ? undefined : response.json();
};

const stop = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  await Promise.race([
    new Promise<void>((resolve) => child.once("exit", () => resolve())),
    sleep(5_000),
  ]);
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
};

describe("native OpenCode plugin loader smoke", () => {
  let owned: OwnedRedis | undefined;
  let child: ChildProcess | undefined;
  let configHome: string | undefined;
  let homeDir: string | undefined;
  let dataHome: string | undefined;
  let cacheHome: string | undefined;
  let stateHome: string | undefined;
  let workDir: string | undefined;

  afterEach(async () => {
    if (child) await stop(child);
    if (configHome) rmSync(configHome, { recursive: true, force: true });
    if (homeDir) rmSync(homeDir, { recursive: true, force: true });
    if (dataHome) rmSync(dataHome, { recursive: true, force: true });
    if (cacheHome) rmSync(cacheHome, { recursive: true, force: true });
    if (stateHome) rmSync(stateHome, { recursive: true, force: true });
    if (workDir) rmSync(workDir, { recursive: true, force: true });
    await owned?.close();
  });

  it("loads default-only plugin, exposes tools, and auto-registers a native session", async (ctx) => {
    const prerequisites = opencodePrerequisites(existsSync, { binary: opencodeBin, models: modelsPath, plugin: pluginPath });
    ctx.skip(prerequisites.kind === "unavailable", "OpenCode model/binary prerequisites unavailable");
    // OpenCode is present: a missing built plugin is a build failure, not a skip.
    if (prerequisites.kind === "build_missing") throw new Error(prerequisites.detail);
    const stderr: string[] = [];
    const stdout: string[] = [];
    let phase = "prerequisites";
    let toolIds: string[] = [];
    let allToolIds: string[] = [];
    let configuredPluginCount: number | undefined;
    let legacyMcpDisabled = false;
    let sessionHash: string | undefined;
    let registrationObserved = false;
    let failure: string | undefined;
    try {
      if (!existsSync(pluginPath) || !existsSync(modelsPath) || !existsSync(opencodeBin)) {
        throw new Error("plugin/model/binary prerequisites unavailable");
      }
      phase = "owned_redis";
      owned = await startOwnedRedis();
      configHome = newConfigHome();
      homeDir = newConfigHome();
      dataHome = newConfigHome();
      cacheHome = newConfigHome();
      stateHome = newConfigHome();
      workDir = newWorkDir();
      const port = await freePort();
      const config = JSON.stringify({
        $schema: "https://opencode.ai/config.json",
        plugin: [`file://${pluginPath}`],
        mcp: { gptqueue: { type: "local", command: ["/bin/false"], enabled: false } },
      });
      const configPath = join(configHome, "opencode", "opencode.json");
      mkdirSync(join(configHome, "opencode"), { recursive: true, mode: 0o700 });
      await writeFile(configPath, `${config}\n`, { mode: 0o600 });
      phase = "server_spawn";
      child = spawn(opencodeBin, ["serve", "--port", String(port), "--hostname", "127.0.0.1"], {
        cwd: workDir,
        env: {
          ...process.env,
          HOME: homeDir,
          XDG_CONFIG_HOME: configHome,
          XDG_DATA_HOME: dataHome,
          XDG_CACHE_HOME: cacheHome,
          XDG_STATE_HOME: stateHome,
          OPENCODE_CONFIG: undefined,
          OPENCODE_CONFIG_DIR: undefined,
          OPENCODE_DISABLE_PROJECT_CONFIG: "1",
          OPENCODE_CONFIG_CONTENT: undefined,
          OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
          OPENCODE_MODELS_PATH: modelsPath,
          GPTQUEUE_REDIS_URL: owned.url,
          REDIS_URL: owned.url,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
      child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk.toString()));
      child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk.toString()));
      const base = `http://127.0.0.1:${port}`;
      const directory = workDir!;
      phase = "http_readiness";
      const readyDeadline = Date.now() + 20_000;
      let ready = false;
      while (Date.now() < readyDeadline) {
        if (child.exitCode !== null) throw new Error(`OpenCode exited before HTTP readiness (${child.exitCode})`);
        try {
          const configResponse = await request(base, `/config?directory=${encodeURIComponent(directory)}`);
          configuredPluginCount = Array.isArray(configResponse?.plugin) ? configResponse.plugin.length : undefined;
          legacyMcpDisabled = configResponse?.mcp?.gptqueue?.enabled === false;
          ready = true;
          break;
        } catch { await sleep(200); }
      }
      if (!ready) throw new Error("OpenCode HTTP readiness deadline exceeded");

      phase = "tool_ids";
      const returnedToolIds = await request(base, `/experimental/tool/ids?directory=${encodeURIComponent(directory)}`);
      if (!Array.isArray(returnedToolIds)) throw new Error("tool ID endpoint did not return an array");
      allToolIds = returnedToolIds.filter((id): id is string => typeof id === "string");
      toolIds = returnedToolIds.filter((id): id is string => typeof id === "string" && id.startsWith("gptqueue_"));
      const expectedTools = [
        "gptqueue_claim_tasks", "gptqueue_acknowledge_tasks", "gptqueue_renew_claim",
        "gptqueue_send_message", "gptqueue_receive_message", "gptqueue_get_queue_status",
        "gptqueue_list_agents", "gptqueue_get_runtime_status", "gptqueue_find_agents",
        "gptqueue_get_agent_details", "gptqueue_get_delivery_status",
      ];
      const missingTools = expectedTools.filter((name) => !returnedToolIds.includes(name));
      if (missingTools.length > 0) throw new Error(`missing native tool IDs: ${missingTools.join(",")}`);

      phase = "session_create";
      const session = await request(base, `/session?directory=${encodeURIComponent(directory)}`, {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ title: "native plugin smoke" }),
      });
      if (typeof session.id !== "string") throw new Error("session create returned no ID");
      sessionHash = createHash("sha256").update(session.id).digest("hex");
      const agent = `gptqueue-opencode-${session.id}`;
      phase = "registration_observation";
      const redis = new Redis(owned.url);
      try {
        const registrationDeadline = Date.now() + 10_000;
        let registration: Record<string, unknown> | undefined;
        while (Date.now() < registrationDeadline) {
          const raw = await redis.hget("gptq:registry", agent);
          if (raw) { registration = JSON.parse(raw) as Record<string, unknown>; break; }
          await sleep(200);
        }
        if (!registration?.description || !String(registration.description).includes("OpenCode native session")) {
          throw new Error("native session registration was not observed");
        }
        if ((registration.metadata as Record<string, unknown> | undefined)?.working_directory !== directory) {
          throw new Error("native session registration directory mismatch");
        }
        registrationObserved = true;
      } finally {
        await redis.quit();
      }
    } catch (error) {
      failure = error instanceof Error ? error.message.replace(/redis:\/\/[^\s)]+/g, "redis://<redacted>") : String(error);
      throw error;
    } finally {
      mkdirSync(artifactDir, { recursive: true });
      const stderrText = stderr.join("");
      const logRoot = dataHome ? join(dataHome, "opencode", "log") : "";
      const logFiles = logRoot ? await readdir(logRoot).catch(() => [] as string[]) : [];
      const opencodeLog = (await Promise.all(logFiles.map((name) => readFile(join(logRoot, name), "utf8").catch(() => "")))).join("\n");
      await writeFile(
        join(artifactDir, "smoke.json"),
        `${JSON.stringify({ phase, server_ready: child?.exitCode === null, configured_plugin_count: configuredPluginCount, legacy_mcp_disabled: legacyMcpDisabled, all_tool_ids: allToolIds, tool_ids: toolIds, session_sha256: sessionHash, registration_observed: registrationObserved, failure, stderr_nonempty: stderrText.length > 0, stderr_sha256: createHash("sha256").update(stderrText).digest("hex"), stdout_sha256: createHash("sha256").update(stdout.join("")).digest("hex"), server_output_tail: stdout.join("").slice(-4_000).replace(/redis:\/\/[^\s)]+/g, "redis://<redacted>"), opencode_log_tail: opencodeLog.slice(-8_000).replace(/redis:\/\/[^\s)]+/g, "redis://<redacted>") }, null, 2)}\n`,
        { mode: 0o600 },
      );
    }
  });
});
