import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:net";
import { join } from "node:path";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { spawn, type ChildProcess } from "node:child_process";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { sanitizeEvidence } from "./public-evidence.js";
import type { GenericCallRecord, GenericParticipant, ParticipantIdentity, RouteAdapter, RouteId, RuntimeStatus } from "./qualification-types.js";

type Json = Record<string, unknown>;
type GenericFactoryOptions = Readonly<{ repo: string; nodePath?: string; serverPort?: number }>;
export type GenericTransportEvidence = Readonly<{
  transport: "http";
  port: number;
  child_pid: number;
  node_path: string;
  node_executable_sha256: string;
  script_path: string;
  script_sha256: string;
  startup_ready: boolean;
  exit_code: number | null;
  signal_code: NodeJS.Signals | null;
  terminated: boolean;
}>;
type OwnedHttp = Readonly<{ port: number; child: ChildProcess; close: () => Promise<void>; evidence: () => GenericTransportEvidence }>;

const timeout = async <T>(promise: Promise<T>, signal: AbortSignal, ms: number): Promise<T> => {
  if (signal.aborted) throw signal.reason ?? new Error("operation aborted");
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const abort = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason ?? new Error("operation aborted"));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`generic operation timed out after ${ms}ms`)), ms); });
  try { return await Promise.race([promise, abort, deadline]); } finally {
    if (timer) clearTimeout(timer);
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
};

export const assertPrivateRedisUrl = (redisUrl: string): URL => {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" || parsed.hostname !== "127.0.0.1" || parsed.pathname !== "/15" || parsed.username || parsed.password) throw new Error("generic qualification requires private loopback Redis database 15 without credentials");
  return parsed;
};

const residualSecretKey = /(?:token|redis[_-]?url)/iu;
const redactResidual = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(redactResidual);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value as Json).map(([key, item]) => [key, residualSecretKey.test(key) ? "[redacted]" : redactResidual(item)]));
};
export const sanitizeGenericValue = (value: unknown): unknown => redactResidual(sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true }));

const identity = (route: RouteId, agent: string, profile: string): ParticipantIdentity => ({
  participantId: `${route}-${agent}`, route, hostRuntimeId: `${route}-${agent}`, agent,
  cwdHash: createHash("sha256").update(profile).digest("hex"), profileHash: createHash("sha256").update(`${profile}:profile`).digest("hex"), epochHash: createHash("sha256").update(`${profile}:epoch`).digest("hex"),
});
const resultObject = (value: unknown): unknown => {
  if (!value || typeof value !== "object") return value;
  const object = value as Json;
  return object.structuredContent ?? object.content ?? value;
};

const genericParticipant = (route: RouteId, agent: string, profile: string, clients: readonly Client[], sessionId: string | undefined, beforeClose: () => Promise<void>, closeExtra: () => Promise<void>, registration: GenericCallRecord): GenericParticipant => {
  const records: GenericCallRecord[] = [];
  records.push(registration);
  let closed = false;
  const participantIdentity = identity(route, agent, profile);
  const client = clients[clients.length - 1]!;
  const call = async (name: string, args: Readonly<Record<string, unknown>>, signal: AbortSignal): Promise<unknown> => {
    const request = sessionId ? { ...args, session_id: sessionId } : { ...args };
    const response = await timeout(client.callTool({ name, arguments: request }), signal, 30_000);
    const sanitizedRequest = sanitizeGenericValue(request) as Readonly<Record<string, unknown>>;
    const sanitizedResult = sanitizeGenericValue(resultObject(response));
    const sanitizedResponse = sanitizeGenericValue({ isError: response.isError === true, result: sanitizedResult });
    // sourceId is the fixture's local call record ID; native/server IDs remain in raw refs.
    records.push(Object.freeze({ sourceId: `fixture-call-${randomUUID()}`, name, request: sanitizedRequest, response: sanitizedResponse }));
    if (response.isError) throw new Error(`generic MCP call failed: ${name}`);
    return sanitizedResult;
  };
  return Object.freeze({
    kind: "generic", identity: participantIdentity,
    call,
    status: async (_signal: AbortSignal): Promise<RuntimeStatus> => closed ? { kind: "terminated", runtimeId: participantIdentity.hostRuntimeId } : { kind: "idle", runtimeId: participantIdentity.hostRuntimeId },
    history: async (signal: AbortSignal) => {
      if (signal.aborted) throw signal.reason ?? new Error("operation aborted");
      return Object.freeze(records.map((record) => Object.freeze({ ...record })));
    },
    close: async () => {
      if (closed) return;
      closed = true;
      const failures: unknown[] = [];
      try { await beforeClose(); } catch (error) { failures.push(error); }
      for (const value of clients) {
        try { await timeout(value.close(), new AbortController().signal, 5_000); } catch (error) { failures.push(error); }
      }
      await closeExtra();
      if (failures.length > 0) throw new AggregateError(failures, "generic participant cleanup failed");
    },
  });
};

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  await new Promise<void>((resolve) => server.close(() => resolve()));
  if (!address || typeof address === "string") throw new Error("failed to reserve owned HTTP port");
  return address.port;
};

const startOwnedHttp = async (options: GenericFactoryOptions, redisUrl: string, signal: AbortSignal): Promise<OwnedHttp> => {
  assertPrivateRedisUrl(redisUrl);
  const port = options.serverPort ?? await freePort();
  const nodePath = options.nodePath ?? process.execPath;
  const scriptPath = join(options.repo, "dist/transports/http.js");
  const [nodeExecutableSha256, scriptSha256] = await Promise.all([readFile(nodePath).then((value) => createHash("sha256").update(value).digest("hex")), readFile(scriptPath).then((value) => createHash("sha256").update(value).digest("hex"))]);
  const child = spawn(nodePath, [scriptPath, "--port", String(port)], { cwd: options.repo, env: { ...process.env, REDIS_URL: redisUrl, GPTQUEUE_HOST: "127.0.0.1", GPTQUEUE_HTTP_TOKEN: "", GPTQUEUE_HTTP_SOCKET: "" }, stdio: ["ignore", "pipe", "pipe"] });
  if (!child.pid) throw new Error("owned HTTP server did not spawn");
  const evidence = (): GenericTransportEvidence => Object.freeze({
    transport: "http", port, child_pid: child.pid!, node_path: nodePath, node_executable_sha256: nodeExecutableSha256,
    script_path: scriptPath, script_sha256: scriptSha256, startup_ready: true, exit_code: child.exitCode, signal_code: child.signalCode,
    terminated: child.exitCode !== null || child.signalCode !== null,
  });
  let stdout = "", stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => { stdout = (stdout + chunk.toString()).slice(-8192); });
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-8192); });
  const waitExit = async (ms: number): Promise<boolean> => {
    if (child.exitCode !== null || child.signalCode !== null) return true;
    return new Promise<boolean>((resolve) => { const timer = setTimeout(() => resolve(false), ms); child.once("exit", () => { clearTimeout(timer); resolve(true); }); });
  };
  const stop = async (): Promise<void> => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGTERM");
    if (await waitExit(3_000)) return;
    child.kill("SIGKILL");
    if (!(await waitExit(2_000))) throw new Error(`owned HTTP server did not exit after SIGKILL: ${stderr}`);
  };
  try {
    const end = Date.now() + 15_000;
    let announced = false;
    while (Date.now() < end) {
      if (signal.aborted) throw signal.reason ?? new Error("HTTP startup aborted");
      if (child.exitCode !== null) throw new Error(`owned HTTP server exited before readiness: ${stderr}`);
      announced = stdout.includes(`gptqueue HTTP server listening on 127.0.0.1:${port}`);
      if (announced && await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(500) }).then((response) => response.ok).catch(() => false)) break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (!announced) throw new Error(`owned HTTP server did not announce exact port ${port}: ${stderr}`);
    if (!(await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(500) }).then((response) => response.ok).catch(() => false))) throw new Error("owned HTTP server health check timed out");
    return { port, child, close: stop, evidence };
  } catch (error) { await stop(); throw error; }
};

const closeClient = async (client: Client): Promise<void> => { await timeout(client.close(), new AbortController().signal, 5_000); };
const connectClient = async (client: Client, transport: StdioClientTransport | StreamableHTTPClientTransport, signal: AbortSignal): Promise<void> => {
  try { await timeout(client.connect(transport), signal, 30_000); }
  catch (error) { try { await closeClient(client); } catch (cleanup) { throw new AggregateError([error, cleanup], "generic client connect cleanup failed"); } throw error; }
};
const register = async (client: Client, name: string, signal: AbortSignal): Promise<Readonly<{ sessionId: string; record: GenericCallRecord }>> => {
  const request = { name, role: "both", description: "frozen generic qualification participant" };
  const response = await timeout(client.callTool({ name: "register_agent", arguments: request }), signal, 30_000);
  if (response.isError) throw new Error("generic registration MCP call failed");
  const value = resultObject(response);
  if (!value || typeof value !== "object" || typeof (value as Json).session_id !== "string") throw new Error("generic registration returned no private session");
  return { sessionId: (value as Json).session_id as string, record: Object.freeze({ sourceId: `fixture-call-${randomUUID()}`, name: "register_agent", request: sanitizeGenericValue(request) as Readonly<Record<string, unknown>>, response: sanitizeGenericValue({ isError: response.isError === true, result: value }) }) };
};

export const createGenericAdapters = (options: GenericFactoryOptions): Readonly<{ adapters: readonly RouteAdapter[]; close: () => Promise<void>; transportEvidence: () => GenericTransportEvidence | null }> => {
  let http: OwnedHttp | undefined;
  let httpPromise: Promise<OwnedHttp> | undefined;
  let httpRedisUrl: string | undefined;
  let retainedHttpEvidence: GenericTransportEvidence | null = null;
  const ensureHttp = async (redisUrl: string, signal: AbortSignal): Promise<OwnedHttp> => {
    if (http && httpRedisUrl !== redisUrl) throw new Error("owned HTTP adapter cannot reuse one port with a different Redis URL");
    if (http) return http;
    if (httpPromise && httpRedisUrl !== redisUrl) throw new Error("owned HTTP startup already targets a different Redis URL");
    httpRedisUrl = redisUrl;
    httpPromise ??= startOwnedHttp(options, redisUrl, signal).then((value) => { http = value; return value; }).catch((error) => { httpPromise = undefined; httpRedisUrl = undefined; throw error; });
    return httpPromise;
  };
  const make = (id: "generic-stdio" | "generic-http" | "generic-stateless"): RouteAdapter => Object.freeze({
    spec: { id, host: "generic", modelBacked: false, availability: { kind: "setup_gap", detail: "preflight not run" } },
    preflight: async (signal) => {
      if (signal.aborted) throw signal.reason ?? new Error("preflight aborted");
      const executable = id === "generic-stdio" ? "dist/mcp-server/index.js" : "dist/transports/http.js";
      return existsSync(join(options.repo, executable)) ? { kind: "available" as const } : { kind: "setup_gap" as const, detail: `missing ${executable}` };
    },
    launch: async (input, signal) => {
      const redisUrl = input.redisUrl;
      assertPrivateRedisUrl(redisUrl);
      const run = randomUUID(), agent = `${id}-${run}`;
      let owner: Client;
      let wire: Client | undefined;
      let sessionId: string | undefined;
      let registration: GenericCallRecord;
      if (id === "generic-stdio") {
        owner = new Client({ name: agent, version: "1" });
        await connectClient(owner, new StdioClientTransport({ command: options.nodePath ?? process.execPath, args: [join(options.repo, "dist/mcp-server/index.js")], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> }), signal);
      } else {
        const owned = await ensureHttp(redisUrl, signal);
        owner = new Client({ name: agent, version: "1" });
        await connectClient(owner, new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${owned.port}/mcp`)), signal);
      }
      try {
        const registered = await register(owner, agent, signal);
        sessionId = registered.sessionId;
        registration = registered.record;
      } catch (error) { try { await closeClient(owner); } catch (cleanup) { throw new AggregateError([error, cleanup], "generic registration cleanup failed"); } throw error; }
      const clients = [owner];
      if (id === "generic-stateless") {
        wire = new Client({ name: `${agent}-wire`, version: "1" });
        const owned = await ensureHttp(redisUrl, signal);
        try { await connectClient(wire, new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${owned.port}/mcp`)), signal); }
        catch (error) { try { await closeClient(owner); } catch (cleanup) { throw new AggregateError([error, cleanup], "generic wire cleanup failed"); } throw error; }
        clients.push(wire);
      }
      const closeOwner = async () => {
        const response = await timeout(owner.callTool({ name: "close_session", arguments: id === "generic-stateless" ? { session_id: sessionId } : {} }), new AbortController().signal, 10_000);
        if (response.isError) throw new Error("generic close_session MCP call failed");
      };
      return genericParticipant(id, agent, options.repo, clients, id === "generic-stateless" ? sessionId : undefined, closeOwner, async () => undefined, registration);
    },
  });
  return Object.freeze({
    adapters: Object.freeze([make("generic-stdio"), make("generic-http"), make("generic-stateless")]),
    transportEvidence: (): GenericTransportEvidence | null => http?.evidence() ?? retainedHttpEvidence,
    close: async () => {
      const active = http;
      let failure: unknown;
      if (active) {
        try { await active.close(); } catch (error) { failure = error; }
        retainedHttpEvidence = active.evidence();
      }
      http = undefined; httpPromise = undefined; httpRedisUrl = undefined;
      if (failure) throw failure;
    },
  });
};
