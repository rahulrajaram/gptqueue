/**
 * F2: the owned Redis URL is a unix-socket path, not a TCP URL. The opt-in
 * qualification suites build receipts and child configuration from it; these
 * ordinary tests exercise that construction in every default run.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { geminiMcpSettings, ownedRedisReceipt, startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { codexEffectiveConfig, codexHeadlessEffectiveConfig } from "./qualification-codex.js";
import { REDIS_ENDPOINT_ENV, testRedisUrl } from "../helpers/test-redis.js";

let owned: OwnedRedis;
beforeAll(async () => { owned = await startOwnedRedis({ db: 15 }); });
afterAll(async () => { await owned.close(); });

describe("owned Redis receipts", () => {
  it("record the transport and database of an owned instance, never a host, port or path", () => {
    expect(() => new URL(owned.url)).toThrow(); // why the suites must not parse it as a URL
    expect(ownedRedisReceipt(owned.url)).toEqual({ transport: "unix-socket", database: "15", owned_process: true });
    expect(JSON.stringify(ownedRedisReceipt(owned.url))).not.toContain(owned.dir);
    expect(ownedRedisReceipt(testRedisUrl()).database).toBe("15");
  });

  it("refuse a TCP URL instead of describing it", () => {
    expect(() => ownedRedisReceipt("redis://127.0.0.1:6379/15")).toThrow(/private unix-socket instance/u);
  });
});

describe("child configuration built from an owned instance", () => {
  it("Gemini MCP settings name the owned socket under every Redis alias", () => {
    const settings = geminiMcpSettings("/repo/dist/mcp-server/index.js", owned.url, "/usr/bin/node");
    expect(settings.mcpServers.gptqueue).toMatchObject({ command: "/usr/bin/node", args: ["/repo/dist/mcp-server/index.js"], trust: true });
    for (const name of REDIS_ENDPOINT_ENV) expect(settings.mcpServers.gptqueue.env[name]).toBe(owned.url);
    expect(() => geminiMcpSettings("/x.js", "redis://127.0.0.1:6379/15")).toThrow(/private unix-socket instance/u);
  });

  it("Codex app-server and headless configs pass the owned socket to the sidecar and every alias", () => {
    for (const config of [codexEffectiveConfig({}, owned.url, "/tmp/control.sock"), codexHeadlessEffectiveConfig({}, owned.url)]) {
      expect(config["mcp_servers.gptqueue-shared.args"]).toEqual(expect.arrayContaining(["--redis-url", owned.url]));
      for (const name of REDIS_ENDPOINT_ENV) expect(config[`mcp_servers.gptqueue-shared.env.${name}`]).toBe(owned.url);
    }
  });
});

describe("no acceptance source parses an owned Redis URL as TCP", () => {
  it("has no new URL(<owned>.url), TCP host/port receipt fields, or env-only REDIS_URL MCP config", () => {
    const dir = join(process.cwd(), "tests/acceptance");
    const self = "owned-redis-metadata.test.ts"; // holds these patterns as text
    const offenders = readdirSync(dir).filter((name) => name.endsWith(".ts") && name !== self).flatMap((name) => {
      const text = readFileSync(join(dir, name), "utf8");
      return [
        /new URL\((?:owned|redis|ownedRedis|ownedProcess)\.url\)/u,
        /redis: \{[^}]*\bhost: "127\.0\.0\.1"/u,
        /env: \{ REDIS_URL: [^}]*\}, trust/u,
      ].filter((pattern) => pattern.test(text)).map((pattern) => `${name}: ${pattern.source}`);
    });
    expect(offenders).toEqual([]);
  });
});
