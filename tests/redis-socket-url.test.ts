/**
 * The additive unix-socket form of --redis-url (/abs/path.sock[?db=0-15]) for
 * gptqueue-session and gptqueue-experiment. redis:// forms are unchanged and
 * stay covered by tests/registered-shell.test.ts and
 * tests/experimental-wrapper.test.ts; end-to-end use of the socket form is
 * covered by tests/registered-shell-process.test.ts and the experimental
 * wrapper's run tests, which now connect through the run's private socket.
 */
import { describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { parseRedisSocketUrl } from "../src/core/redis-socket-url.js";
import { parseRegisteredShellArgs } from "../src/registered-shell/server.js";
import { parseWrapperArgs } from "../src/experimental-wrapper/config.js";
import { testRedisUrl } from "./helpers/test-redis.js";

const ACCEPTED: ReadonlyArray<readonly [string, { path: string; db: number }]> = [
  ["/run/redis/redis.sock", { path: "/run/redis/redis.sock", db: 0 }],
  ["/run/redis/redis.sock?db=0", { path: "/run/redis/redis.sock", db: 0 }],
  ["/run/redis/redis.sock?db=7", { path: "/run/redis/redis.sock", db: 7 }],
  ["/run/redis/redis.sock?db=15", { path: "/run/redis/redis.sock", db: 15 }],
  ["/srv/a-b_c.d/~x@y+z,w=v:u/s.sock?db=14", { path: "/srv/a-b_c.d/~x@y+z,w=v:u/s.sock", db: 14 }],
];
const MALFORMED: ReadonlyArray<readonly [string, RegExp]> = [
  ["/run/redis/redis.sock?db=16", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?db=015", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?db=", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?db=-1", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?db=1&host=127.0.0.1", /db=N with N in 0-15/u],
  ["/run/redis/redis.sock?port=6379", /db=N with N in 0-15/u],
  ["//127.0.0.1/redis.sock", /empty segment/u],
  ["/run//redis.sock", /empty segment/u],
  ["/run/redis/", /names a directory/u],
  ["/run/redis sock", /unsupported characters/u],
  ["/run/redis.sock#frag", /unsupported characters/u],
  ["/run/redis%2Esock", /unsupported characters/u],
  ["/run/re\\dis.sock", /unsupported characters/u],
];

describe("parseRedisSocketUrl", () => {
  it.each(ACCEPTED)("accepts %s", (value, expected) => {
    expect(parseRedisSocketUrl(value)).toEqual(expected);
  });

  it.each(ACCEPTED)("validates exactly what ioredis connects to: %s", (value, expected) => {
    const redis = new Redis(value, { lazyConnect: true });
    try {
      expect(redis.options.path).toBe(expected.path);
      expect(redis.options.db).toBe(expected.db);
    } finally { redis.disconnect(); }
  });

  it.each(MALFORMED)("refuses %s with a clear error", (value, reason) => {
    expect(() => parseRedisSocketUrl(value)).toThrow(/^Invalid Redis socket path \(expected an absolute socket path/u);
    expect(() => parseRedisSocketUrl(value)).toThrow(reason);
  });

  it("leaves non-socket values to the caller's redis:// handling", () => {
    for (const value of ["redis://127.0.0.1:6379/15", "rediss://h/1", "relative/redis.sock", "", "unix:///run/redis.sock"]) {
      expect(parseRedisSocketUrl(value)).toBeUndefined();
    }
  });
});

describe("gptqueue-session --redis-url", () => {
  it("accepts the socket form, with or without a db", () => {
    for (const [value] of ACCEPTED) {
      expect(parseRegisteredShellArgs(["--client", "codex", "--redis-url", value])).toMatchObject({ client: "codex", redisUrl: value });
    }
    expect(parseRegisteredShellArgs(["--client", "pi", "--redis-url", testRedisUrl()]).redisUrl).toBe(testRedisUrl());
  });

  it("refuses a malformed socket form with the socket error", () => {
    for (const [value, reason] of MALFORMED) {
      expect(() => parseRegisteredShellArgs(["--client", "codex", "--redis-url", value])).toThrow(reason);
    }
  });

  it("keeps redis:// behavior byte-identical", () => {
    expect(parseRegisteredShellArgs(["--client", "codex", "--redis-url", "redis://127.0.0.1:6379/15"]).redisUrl).toBe("redis://127.0.0.1:6379/15");
    expect(parseRegisteredShellArgs(["--client", "codex", "--redis-url", "rediss://h:1/0"]).redisUrl).toBe("rediss://h:1/0");
    for (const value of ["redis://127.0.0.1:6379/15?x=1", "redis://127.0.0.1:6379/a", "http://h/1", "relative/redis.sock", "unix:///run/redis.sock"]) {
      expect(() => parseRegisteredShellArgs(["--client", "codex", "--redis-url", value])).toThrow("Invalid Redis URL (expected redis://host/database)");
    }
  });
});

describe("gptqueue-experiment --redis-url", () => {
  const args = (redisUrl: string) => ["codex", "--agent", "gptqueue-experiment-socket", "--workspace", "/tmp", "--redis-url", redisUrl, "--", "prompt"];

  it("accepts the socket form with a non-zero db", () => {
    expect(parseWrapperArgs(args("/run/redis/redis.sock?db=14")).redisUrl).toBe("/run/redis/redis.sock?db=14");
    expect(parseWrapperArgs(args(testRedisUrl())).redisUrl).toBe(testRedisUrl());
  });

  it("still refuses db0, which a socket path without ?db selects", () => {
    for (const value of ["/run/redis/redis.sock", "/run/redis/redis.sock?db=0"]) {
      expect(() => parseWrapperArgs(args(value))).toThrow(/refuses Redis db0/u);
    }
  });

  it("refuses a malformed socket form with the socket error", () => {
    for (const [value, reason] of MALFORMED) expect(() => parseWrapperArgs(args(value))).toThrow(reason);
  });

  it("keeps redis:// behavior byte-identical", () => {
    expect(parseWrapperArgs(args("redis://127.0.0.1:6379/14")).redisUrl).toBe("redis://127.0.0.1:6379/14");
    expect(() => parseWrapperArgs(args("redis://127.0.0.1:6379/0"))).toThrow(/refuses Redis db0/u);
    expect(() => parseWrapperArgs(args("http://127.0.0.1/1"))).toThrow("Redis URL must use redis:// or rediss://: http://127.0.0.1/1");
  });
});
