// RT-1: immutable, verified runtime releases (scripts/install-runtime.mjs).
// Installs into temp prefixes from a fixture git checkout built from this
// worktree's dist/, bin/ and package files. Verification runs the real
// release against the private Redis from with-owned-redis.mjs.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFileSync, chmodSync, cpSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync,
  readdirSync, readlinkSync, statSync, writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
// @ts-expect-error untyped .mjs tooling module
import * as rr from "../scripts/lib/runtime-release.mjs";
// @ts-expect-error untyped .mjs tooling module
import { buildPlan, renderPlan } from "../scripts/lib/runtime-plan.mjs";

const REPO = process.cwd();
const REDIS_URL = process.env.REDIS_URL!;
const scratch = mkdtempSync(join(tmpdir(), "gptq-rt1-"));
const source = join(scratch, "src-checkout");
const sha256 = (p: string) => createHash("sha256").update(readFileSync(p)).digest("hex");
const gitIn = (...args: string[]) => {
  const r = spawnSync("git", ["-C", source, "-c", "user.name=t", "-c", "user.email=t@t", ...args], { encoding: "utf8" });
  if (r.status !== 0) throw new Error(r.stderr);
  return r.stdout.trim();
};
const commit = (message: string) => { gitIn("add", "-A"); gitIn("commit", "-q", "-m", message); };
const writable = (p: string) => chmodSync(p, statSync(p).mode | 0o200);

let refDeps = "";
/** Copy a previously npm-ci'd node_modules (the first install runs real npm ci). */
const copyDeps = (dir: string) => cpSync(refDeps, join(dir, "node_modules"), { recursive: true, verbatimSymlinks: true });
const newPrefix = () => mkdtempSync(join(scratch, "prefix-"));
const installInto = (prefix: string, extra: Record<string, unknown> = {}) =>
  rr.install({ source, prefix, redisUrl: REDIS_URL, installDependencies: copyDeps, ...extra });

beforeAll(async () => {
  mkdirSync(join(source, "src/mcp-server"), { recursive: true });
  for (const entry of ["dist", "bin", "package.json", "package-lock.json"]) cpSync(join(REPO, entry), join(source, entry), { recursive: true });
  cpSync(join(REPO, "src/mcp-server/lua"), join(source, "src/mcp-server/lua"), { recursive: true });
  spawnSync("git", ["init", "-q", source]);
  commit("fixture");
  // One real lockfile install; later installs copy it to keep the file fast.
  const prefix = newPrefix();
  const result = await rr.install({ source, prefix, redisUrl: REDIS_URL });
  refDeps = join(prefix, "releases", result.id, "node_modules");
}, 240_000);

afterAll(() => rr.removeTree(scratch));

describe("RT-1 release staging", () => {
  it("stages dist, bin, package files, Lua and production deps into an immutable release named <sha>-<digest>", async () => {
    const prefix = newPrefix();
    const result = await installInto(prefix);
    const dir = join(prefix, "releases", result.id);
    const shortSha = gitIn("rev-parse", "--short=10", "HEAD");
    const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
    expect(result.id).toBe(`${shortSha}-${manifest.fullDigest.slice(0, 12)}`); // dependencies included (review C)
    for (const p of ["dist/registered-shell/server.js", "bin/gptqueue-session", "bin/gptqueue-claude-hook", "package.json", "package-lock.json", "node_modules/ioredis/package.json"]) {
      expect(existsSync(join(dir, p)), p).toBe(true);
    }
    for (const lua of readdirSync(join(REPO, "src/mcp-server/lua"))) {
      expect(manifest.files[`dist/mcp-server/lua/${lua}`], lua).toBe(sha256(join(REPO, "src/mcp-server/lua", lua)));
    }
    expect(manifest.files["bin/gptqueue-session"]).toBe(sha256(join(REPO, "bin/gptqueue-session")));
    expect(Object.keys(manifest.files).some((p) => p.startsWith("src/"))).toBe(false);
    expect(statSync(join(dir, "bin/gptqueue-session")).mode & 0o222).toBe(0);
    expect(readdirSync(join(prefix, "releases")).filter((n) => n.includes(".tmp") || n.startsWith(".staging"))).toEqual([]);
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${result.id}`);
    const verify = JSON.parse(readFileSync(join(dir, "verify.json"), "utf8"));
    expect(verify).toMatchObject({ ok: true, integrity: { ok: true }, imports: { ok: true, problems: [] } });
    expect(verify.mcpInitialize).toMatchObject({ ok: true, escapes: [] });
    expect(verify.mcpInitialize.modulesLoaded).toBeGreaterThan(0);
    expect(verify.claudeHook).toMatchObject({ ok: true, escapes: [] });
  }, 60_000);

  it("refuses a checkout with tracked changes unless allowed, and a dist whose Lua is stale", () => {
    const pkg = join(source, "package.json");
    const before = readFileSync(pkg);
    try {
      appendFileSync(pkg, "\n");
      expect(() => rr.checkSource(source)).toThrow(expect.objectContaining({ code: "dirty_checkout" }));
      expect(rr.checkSource(source, { allowDirty: true }).dirty).toBe(true);
    } finally { writeFileSync(pkg, before); }
    const lua = join(source, "src/mcp-server/lua/bounded-push.lua");
    const luaBefore = readFileSync(lua);
    try {
      appendFileSync(lua, "-- newer source\n");
      commit("stale lua");
      expect(() => rr.checkSource(source)).toThrow(expect.objectContaining({ code: "stale_lua" }));
    } finally { writeFileSync(lua, luaBefore); commit("restore lua"); }
  });
});

describe("RT-1 digest checks on an existing release id", () => {
  it("refuses a tampered existing release and one whose recorded digest differs; reuses an intact one", async () => {
    const prefix = newPrefix();
    const first = await installInto(prefix);
    expect((await installInto(prefix)).reused).toBe(true);
    const dir = join(prefix, "releases", first.id);
    const target = join(dir, "dist/registered-shell/server.js");
    const original = readFileSync(target);
    writable(target);
    appendFileSync(target, "// tampered\n");
    await expect(installInto(prefix)).rejects.toMatchObject({ code: "tampered_release" });
    writeFileSync(target, original);
    const manifestPath = join(dir, "manifest.json");
    writable(manifestPath);
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    writeFileSync(manifestPath, JSON.stringify({ ...manifest, sourceDigest: "0".repeat(64) }));
    await expect(installInto(prefix)).rejects.toMatchObject({ code: "digest_mismatch" });
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${first.id}`);
  }, 90_000);
});

describe("RT-1 concurrent installs of the same source", () => {
  it("each stages into its own temporary dir; one publishes releases/<id>, the other reuses it", async () => {
    const prefix = newPrefix();
    const [a, b] = await Promise.all([installInto(prefix), installInto(prefix)]);
    expect(a.id).toBe(b.id);
    expect([a.reused, b.reused].sort()).toEqual([false, true]);
    expect(a.verify.ok && b.verify.ok).toBe(true);
    const left = readdirSync(join(prefix, "releases"));
    expect(left.filter((n) => n.includes(".tmp") || n.startsWith(".staging") || n.endsWith(".owner") || n.includes(".rejected"))).toEqual([]);
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${a.id}`);
  }, 180_000);
});

describe("RT-1 release id covers dependencies", () => {
  it("same commit, different dependency contents: a different release id and a successful install", async () => {
    const prefix = newPrefix();
    const first = await installInto(prefix);
    const altered = (dir: string) => {
      copyDeps(dir);
      const p = join(dir, "node_modules/zod/package.json");
      writable(p);
      appendFileSync(p, "\n");
    };
    const second = await installInto(prefix, { installDependencies: altered });
    expect(second.id).not.toBe(first.id);
    expect(second.id.split("-")[0]).toBe(first.id.split("-")[0]); // same commit
    expect(second.reused).toBeFalsy();
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${second.id}`);
    expect(rr.checkIntegrity(join(prefix, "releases", second.id)).ok).toBe(true);
  }, 120_000);
});

describe("RT-1 verification gate", () => {
  const corrupt = (rel: string, content: string) => (dir: string) => {
    copyDeps(dir);
    const p = join(dir, rel);
    writable(p);
    writeFileSync(p, content);
  };

  it.each([
    ["sidecar entry imports a missing module", "bin/gptqueue-session", '#!/usr/bin/env node\nimport "../dist/registered-shell/missing.js";\n', "imports"],
    ["sidecar never answers initialize", "dist/registered-shell/server.js", "export const runRegisteredShell = () => new Promise(() => setInterval(() => {}, 1000));\n", "mcpInitialize"],
    ["Claude hook hangs past its cold-start bound", "dist/claude/hook-main.js", "setInterval(() => {}, 1000);\n", "claudeHook"],
  ])("never activates a broken release: %s", async (_name, rel, content, failed) => {
    const prefix = newPrefix();
    const good = await installInto(prefix);
    appendFileSync(join(source, "dist/version.js"), `// gate ${failed}\n`);
    commit(`gate ${failed}`); // a new id, so the corrupt install cannot reuse the good release
    const error = await installInto(prefix, { allowDirty: false, installDependencies: corrupt(rel, content), initBoundMs: 2_000, hookBoundMs: 1_500 })
      .then(() => undefined, (e: { code: string; verify: Record<string, { ok: boolean }> }) => e);
    expect(error).toMatchObject({ code: "verify_failed" });
    expect(error!.verify[failed].ok).toBe(false);
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${good.id}`);
    expect(existsSync(join(prefix, "previous"))).toBe(false);
    expect(rr.status(prefix).releases.map((r: { id: string }) => r.id)).toEqual([good.id]);
    expect(readdirSync(join(prefix, "releases")).some((n) => n.includes(".rejected-"))).toBe(true);
  }, 60_000);
});

describe("RT-1 verification fails closed", () => {
  it("rejects a release when the module-resolution trace comes back empty (tracer bypassed)", async () => {
    const prefix = newPrefix();
    const { id } = await installInto(prefix);
    // A node that silently drops the --import tracer: every escape check would pass vacuously.
    const fakeNode = join(scratch, "node-without-tracer");
    writeFileSync(fakeNode, `#!/bin/sh\nshift 2\nexec ${JSON.stringify(process.execPath)} "$@"\n`);
    chmodSync(fakeNode, 0o755);
    const verify = await rr.verifyRelease(join(prefix, "releases", id), { redisUrl: REDIS_URL, node: fakeNode });
    expect(verify.mcpInitialize).toMatchObject({ modulesLoaded: 0, ok: false });
    expect(verify.ok).toBe(false);
  }, 60_000);
});

describe("RT-1 switch, rollback and prune", () => {
  const prefix = newPrefix();
  const ids: string[] = [];
  const nextRelease = async (label: string) => {
    appendFileSync(join(source, "dist/version.js"), `// ${label}\n`);
    commit(label);
    const r = await installInto(prefix);
    ids.push(r.id);
    return r;
  };

  it("switches atomically: a concurrent reader always sees current resolve to a complete release", async () => {
    await nextRelease("A");
    const reader = spawn(process.execPath, ["-e", `
      const fs = require("fs"); const path = require("path");
      const cur = path.join(${JSON.stringify(prefix)}, "current");
      let reads = 0, failures = 0; const seen = new Set();
      const end = Date.now() + 6000;
      while (Date.now() < end) {
        try {
          const m = JSON.parse(fs.readFileSync(path.join(cur, "manifest.json"), "utf8"));
          fs.statSync(path.join(cur, "bin/gptqueue-session")); fs.statSync(path.join(cur, "verify.json"));
          seen.add(m.id); reads++;
        } catch (e) { failures++; }
      }
      process.stdout.write(JSON.stringify({ reads, failures, seen: [...seen] }));
    `], { stdio: ["ignore", "pipe", "inherit"] });
    let out = "";
    reader.stdout.on("data", (c) => { out += c; });
    const done = new Promise((r) => reader.once("exit", r));
    const b = await nextRelease("B");
    for (let i = 0; i < 200; i += 1) rr.rollback(prefix);
    await done;
    const result = JSON.parse(out);
    expect(result.failures).toBe(0);
    expect(result.reads).toBeGreaterThan(100);
    expect(result.seen).toEqual(expect.arrayContaining([ids[0], b.id]));
    expect(lstatSync(join(prefix, "current")).isSymbolicLink()).toBe(true);
    expect(existsSync(join(prefix, "current.new"))).toBe(false);
  }, 90_000);

  it("rollback swaps current and previous; it refuses without a previous release", () => {
    expect(rr.pointerTarget(prefix, "current")).toBe(ids[1]);
    expect(rr.rollback(prefix)).toEqual({ current: ids[0], previous: ids[1] });
    expect(readlinkSync(join(prefix, "current"))).toBe(`releases/${ids[0]}`);
    expect(rr.rollback(prefix)).toEqual({ current: ids[1], previous: ids[0] });
    const lone = newPrefix();
    expect(() => rr.rollback(lone)).toThrow(expect.objectContaining({ code: "no_previous" }));
  });

  it("prune removes the oldest releases but never current or previous", async () => {
    await nextRelease("C");
    expect(rr.status(prefix)).toMatchObject({ current: ids[2], previous: ids[1] });
    rr.rollback(prefix); rr.rollback(prefix); // C -> B -> C: previous is B
    rr.setPointer(prefix, "previous", ids[0]); // make the OLDEST release previous
    expect(rr.prune(prefix, 1)).toEqual({ removed: [ids[1]], reclaimed: [], kept: [ids[0], ids[2]] });
    expect(rr.prune(prefix, 0).removed).toEqual([]);
    expect(existsSync(join(prefix, "releases", ids[0]!))).toBe(true);
    expect(existsSync(join(prefix, "releases", ids[2]!))).toBe(true);
  }, 60_000);
});

describe("RT-1 staging left by a killed install (LC-F5)", () => {
  // A pid that cannot be alive: a child that already exited and was reaped.
  const deadPid = () => spawnSync(process.execPath, ["-e", ""]).pid!;
  const stage = (prefix: string, name: string, owner?: number) => {
    const dir = join(prefix, "releases", name);
    mkdirSync(join(dir, "node_modules/x"), { recursive: true });
    writeFileSync(join(dir, "node_modules/x/index.js"), "partial");
    if (owner !== undefined) writeFileSync(`${dir}.owner`, JSON.stringify({ pid: owner, createdAt: new Date().toISOString() }));
    return dir;
  };

  it("status reports, and prune reclaims, staging owned by a dead pid; a live owner's staging is kept", () => {
    const prefix = newPrefix();
    const stale = stage(prefix, ".staging-dead01", deadPid());
    const staleTmp = stage(prefix, "abc-def.tmp", deadPid());
    const live = stage(prefix, ".staging-live01", process.pid);
    const fresh = stage(prefix, ".staging-nomark"); // no marker, younger than the threshold
    const reported = rr.status(prefix).staleStaging.map((r: { name: string; reason: string }) => [r.name, r.reason]);
    expect(reported.sort()).toEqual([[".staging-dead01", "owner_dead"], ["abc-def.tmp", "owner_dead"]]);
    expect(rr.status(prefix).releases).toEqual([]); // markers are never mistaken for releases
    expect([...rr.prune(prefix, 5).reclaimed].sort()).toEqual([".staging-dead01", "abc-def.tmp"]);
    for (const gone of [stale, staleTmp, `${stale}.owner`, `${staleTmp}.owner`]) expect(existsSync(gone), gone).toBe(false);
    for (const kept of [live, fresh]) expect(existsSync(kept), kept).toBe(true);
    // Without a marker, only the age threshold reclaims it.
    expect(rr.findStaleStaging(prefix, { now: Date.now() + rr.STALE_STAGING_MS + 1_000 }).map((r: { name: string }) => r.name)).toEqual([".staging-nomark"]);
  });

  it("an in-progress <id>.tmp-<hex> with a live owner is neither listed as a release nor pruned; a dead one is reclaimed", () => {
    const prefix = newPrefix();
    const live = stage(prefix, "abc-def.tmp-0a1b2c3d4e5f", process.pid);
    const dead = stage(prefix, "abc-def.tmp-ffeeddccbbaa", deadPid());
    const s = rr.status(prefix);
    expect(s.releases).toEqual([]);
    expect(s.staleStaging.map((r: { name: string; reason: string }) => [r.name, r.reason])).toEqual([["abc-def.tmp-ffeeddccbbaa", "owner_dead"]]);
    const pruned = rr.prune(prefix, 0);
    expect(pruned.removed).toEqual([]);
    expect(pruned.reclaimed).toEqual(["abc-def.tmp-ffeeddccbbaa"]);
    expect(existsSync(live)).toBe(true);
    expect(existsSync(`${live}.owner`)).toBe(true);
    expect(existsSync(dead)).toBe(false);
  });

  it("install reclaims stale staging before staging a new release", async () => {
    const prefix = newPrefix();
    const stale = stage(prefix, ".staging-dead02", deadPid());
    await installInto(prefix);
    expect(existsSync(stale)).toBe(false);
    expect(readdirSync(join(prefix, "releases")).filter((n) => n.startsWith(".") || n.includes(".tmp") || n.endsWith(".owner"))).toEqual([]);
  }, 60_000);
});

describe("RT-1 hot-swap plan", () => {
  it("prints exact before/after edits for every entry point and writes nothing", () => {
    const home = mkdtempSync(join(scratch, "home-"));
    const node = "/opt/node/bin/node";
    const files: Record<string, string> = {
      ".config/systemd/user/mcp-gptqueue.service": `[Service]\nWorkingDirectory=/work/gq\nExecStart=${node} /work/gq/dist/transports/http.js --port 8101\nEnvironment=NOTE=/work/gq/untouched\n`,
      ".codex/config.toml": `[mcp_servers.other]\nargs = ["/work/gq/bin/other"]\n\n[mcp_servers."gptqueue-shared"]\ncommand = "${node}"\nargs = ["/work/gq/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis://127.0.0.1:6379/0"]\nrequired = true\n`,
      ".codex/hooks.json": `{\n  "hooks": { "SessionStart": [ { "hooks": [ { "type": "command", "command": "${node} /work/gq/bin/gptqueue-codex-hook", "timeout": 35 } ] } ] }\n}\n`,
      ".pi/agent/extensions/gptqueue-registration.ts": `export default async function (pi) {\n  try {\n    const { createRegisteredPiExtension } = await import("file:///work/gq/dist/registered-shell/pi-extension.js");\n    await createRegisteredPiExtension({"redisUrl": "redis://127.0.0.1:6379/0", "nodePath": "${node}", "sidecarPath": "/work/gq/bin/gptqueue-session"})(pi);\n  } catch (error) {\n    process.exit(1);\n  }\n}\n`,
      ".local/bin/gptqueue-claude-hook": "#!/usr/bin/env bash\nexec redis-cli\n",
      ".claude/settings.json": JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: "command", command: `${home}/.local/bin/gptqueue-claude-hook stop`, timeout: 10 }] }] } }),
    };
    for (const [rel, content] of Object.entries(files)) { mkdirSync(join(home, rel, ".."), { recursive: true }); writeFileSync(join(home, rel), content); }
    const digests = () => Object.keys(files).map((rel) => sha256(join(home, rel)));
    const before = digests();
    const plan = buildPlan({ home, prefix: "/srv/x/gptqueue" });
    const text = renderPlan(plan);
    expect(digests()).toEqual(before);
    expect(plan.fromRoot).toBe("/work/gq");
    expect(plan.node).toBe(node);
    // Unrelated references (another server's args, an Environment= line) are left alone but flagged.
    expect(plan.leftovers).toEqual([join(home, ".config/systemd/user/mcp-gptqueue.service"), join(home, ".codex/config.toml")]);
    expect(text).toContain("# WARNING: still references /work/gq");
    const by = (rel: string) => plan.changes.find((c: { path: string }) => c.path === join(home, rel));
    expect(by(".config/systemd/user/mcp-gptqueue.service").after).toBe(`[Service]\nWorkingDirectory=/srv/x/gptqueue/current\nExecStart=${node} /srv/x/gptqueue/current/dist/transports/http.js --port 8101\nEnvironment=NOTE=/work/gq/untouched\n`);
    const codex = by(".codex/config.toml").after;
    expect(codex).toContain('args = ["/work/gq/bin/other"]');
    expect(codex).toContain('args = ["/srv/x/gptqueue/current/bin/gptqueue-session", "--client", "codex"');
    expect(by(".codex/hooks.json").after).toContain(`"command": "${node} /srv/x/gptqueue/current/bin/gptqueue-codex-hook"`);
    const stub = by(".pi/agent/extensions/gptqueue-registration.ts").after;
    expect(stub).toContain('const DEFAULT_RUNTIME_ROOT = "/srv/x/gptqueue/current"');
    expect(stub).toContain('"redisUrl": "redis://127.0.0.1:6379/0"');
    expect(stub).not.toContain("process.exit");
    expect(by(".local/bin/gptqueue-claude-hook").after).toBe(`#!/bin/sh\n# Managed by gptqueue install-runtime plan: run the installed release on local disk.\nexec ${node} /srv/x/gptqueue/current/bin/gptqueue-claude-hook "$@"\n`);
    expect(by(".local/bin/gptqueue-claude-mcp").status).toBe("missing");
    expect(by(".claude/settings.json").notes[0]).toContain("timeout 10 s, unchanged");
    expect(text).toContain(`- ExecStart=${node} /work/gq/dist/transports/http.js --port 8101`);
    expect(text).toContain(`+ ExecStart=${node} /srv/x/gptqueue/current/dist/transports/http.js --port 8101`);
  });
});
