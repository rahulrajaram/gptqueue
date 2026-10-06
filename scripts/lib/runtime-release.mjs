// Immutable, verified gptqueue runtime releases on local disk (criterion RT-1).
//
// Layout under <prefix>:
//   releases/<id>/           read-only release: dist/, bin/, package*.json, node_modules/,
//                            manifest.json (per-file sha256), verify.json (gate result)
//   releases/<id>.tmp/       staging; never referenced by current/previous
//   releases/<id>.rejected-<ts>/  a staged release that failed verification (kept as evidence)
//   releases/.staging-*/     scratch copy before the id is known
//   releases/<name>.owner    {pid, createdAt} of the install that owns a .staging-* or
//                            <id>.tmp entry; install and prune reclaim entries whose
//                            owner is dead (or, without a marker, older than a threshold)
//   current -> releases/<id> atomic symlink (rename of current.new over current)
//   previous -> releases/<id>
//
// Release id = <git short sha>-<first 12 hex of the full digest>, computed
// AFTER dependencies are installed. The full digest covers every staged file,
// node_modules included (sorted paths, per-file sha256, symlinks by target),
// so any change in dependency contents is a distinct release instead of a
// collision with an existing id. Nothing is excluded as volatile: a
// non-reproducible native rebuild only costs a new id, never a false match.
// manifest.json records every file so on-disk tampering stays detectable.
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync, cpSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync,
  readlinkSync, realpathSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync,
} from "node:fs";
import { builtinModules } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";

export const STAGED_ENTRIES = Object.freeze(["dist", "bin", "package.json", "package-lock.json"]);
export const LUA_SOURCE_DIR = "src/mcp-server/lua";
export const LUA_DIST_DIR = "dist/mcp-server/lua";
const DEPS_DIR = "node_modules";
const META_FILES = Object.freeze(["manifest.json", "verify.json"]);

export class ReleaseError extends Error {
  /** @param {"dirty_checkout"|"missing_build"|"stale_lua"|"digest_mismatch"|"tampered_release"|"verify_failed"|"no_previous"|"not_installed"|"bad_argument"} code */
  constructor(code, message) { super(message); this.name = "ReleaseError"; this.code = code; }
}

const sha256 = (data) => createHash("sha256").update(data).digest("hex");

/** Sorted relative POSIX paths of every regular file and symlink below dir. */
export function listFiles(dir, skip = () => false) {
  const out = [];
  const walk = (rel) => {
    for (const entry of readdirSync(join(dir, rel), { withFileTypes: true })) {
      const child = rel ? `${rel}/${entry.name}` : entry.name;
      if (skip(child)) continue;
      if (entry.isDirectory()) walk(child);
      else if (entry.isFile() || entry.isSymbolicLink()) out.push(child);
    }
  };
  walk("");
  return out.sort();
}

const fileDigest = (path) => {
  const st = lstatSync(path);
  return st.isSymbolicLink() ? `link:${readlinkSync(path)}` : sha256(readFileSync(path));
};

const digestOf = (files) => sha256(Object.entries(files).map(([p, h]) => `${p}\0${h}\n`).join(""));

/** Per-file sha256 manifest of a release directory (meta files excluded). */
export function computeManifest(dir) {
  const paths = listFiles(dir, (rel) => META_FILES.includes(rel));
  const files = Object.fromEntries(paths.map((p) => [p, fileDigest(join(dir, p))]));
  const source = Object.fromEntries(Object.entries(files).filter(([p]) => !p.startsWith(`${DEPS_DIR}/`)));
  return Object.freeze({ files, sourceDigest: digestOf(source), fullDigest: digestOf(files) });
}

export const releaseId = (shortSha, fullDigest) => `${shortSha}-${fullDigest.slice(0, 12)}`;

const git = (source, args) => {
  const r = spawnSync("git", ["-C", source, ...args], { encoding: "utf8" });
  if (r.status !== 0) throw new ReleaseError("bad_argument", `git ${args.join(" ")} failed: ${r.stderr.trim()}`);
  return r.stdout;
};

/** Refuse a checkout with tracked changes; untracked files never reach the release. */
export function checkSource(source, { allowDirty = false } = {}) {
  const tracked = git(source, ["status", "--porcelain", "--untracked-files=no"]).trim();
  if (tracked && !allowDirty) throw new ReleaseError("dirty_checkout", `tracked changes in ${source}:\n${tracked}`);
  for (const entry of STAGED_ENTRIES) {
    if (!existsSync(join(source, entry))) throw new ReleaseError("missing_build", `${entry} missing in ${source}; run npm run build`);
  }
  // The build copies Lua into dist; refuse a stale dist whose Lua differs from source.
  for (const name of readdirSync(join(source, LUA_SOURCE_DIR)).filter((n) => n.endsWith(".lua"))) {
    const built = join(source, LUA_DIST_DIR, name);
    if (!existsSync(built) || fileDigest(built) !== fileDigest(join(source, LUA_SOURCE_DIR, name))) {
      throw new ReleaseError("stale_lua", `${LUA_DIST_DIR}/${name} differs from ${LUA_SOURCE_DIR}/${name}; run npm run build`);
    }
  }
  return Object.freeze({ shortSha: git(source, ["rev-parse", "--short=10", "HEAD"]).trim(), dirty: tracked !== "" });
}

/**
 * Production dependency install used by the CLI (lockfile-exact). The root
 * package's own lifecycle scripts (prepare = tsc build) must not run in a
 * release, so install with --ignore-scripts and then `npm rebuild`, which runs
 * only the dependencies' install scripts (node-pty).
 */
export function npmCiInstall(dir) {
  const env = { ...process.env, npm_config_update_notifier: "false" };
  for (const args of [["ci", "--omit=dev", "--prefer-offline", "--no-audit", "--no-fund", "--ignore-scripts"], ["rebuild", "--no-audit", "--no-fund"]]) {
    const r = spawnSync("npm", args, { cwd: dir, encoding: "utf8", env });
    if (r.status !== 0) throw new ReleaseError("missing_build", `npm ${args[0]} failed in ${dir}:\n${r.stderr.slice(-2000)}`);
  }
}

const setReadOnly = (dir, readOnly) => {
  const walk = (path) => {
    const st = lstatSync(path);
    if (st.isSymbolicLink()) return;
    if (st.isDirectory()) {
      if (!readOnly) chmodSync(path, st.mode | 0o200);
      for (const name of readdirSync(path)) walk(join(path, name));
      if (readOnly) chmodSync(path, st.mode & ~0o222);
    } else chmodSync(path, readOnly ? st.mode & ~0o222 : st.mode | 0o200);
  };
  if (existsSync(dir)) walk(dir);
};

export const removeTree = (dir) => { setReadOnly(dir, false); rmSync(dir, { recursive: true, force: true }); };

const paths = (prefix) => Object.freeze({
  releases: join(prefix, "releases"),
  current: join(prefix, "current"),
  previous: join(prefix, "previous"),
});

/** The release id a pointer (current/previous) targets, or undefined. */
export function pointerTarget(prefix, name) {
  const link = join(prefix, name);
  try { return readlinkSync(link).split("/").pop(); } catch { return undefined; }
}

/** Atomically point <prefix>/<name> at releases/<id> (symlink + rename). */
export function setPointer(prefix, name, id) {
  const link = join(prefix, name);
  const staging = `${link}.new`;
  rmSync(staging, { force: true });
  symlinkSync(join("releases", id), staging);
  renameSync(staging, link);
}

// ---------------------------------------------------------------- staging ownership

const OWNER_SUFFIX = ".owner";
/** Staging entries without a readable owner marker are reclaimed only after this age. */
export const STALE_STAGING_MS = 60 * 60 * 1000;
const ownerMarker = (dir) => `${dir}${OWNER_SUFFIX}`;
const writeOwner = (dir) => writeFileSync(ownerMarker(dir), `${JSON.stringify({ pid: process.pid, createdAt: new Date().toISOString() })}\n`);
const dropOwner = (dir) => rmSync(ownerMarker(dir), { force: true });
const isStagingName = (name) => (name.startsWith(".staging-") || name.endsWith(".tmp")) && !name.endsWith(OWNER_SUFFIX);

/** True when pid names a live process (EPERM still means it exists). */
export const pidAlive = (pid) => {
  try { process.kill(pid, 0); return true; } catch (error) { return error?.code === "EPERM"; }
};

const readOwner = (dir) => {
  try {
    const owner = JSON.parse(readFileSync(ownerMarker(dir), "utf8"));
    return Number.isInteger(owner?.pid) && owner.pid > 0 ? owner : undefined;
  } catch { return undefined; }
};

/**
 * Staging entries (.staging-* and <id>.tmp) left by installs that are no
 * longer running. An entry is stale when its owner marker names a dead pid,
 * or when it has no usable marker and is older than staleAfterMs. A live
 * owner is never reclaimed.
 */
export function findStaleStaging(prefix, { now = Date.now(), staleAfterMs = STALE_STAGING_MS, isAlive = pidAlive } = {}) {
  const { releases } = paths(prefix);
  if (!existsSync(releases)) return Object.freeze([]);
  const rows = readdirSync(releases).filter(isStagingName).flatMap((name) => {
    const dir = join(releases, name);
    const owner = readOwner(dir);
    const ageMs = Math.max(0, now - lstatSync(dir).mtimeMs);
    if (owner) return isAlive(owner.pid) ? [] : [{ name, ownerPid: owner.pid, ageMs, reason: "owner_dead" }];
    return ageMs > staleAfterMs ? [{ name, ownerPid: undefined, ageMs, reason: "no_owner_expired" }] : [];
  });
  return Object.freeze(rows.map((r) => Object.freeze(r)));
}

/** Remove the entries findStaleStaging reports, with their markers; returns their names. */
export function reclaimStaleStaging(prefix, options = {}) {
  const { releases } = paths(prefix);
  const stale = findStaleStaging(prefix, options);
  for (const { name } of stale) { removeTree(join(releases, name)); dropOwner(join(releases, name)); }
  return Object.freeze(stale.map((r) => r.name));
}

/** Stage a release from a built checkout into releases/<id>.tmp. */
export function stageRelease({ source, prefix, shortSha, installDependencies = npmCiInstall }) {
  const { releases } = paths(prefix);
  mkdirSync(releases, { recursive: true });
  const scratch = mkdtempSync(join(releases, ".staging-"));
  writeOwner(scratch);
  let tmp;
  try {
    for (const entry of STAGED_ENTRIES) cpSync(join(source, entry), join(scratch, entry), { recursive: true, verbatimSymlinks: true });
    installDependencies(scratch);
    const id = releaseId(shortSha, computeManifest(scratch).fullDigest);
    tmp = join(releases, `${id}.tmp`);
    removeTree(tmp);
    writeOwner(tmp);
    renameSync(scratch, tmp);
    dropOwner(scratch);
    const manifest = { id, shortSha, createdAt: new Date().toISOString(), ...computeManifest(tmp) };
    writeFileSync(join(tmp, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
    return Object.freeze({ id, tmp, manifest });
  } catch (error) {
    removeTree(scratch);
    dropOwner(scratch);
    if (tmp) { removeTree(tmp); dropOwner(tmp); }
    throw error;
  }
}

/** Recompute an installed release's manifest and compare it to its manifest.json. */
export function checkIntegrity(dir) {
  const recorded = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
  const actual = computeManifest(dir);
  const changed = [...new Set([...Object.keys(recorded.files), ...Object.keys(actual.files)])]
    .filter((p) => recorded.files[p] !== actual.files[p]);
  return Object.freeze({ ok: changed.length === 0 && recorded.fullDigest === actual.fullDigest, changed, recorded, actual });
}

// ---------------------------------------------------------------- verification

const IMPORT_RE = /(?:^|[\s;])(?:import|export)\s*(?:[^'";]*?\sfrom\s*)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/gm;
const isBuiltin = (spec) => spec.startsWith("node:") || builtinModules.includes(spec.split("/")[0]);
const packageName = (spec) => (spec.startsWith("@") ? spec.split("/").slice(0, 2) : spec.split("/").slice(0, 1)).join("/");
const inside = (root, path) => path === root || path.startsWith(root + sep);

/** Node entry points in bin/ (shebang "node"). */
export const nodeEntryPoints = (dir) => readdirSync(join(dir, "bin"))
  .filter((name) => /^#!.*\bnode\b/.test(readFileSync(join(dir, "bin", name), "utf8").split("\n", 1)[0]))
  .sort();

/**
 * Static check (a): every relative import reachable from a bin entry resolves
 * to a file inside the release, and every package import is installed in the
 * release's own node_modules.
 */
export function checkEntryImports(dir) {
  const root = realpathSync(dir);
  const problems = [];
  const seen = new Set();
  const visit = (file) => {
    if (seen.has(file)) return;
    seen.add(file);
    const text = readFileSync(file, "utf8");
    for (const match of text.matchAll(IMPORT_RE)) {
      const spec = match[1] ?? match[2];
      if (isBuiltin(spec)) continue;
      if (spec.startsWith(".") || spec.startsWith("/")) {
        const target = resolve(dirname(file), spec);
        if (!inside(root, target)) problems.push(`${relative(root, file)} imports ${spec} outside the release`);
        else if (!existsSync(target)) problems.push(`${relative(root, file)} imports missing ${spec}`);
        else if (/\.(m?js)$/.test(target)) visit(target);
      } else {
        const pkg = join(root, DEPS_DIR, packageName(spec));
        if (!existsSync(pkg)) problems.push(`${relative(root, file)} imports ${spec}, not installed in the release`);
        else if (!inside(root, realpathSync(pkg))) problems.push(`${spec} resolves outside the release`);
      }
    }
  };
  const entries = nodeEntryPoints(root);
  for (const name of entries) visit(join(root, "bin", name));
  return Object.freeze({ ok: problems.length === 0 && entries.length > 0, entries, modules: seen.size, problems });
}

const TRACE_HOOK = `import { registerHooks } from "node:module";
import { appendFileSync } from "node:fs";
const out = process.env.GPTQ_RESOLVE_TRACE;
registerHooks({ resolve(specifier, context, next) {
  const result = next(specifier, context);
  try { appendFileSync(out, result.url + "\\n"); } catch {}
  return result;
} });
`;

/** Runtime check for (a): every module a traced run loaded came from the release. */
const traceEscapes = (root, traceFile) => {
  const urls = existsSync(traceFile) ? readFileSync(traceFile, "utf8").split("\n").filter(Boolean) : [];
  const escapes = [...new Set(urls.filter((u) => u.startsWith("file:")).map((u) => fileURLToPath(u)))]
    .filter((p) => !inside(root, existsSync(p) ? realpathSync(p) : p));
  return { loaded: urls.length, escapes };
};

/** Verification children still running, so a signalled installer can kill them. */
const verificationChildren = new Set();

/** Kill every running verification child (used by install-runtime's signal handlers). */
export function killVerificationChildren(signal = "SIGKILL") {
  for (const child of verificationChildren) { try { child.kill(signal); } catch { /* already gone */ } }
}

const timedChild = ({ command, args, cwd, env, input, boundMs, until }) => new Promise((resolvePromise) => {
  const started = process.hrtime.bigint();
  const child = spawn(command, args, { cwd, env, stdio: ["pipe", "pipe", "pipe"] });
  verificationChildren.add(child);
  child.once("close", () => verificationChildren.delete(child));
  let stdout = ""; let stderr = ""; let settled = false;
  const finish = (ok, reason) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    const ms = Number(process.hrtime.bigint() - started) / 1e6;
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    resolvePromise({ ok, reason, ms: Math.round(ms * 10) / 10, stdout: stdout.slice(0, 2000), stderr: stderr.slice(-2000) });
  };
  const timer = setTimeout(() => finish(false, `no answer within ${boundMs} ms`), boundMs);
  child.stdout.on("data", (chunk) => { stdout += chunk; if (until?.(stdout)) finish(true, "answered"); });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", (error) => finish(false, String(error)));
  child.once("exit", (code, signal) => {
    if (until) finish(false, `exited (${code ?? signal}) before answering`);
    else finish(code === 0, `exit ${code ?? signal}`);
  });
  child.stdin.on("error", () => undefined);
  child.stdin.end(input);
});

const INITIALIZE = `${JSON.stringify({
  jsonrpc: "2.0", id: 1, method: "initialize",
  params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "gptqueue-release-verify", version: "1" } },
})}\n`;

const answeredInitialize = (stdout) => stdout.split("\n").some((line) => {
  try { const m = JSON.parse(line); return m.id === 1 && m.result?.serverInfo !== undefined; } catch { return false; }
});

/**
 * The verification gate. Runs every check against the staged directory
 * through its own paths (no reference to the source checkout), with a private
 * HOME and the given private Redis.
 */
export async function verifyRelease(dir, { redisUrl, initBoundMs = 3_000, hookBoundMs = 3_000, node = process.execPath }) {
  const root = realpathSync(dir);
  const scratch = mkdtempSync(join(tmpdir(), "gptqueue-release-verify-"));
  try {
    const hook = join(scratch, "trace-hook.mjs");
    writeFileSync(hook, TRACE_HOOK);
    const integrity = checkIntegrity(root);
    const imports = checkEntryImports(root);
    const run = async (label, options) => {
      const trace = join(scratch, `${label}.trace`);
      const env = { PATH: process.env.PATH, HOME: scratch, REDIS_URL: redisUrl, GPTQ_RESOLVE_TRACE: trace };
      const result = await timedChild({ ...options, command: node, args: ["--import", pathToFileURL(hook).href, ...options.args], cwd: scratch, env });
      const { loaded, escapes } = traceEscapes(root, trace);
      // An empty trace means the tracer failed, not that nothing escaped.
      return { ...result, ok: result.ok && loaded > 0 && escapes.length === 0, modulesLoaded: loaded, escapes };
    };
    const mcpInitialize = await run("session", {
      args: [join(root, "bin/gptqueue-session"), "--client", "codex", "--redis-url", redisUrl],
      input: INITIALIZE, boundMs: initBoundMs, until: answeredInitialize,
    });
    const claudeHook = await run("hook", {
      args: [join(root, "bin/gptqueue-claude-hook"), "session-start"],
      input: JSON.stringify({ cwd: scratch }), boundMs: hookBoundMs,
    });
    const ok = integrity.ok && imports.ok && mcpInitialize.ok && claudeHook.ok;
    return Object.freeze({
      ok, verifiedAt: new Date().toISOString(), node, bounds: { initBoundMs, hookBoundMs },
      integrity: { ok: integrity.ok, changed: integrity.changed.slice(0, 20) },
      imports,
      mcpInitialize: { ok: mcpInitialize.ok, ms: mcpInitialize.ms, reason: mcpInitialize.reason, modulesLoaded: mcpInitialize.modulesLoaded, escapes: mcpInitialize.escapes, stderr: mcpInitialize.ok ? undefined : mcpInitialize.stderr },
      claudeHook: { ok: claudeHook.ok, ms: claudeHook.ms, reason: claudeHook.reason, modulesLoaded: claudeHook.modulesLoaded, escapes: claudeHook.escapes, stderr: claudeHook.ok ? undefined : claudeHook.stderr },
    });
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------- lifecycle

/** Make releases/<id> current; the old current becomes previous. */
export function activate(prefix, id) {
  const before = pointerTarget(prefix, "current");
  if (!existsSync(join(paths(prefix).releases, id, "verify.json"))) throw new ReleaseError("verify_failed", `release ${id} has no verify.json`);
  if (before === id) return Object.freeze({ current: id, previous: pointerTarget(prefix, "previous"), changed: false });
  if (before) setPointer(prefix, "previous", before);
  setPointer(prefix, "current", id);
  return Object.freeze({ current: id, previous: before, changed: true });
}

/**
 * Stage, verify and (only if verification passes) activate a release.
 * An existing release with the same id is reused only if its on-disk content
 * still matches its manifest and its source digest equals the staged one.
 */
export async function install({ source, prefix, allowDirty = false, installDependencies = npmCiInstall, redisUrl, initBoundMs, hookBoundMs, node, activateRelease = true }) {
  const { shortSha } = checkSource(source, { allowDirty });
  reclaimStaleStaging(prefix);
  const staged = stageRelease({ source, prefix, shortSha, installDependencies });
  const final = join(paths(prefix).releases, staged.id);
  if (existsSync(final)) {
    removeTree(staged.tmp);
    dropOwner(staged.tmp);
    const integrity = checkIntegrity(final);
    if (!integrity.ok) throw new ReleaseError("tampered_release", `release ${staged.id} differs from its manifest: ${integrity.changed.slice(0, 5).join(", ")}`);
    if (integrity.recorded.sourceDigest !== staged.manifest.sourceDigest || integrity.recorded.fullDigest !== staged.manifest.fullDigest) {
      throw new ReleaseError("digest_mismatch", `release ${staged.id} exists with digests ${integrity.recorded.sourceDigest}/${integrity.recorded.fullDigest}, staged ${staged.manifest.sourceDigest}/${staged.manifest.fullDigest}`);
    }
    const verify = JSON.parse(readFileSync(join(final, "verify.json"), "utf8"));
    if (!verify.ok) throw new ReleaseError("verify_failed", `release ${staged.id} exists but failed verification`);
    return Object.freeze({ id: staged.id, reused: true, verify, ...(activateRelease ? activate(prefix, staged.id) : {}) });
  }
  let verify;
  try {
    verify = await verifyRelease(staged.tmp, { redisUrl, initBoundMs, hookBoundMs, node });
  } catch (error) {
    // A verifier that throws (rather than returning ok=false) leaves no <id>.tmp behind.
    removeTree(staged.tmp);
    dropOwner(staged.tmp);
    throw error;
  }
  writeFileSync(join(staged.tmp, "verify.json"), `${JSON.stringify(verify, null, 2)}\n`);
  if (!verify.ok) {
    const rejected = `${final}.rejected-${Date.now()}`;
    renameSync(staged.tmp, rejected);
    dropOwner(staged.tmp);
    throw Object.assign(new ReleaseError("verify_failed", `release ${staged.id} failed verification; kept at ${rejected}`), { verify });
  }
  setReadOnly(staged.tmp, true);
  renameSync(staged.tmp, final);
  dropOwner(staged.tmp);
  return Object.freeze({ id: staged.id, reused: false, verify, ...(activateRelease ? activate(prefix, staged.id) : {}) });
}

/** Swap current and previous atomically (each pointer is a rename). */
export function rollback(prefix) {
  const current = pointerTarget(prefix, "current");
  const previous = pointerTarget(prefix, "previous");
  if (!previous) throw new ReleaseError("no_previous", "no previous release to roll back to");
  if (!existsSync(join(paths(prefix).releases, previous))) throw new ReleaseError("no_previous", `previous release ${previous} is missing`);
  setPointer(prefix, "current", previous);
  if (current) setPointer(prefix, "previous", current);
  return Object.freeze({ current: previous, previous: current });
}

const isRelease = (name) => !name.startsWith(".") && !name.endsWith(".tmp") && !name.endsWith(OWNER_SUFFIX) && !name.includes(".rejected-");

export function status(prefix) {
  const { releases } = paths(prefix);
  const names = existsSync(releases) ? readdirSync(releases).filter(isRelease) : [];
  const rows = names.map((id) => {
    const dir = join(releases, id);
    const read = (f) => { try { return JSON.parse(readFileSync(join(dir, f), "utf8")); } catch { return undefined; } };
    const manifest = read("manifest.json"); const verify = read("verify.json");
    return {
      id, createdAt: manifest?.createdAt, mtimeMs: statSync(dir).mtimeMs,
      verified: verify?.ok ?? false, mcpInitializeMs: verify?.mcpInitialize?.ms, claudeHookMs: verify?.claudeHook?.ms,
    };
  }).sort((a, b) => (a.createdAt ?? "").localeCompare(b.createdAt ?? "") || a.mtimeMs - b.mtimeMs);
  return Object.freeze({
    prefix, current: pointerTarget(prefix, "current"), previous: pointerTarget(prefix, "previous"),
    releases: rows.map(({ mtimeMs: _m, ...r }) => r), staleStaging: findStaleStaging(prefix),
  });
}

/** Delete the oldest releases beyond `keep`; never current or previous. */
export function prune(prefix, keep) {
  if (!Number.isInteger(keep) || keep < 0) throw new ReleaseError("bad_argument", "--keep must be a non-negative integer");
  const reclaimed = reclaimStaleStaging(prefix);
  const s = status(prefix);
  const protectedIds = new Set([s.current, s.previous].filter(Boolean));
  const candidates = s.releases.map((r) => r.id).filter((id) => !protectedIds.has(id));
  const excess = Math.max(0, s.releases.length - Math.max(keep, protectedIds.size));
  const removed = candidates.slice(0, excess);
  for (const id of removed) removeTree(join(paths(prefix).releases, id));
  return Object.freeze({ removed, reclaimed, kept: s.releases.map((r) => r.id).filter((id) => !removed.includes(id)) });
}
