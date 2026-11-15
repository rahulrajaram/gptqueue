// Read-only hot-swap plan: the exact host edits that move every gptqueue entry
// point from a checkout (e.g. ~/src/gptqueue) to <prefix>/current.
// Nothing here writes outside the returned plan object.
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
export const CLAUDE_WRAPPERS = Object.freeze(["gptqueue-claude-hook", "gptqueue-claude-mcp", "gptqueue-claude-name", "gptqueue-inbox-peek"]);
const SERVER_NAME = "gptqueue-shared";

const read = (path) => (existsSync(path) ? readFileSync(path, "utf8") : undefined);
const replaceAll = (text, from, to) => text.split(from).join(to);

/** Line-level substitution of `from` by `to`, limited to lines `select` accepts. */
const substituteLines = (text, from, to, select = () => true) => text.split("\n")
  .map((line, index, lines) => (select(line, index, lines) ? replaceAll(line, from, to) : line)).join("\n");

/** Is line `index` inside the TOML table [mcp_servers."<name>"] (or its subtables)? */
const inCodexServerTable = (name) => (_line, index, lines) => {
  for (let i = index; i >= 0; i -= 1) {
    const header = lines[i].match(/^\s*\[([^\]]+)\]\s*(?:#.*)?$/);
    if (header) {
      const path = header[1].replace(/\s+/g, "");
      return path === `mcp_servers."${name}"` || path === `mcp_servers.${name}` || path.startsWith(`mcp_servers."${name}".`) || path.startsWith(`mcp_servers.${name}.`);
    }
  }
  return false;
};

/** Detect the checkout root the host currently runs from (Codex sidecar path). */
export function detectFromRoot(home) {
  const config = read(join(home, ".codex/config.toml")) ?? "";
  const match = config.match(/"([^"]+)\/bin\/gptqueue-session"/);
  return match?.[1];
}

const detectNode = (home) => (read(join(home, ".codex/config.toml")) ?? "").match(/\[mcp_servers\."?gptqueue-shared"?\][^[]*?command\s*=\s*"([^"]+)"/s)?.[1];

const wrapperFor = (node, prefix, name) => `#!/bin/sh
# Managed by gptqueue install-runtime plan: run the installed release on local disk.
exec ${node} ${prefix}/current/bin/${name} "$@"
`;

const piShim = (repoRoot, runtimeRoot, before) => {
  const options = before?.match(/createRegisteredPiExtension\((\{[^)]*?\})\)/)?.[1];
  let parsed;
  try { parsed = options ? JSON.parse(options) : undefined; } catch { parsed = undefined; }
  if (!parsed?.redisUrl || !parsed?.nodePath) return { after: undefined, note: "could not read redisUrl/nodePath from the current stub; regenerate with install-shell-registration.py --runtime-root" };
  const r = spawnSync("python3", [join(repoRoot, "scripts/install-shell-registration.py"), "--print-pi-shim",
    "--runtime-root", runtimeRoot, "--redis-url", parsed.redisUrl, "--node-bin", parsed.nodePath], { encoding: "utf8" });
  return r.status === 0 ? { after: r.stdout } : { after: undefined, note: `stub generator failed: ${r.stderr.trim()}` };
};

/**
 * @returns {{ prefix, fromRoot, node, changes: Array<{ path, kind, status, before?, after?, notes: string[] }> }}
 * kind: "lines" (in-place line edits) | "file" (whole-file replacement) | "review" (no edit, reported for review)
 */
export function buildPlan({ home, prefix, fromRoot = detectFromRoot(home), node = detectNode(home) ?? process.execPath, repoRoot = REPO_ROOT, verify }) {
  if (!fromRoot) throw new Error(`cannot detect the current gptqueue root from ${home}/.codex/config.toml; pass --from-root`);
  const to = `${prefix}/current`;
  const changes = [];
  const add = (path, kind, before, after, notes = []) => changes.push(Object.freeze({
    path, kind, before, after, notes,
    status: before === undefined ? "missing" : after === undefined || after === before ? "unchanged" : "change",
  }));

  const unit = join(home, ".config/systemd/user/mcp-gptqueue.service");
  const unitBefore = read(unit);
  add(unit, "lines", unitBefore, unitBefore && substituteLines(unitBefore, fromRoot, to, (l) => /^(ExecStart|WorkingDirectory)=/.test(l)),
    ["then: systemctl --user daemon-reload && systemctl --user restart mcp-gptqueue.service (operator step)"]);

  const codex = join(home, ".codex/config.toml");
  const codexBefore = read(codex);
  add(codex, "lines", codexBefore, codexBefore && substituteLines(codexBefore, fromRoot, to, (l, i, ls) => /^\s*args\s*=/.test(l) && inCodexServerTable(SERVER_NAME)(l, i, ls)),
    ["affects new Codex sessions only; running sessions keep their sidecar"]);

  const hooks = join(home, ".codex/hooks.json");
  const hooksBefore = read(hooks);
  add(hooks, "lines", hooksBefore, hooksBefore && substituteLines(hooksBefore, fromRoot, to, (l) => /"command"\s*:/.test(l)));

  const stub = join(home, ".pi/agent/extensions/gptqueue-registration.ts");
  const stubBefore = read(stub);
  const shim = stubBefore === undefined ? { after: undefined } : piShim(repoRoot, to, stubBefore);
  add(stub, "file", stubBefore, shim.after, [
    "regenerated by the fixed installer: load failure degrades (status line) instead of process.exit(1); runtime root overridable via GPTQUEUE_RUNTIME_ROOT",
    ...(shim.note ? [shim.note] : []),
  ]);

  for (const name of CLAUDE_WRAPPERS) {
    const path = join(home, ".local/bin", name);
    const before = read(path);
    const notes = [];
    if (before && !before.includes("current/bin/")) notes.push(`replaces the installed ${before.startsWith("#!/usr/bin/env bash") ? "bash" : "older"} wrapper with the release's Node entry point; behaviour follows the release (review)`);
    add(path, "file", before, before === undefined ? undefined : wrapperFor(node, prefix, name), notes);
  }

  const settings = join(home, ".claude/settings.json");
  const settingsBefore = read(settings);
  const hookNotes = [];
  try {
    const parsed = settingsBefore ? JSON.parse(settingsBefore) : {};
    for (const [event, groups] of Object.entries(parsed.hooks ?? {})) {
      for (const group of groups) for (const h of group.hooks ?? []) {
        if (String(h.command ?? "").includes("gptqueue")) {
          const bound = verify?.claudeHook?.ms;
          hookNotes.push(`${event}: "${h.command}" timeout ${h.timeout ?? "default"} s, unchanged${bound !== undefined ? ` (verified cold start ${bound} ms)` : ""}`);
        }
      }
    }
  } catch (error) { hookNotes.push(`cannot parse: ${error.message}`); }
  add(settings, "review", settingsBefore, settingsBefore, hookNotes.length ? hookNotes : ["no gptqueue hooks found"]);

  const leftovers = changes.filter((c) => c.after?.includes(fromRoot)).map((c) => c.path);
  return Object.freeze({ prefix, fromRoot, to, node, changes, leftovers });
}

/** Human-readable plan: line edits as -/+ pairs, file replacements in full. */
export function renderPlan(plan) {
  const out = [`# gptqueue runtime hot-swap plan (read-only; nothing was changed)`,
    `# from: ${plan.fromRoot}`, `# to:   ${plan.to}`, `# node: ${plan.node}`, ""];
  for (const c of plan.changes) {
    out.push(`== ${c.path} [${c.status}]`);
    if (c.status === "change" && c.kind === "lines") {
      const a = c.before.split("\n"); const b = c.after.split("\n");
      a.forEach((line, i) => { if (line !== b[i]) out.push(`@@ line ${i + 1}`, `- ${line}`, `+ ${b[i]}`); });
    } else if (c.status === "change" && c.kind === "file") {
      out.push("--- before", c.before.trimEnd(), "+++ after", c.after.trimEnd());
    }
    for (const note of c.notes) out.push(`# ${note}`);
    out.push("");
  }
  if (plan.leftovers.length) out.push(`# WARNING: still references ${plan.fromRoot}: ${plan.leftovers.join(", ")}`);
  return out.join("\n");
}
