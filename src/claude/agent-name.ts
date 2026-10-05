/**
 * The name a Claude Code session uses when nothing better is known: a port of
 * the original gptqueue-claude-name script, behaviour preserved.
 * Precedence: GPTQ_AGENT_NAME, then the longest matching prefix in the names
 * file (TSV: prefix<TAB>name), then claude-<basename of the git top level (or
 * the directory)>, lowercased with [^a-z0-9-] mapped to '-'.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join, resolve } from "node:path";

export const namesFile = (env: NodeJS.ProcessEnv = process.env): string =>
  env.GPTQ_CLAUDE_NAMES_FILE || join(env.XDG_CONFIG_HOME || join(env.HOME || homedir(), ".config"), "gptqueue", "claude-agent-names.tsv");

/** Longest-prefix match over TSV lines; blank and '#' lines are ignored. */
export const matchNamesFile = (dir: string, tsv: string): string | null => {
  let best: { length: number; name: string } | null = null;
  for (const line of tsv.split("\n")) {
    const [rawPrefix = "", name = ""] = line.split("\t");
    if (rawPrefix === "" || rawPrefix.startsWith("#")) continue;
    const prefix = rawPrefix.replace(/\/$/, "");
    if ((dir === prefix || dir.startsWith(`${prefix}/`)) && prefix.length > (best?.length ?? 0)) {
      best = { length: prefix.length, name: name.trim() };
    }
  }
  return best && best.name ? best.name : null;
};

export const sanitizeBase = (base: string): string => base.toLowerCase().replace(/[^a-z0-9-]/g, "-");

const gitTop = (dir: string): string => {
  try {
    return execFileSync("git", ["-C", dir, "rev-parse", "--show-toplevel"], { stdio: ["ignore", "pipe", "ignore"] }).toString().trim() || dir;
  } catch { return dir; }
};

export const resolveAgentName = (dirArg: string | undefined, env: NodeJS.ProcessEnv = process.env): string => {
  if (env.GPTQ_AGENT_NAME) return env.GPTQ_AGENT_NAME;
  const dir = resolve(dirArg || process.cwd());
  let tsv: string | null = null;
  try { tsv = readFileSync(namesFile(env), "utf8"); } catch { /* no names file */ }
  const mapped = tsv === null ? null : matchNamesFile(dir, tsv);
  if (mapped) return mapped;
  return `claude-${sanitizeBase(basename(gitTop(dir))) || "root"}`;
};
