/**
 * Legacy identity confirmation for Claude sessions whose MCP server predates
 * session records (7347a85 and earlier). Their wrapper did
 * `export GPTQ_AGENT_NAME=...; exec node .../dist/mcp-server/index.js`, so the
 * old server is a DIRECT child of its Claude process and carries the name it
 * registered in its environment.
 *
 * Search: walk this process's ancestors nearest-first; at each ancestor, the
 * candidates are its direct children that are owned by this uid, run
 * `.../dist/mcp-server/index.js`, and have GPTQ_AGENT_NAME in their environ.
 * The first ancestor with any candidate decides: exactly one candidate means
 * a confirmed name (origin legacy_env); several mean ambiguity (unconfirmed).
 * Another Claude's server is a child of that other Claude, which is not our
 * ancestor, so it is never considered. Other users' processes are never read.
 * GPTQ_CLAUDE_LEGACY_SCAN=off disables the scan.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { readProcess, type ProcessInfo } from "./session-record.js";

export type ProcReader = Readonly<{
  pids(): readonly number[];
  stat(pid: number): ProcessInfo | null;
  uid(pid: number): number | null;
  cmdline(pid: number): readonly string[];
  environ(pid: number): Readonly<Record<string, string>>;
}>;

const splitNul = (text: string): string[] => text.split("\0").filter((part) => part.length > 0);

export const realProcReader: ProcReader = Object.freeze({
  pids: () => { try { return readdirSync("/proc").filter((name) => /^\d+$/.test(name)).map(Number); } catch { return []; } },
  stat: (pid: number) => readProcess(pid),
  uid: (pid: number) => { try { return statSync(`/proc/${pid}`).uid; } catch { return null; } },
  cmdline: (pid: number) => { try { return splitNul(readFileSync(`/proc/${pid}/cmdline`, "utf8")); } catch { return []; } },
  environ: (pid: number) => {
    try {
      return Object.fromEntries(splitNul(readFileSync(`/proc/${pid}/environ`, "utf8")).flatMap((entry) => {
        const eq = entry.indexOf("=");
        return eq > 0 ? [[entry.slice(0, eq), entry.slice(eq + 1)] as const] : [];
      }));
    } catch { return {}; }
  },
});

export type LegacyLookup =
  | Readonly<{ kind: "confirmed"; agent: string; server_pid: number; claude_pid: number }>
  | Readonly<{ kind: "ambiguous"; agents: readonly string[]; claude_pid: number }>
  | Readonly<{ kind: "none" }>;

const isMcpServer = (argv: readonly string[]): boolean =>
  argv.some((arg) => arg.endsWith("dist/mcp-server/index.js"));

/** Find the legacy MCP server among the direct children of this process's nearest ancestor that has one. */
export const findLegacyIdentity = (
  chain: readonly ProcessInfo[],
  reader: ProcReader = realProcReader,
  myUid: number | null = process.getuid?.() ?? null,
): LegacyLookup => {
  if (chain.length === 0) return { kind: "none" };
  const self = chain[0]!.pid;
  const ancestors = chain.slice(1).map((p) => p.pid);
  if (ancestors.length === 0) return { kind: "none" };
  const position = new Map(ancestors.map((pid, index) => [pid, index]));
  const byAncestor = new Map<number, { pid: number; agent: string }[]>();
  for (const pid of reader.pids()) {
    if (pid === self) continue;
    // Identity first (start time, ppid, uid), then the contents, then the
    // identity again: if the pid exited and was reused between reads, the
    // cmdline/environ could belong to a different process than the stat.
    const uid = reader.uid(pid);
    if (myUid !== null && uid !== myUid) continue; // never read other users' processes
    const stat = reader.stat(pid);
    if (!stat || !position.has(stat.ppid)) continue;
    const isServer = isMcpServer(reader.cmdline(pid));
    const agent = isServer ? reader.environ(pid).GPTQ_AGENT_NAME : undefined;
    if (!agent) continue;
    const again = reader.stat(pid);
    if (!again || again.start !== stat.start || again.ppid !== stat.ppid || reader.uid(pid) !== uid) continue;
    byAncestor.set(stat.ppid, [...(byAncestor.get(stat.ppid) ?? []), { pid, agent }]);
  }
  for (const ancestor of ancestors) {
    const found = byAncestor.get(ancestor);
    if (!found) continue;
    return found.length === 1
      ? { kind: "confirmed", agent: found[0]!.agent, server_pid: found[0]!.pid, claude_pid: ancestor }
      : { kind: "ambiguous", agents: found.map((f) => f.agent), claude_pid: ancestor };
  }
  return { kind: "none" };
};
