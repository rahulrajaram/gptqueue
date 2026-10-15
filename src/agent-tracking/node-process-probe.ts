/** ProcessProbe for Linux hosts: hostname, PID namespace, and /proc/<pid>/stat start time. */
import { readFileSync, readlinkSync } from "node:fs";
import { hostname } from "node:os";
import type { ProcessObservation, ProcessProbe } from "./ports.js";

/**
 * Parse /proc/<pid>/stat. The comm field may contain spaces and parens, so
 * fields are counted from the last ")": field 3 (state) is index 0 and
 * field 22 (start time in clock ticks since boot) is index 19.
 */
const parseStat = (text: string): Readonly<{ state: string; start: string }> | null => {
  const close = text.lastIndexOf(")");
  if (close < 0) return null;
  const fields = text.slice(close + 2).trim().split(/\s+/);
  const state = fields[0];
  const start = fields[19];
  return state && start ? { state, start } : null;
};

const observe = (pid: number): ProcessObservation => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return { state: "unreadable" };
  let text: string;
  try {
    text = readFileSync(`/proc/${pid}/stat`, "utf8");
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" ? { state: "absent" } : { state: "unreadable" };
  }
  const stat = parseStat(text);
  if (stat === null) return { state: "unreadable" };
  // A zombie (Z) or dead (X) entry has exited; it only awaits reaping by its parent.
  return stat.state === "Z" || stat.state === "X" ? { state: "absent" } : { state: "running", start: stat.start };
};

/** Start time of a local pid (/proc/<pid>/stat field 22), or null when it is not a running process. */
export const readProcessStart = (pid: number): string | null => {
  const seen = observe(pid);
  return seen.state === "running" ? seen.start : null;
};

const pidNamespace = (): string | null => {
  try {
    return readlinkSync("/proc/self/ns/pid");
  } catch {
    return null;
  }
};

/** The first readable, non-empty file among `paths`, trimmed; null when none is. */
const firstLine = (paths: readonly string[]): string | null => {
  for (const path of paths) {
    try {
      const text = readFileSync(path, "utf8").trim();
      if (text.length > 0) return text;
    } catch {
      // try the next source
    }
  }
  return null;
};

export const createNodeProcessProbe = (): ProcessProbe => {
  const local = Object.freeze({
    host: hostname(),
    machine_id: firstLine(["/etc/machine-id", "/var/lib/dbus/machine-id"]),
    boot_id: firstLine(["/proc/sys/kernel/random/boot_id"]),
    pidns: pidNamespace(),
  });
  return Object.freeze({ local: () => local, observe });
};
