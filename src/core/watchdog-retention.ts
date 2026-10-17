/**
 * Watchdog state retention (W2). The escalation log rotates before an append
 * once it reaches a size or its first line reaches an age; each real pass then
 * deletes rotated logs and outbox files older than the retention window.
 * Thresholds are constants, overridable for tests.
 */
import { open, readdir, rename, stat, unlink } from "node:fs/promises";
import { join } from "node:path";

export interface WatchdogRetention {
  readonly maxLogBytes: number;
  readonly maxLogAgeMs: number;
  readonly pruneAgeMs: number;
}

const DAY_MS = 24 * 60 * 60 * 1000;
export const WATCHDOG_RETENTION: WatchdogRetention = Object.freeze({
  maxLogBytes: 50 * 1024 * 1024,
  maxLogAgeMs: 30 * DAY_MS,
  pruneAgeMs: 30 * DAY_MS,
});

const ROTATED = /^escalations-\d{4}-\d{2}-\d{2}(?:-\d+)?\.jsonl$/u;

const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === "ENOENT";

/** The `at` of the log's first line (read from its head only), or null. */
const firstLineAt = async (path: string): Promise<number | null> => {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(65_536);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const first = buffer.toString("utf8", 0, bytesRead).split("\n", 1)[0] ?? "";
    try {
      const at = Date.parse(String((JSON.parse(first) as { at?: unknown }).at));
      return Number.isNaN(at) ? null : at;
    } catch {
      return null;
    }
  } finally {
    await handle.close();
  }
};

/** Pure: whether a log of this size and first-line time is due for rotation. */
export const rotationDue = (size: number, firstAt: number | null, now: Date, retention: WatchdogRetention): boolean =>
  size >= retention.maxLogBytes || (firstAt !== null && now.getTime() - firstAt >= retention.maxLogAgeMs);

/**
 * Rotate `<dir>/escalations.jsonl` to `escalations-<UTC date>.jsonl` (then -1,
 * -2, ... on collision) when it is due. Returns the rotated path, or null.
 */
export const rotateEscalationLog = async (dir: string, now: Date, retention: WatchdogRetention = WATCHDOG_RETENTION): Promise<string | null> => {
  const log = join(dir, "escalations.jsonl");
  let size: number;
  try {
    size = (await stat(log)).size;
  } catch (error) {
    if (missing(error)) return null;
    throw error;
  }
  if (size === 0 || !rotationDue(size, await firstLineAt(log), now, retention)) return null;
  const date = now.toISOString().slice(0, 10);
  const existing = new Set(await readdir(dir));
  let index = 0;
  let name = `escalations-${date}.jsonl`;
  while (existing.has(name)) {
    index += 1;
    name = `escalations-${date}-${index}.jsonl`;
  }
  const target = join(dir, name);
  await rename(log, target);
  return target;
};

/** Delete rotated escalation logs and outbox .eml files older than the retention window. Returns the paths removed. */
export const pruneWatchdogState = async (dir: string, now: Date, retention: WatchdogRetention = WATCHDOG_RETENTION): Promise<string[]> => {
  const cutoff = now.getTime() - retention.pruneAgeMs;
  const candidates = async (folder: string, match: (name: string) => boolean): Promise<string[]> => {
    try {
      return (await readdir(folder)).filter(match).map((name) => join(folder, name));
    } catch (error) {
      if (missing(error)) return [];
      throw error;
    }
  };
  const paths = [
    ...(await candidates(dir, (name) => ROTATED.test(name))),
    ...(await candidates(join(dir, "outbox"), (name) => name.endsWith(".eml"))),
  ];
  const removed: string[] = [];
  for (const path of paths) {
    try {
      const info = await stat(path);
      if (info.isFile() && info.mtimeMs < cutoff) {
        await unlink(path);
        removed.push(path);
      }
    } catch (error) {
      if (!missing(error)) throw error;
    }
  }
  return removed;
};
