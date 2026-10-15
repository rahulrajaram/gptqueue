/**
 * Task ledger: a short record of each `type: "task"` send, so the watchdog can
 * name what a lead is waiting on, plus the answered marker of a request. Both
 * are written by the bounded-push Lua scripts in the same call as the delivery
 * (S8), so delivery and correlation cannot diverge. This module holds the
 * TypeScript side: the subject (passed to the script) and the reader.
 */
import { SESSION_KEYS } from "./keys.js";

/** Same lifetime as the outstanding-reply key (bounded-push.lua: EX 604800). */
export const TASK_LEDGER_TTL_SECONDS = 604800;
export const TASK_SUBJECT_MAX = 160;

export const taskLedgerKey = SESSION_KEYS.taskLedger;

export interface TaskLedgerEntry {
  readonly from: string;
  readonly to: string;
  readonly subject: string;
  readonly sent_at: string;
}

/** Pure: the first 160 characters of the content, collapsed to one line. */
export const taskSubject = (content: string): string =>
  content.replace(/\s+/gu, " ").trim().slice(0, TASK_SUBJECT_MAX);

export const admitTaskLedgerEntry = (raw: string | null): TaskLedgerEntry | null => {
  if (raw === null) return null;
  try {
    const v = JSON.parse(raw) as Record<string, unknown>;
    return typeof v?.from === "string" && typeof v.to === "string" && typeof v.subject === "string" && typeof v.sent_at === "string"
      ? Object.freeze({ from: v.from, to: v.to, subject: v.subject, sent_at: v.sent_at })
      : null;
  } catch {
    return null;
  }
};
