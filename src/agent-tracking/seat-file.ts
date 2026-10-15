/**
 * Seat file admission. The seat file is operator-owned (the skills repo
 * versions it); a session can only hold a seat this file declares (AT9).
 *
 *   { "schema_version": 1,
 *     "seats": [ { "seat_id", "name", "description", "capabilities": [],
 *                  "policy": { "runtimes": [], "cwd_roots": [] } } ] }
 */
import { readFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import type { Result, SeatDeclaration } from "./types.js";

export const SEAT_FILE_SCHEMA_VERSION = 1;
/** sw4rm AgentDescriptor.description limit. */
export const MAX_DESCRIPTION_WORDS = 200;
export const MAX_DESCRIPTION_CHARS = 1600;
export const MAX_NAME_CHARS = 80;
export const MAX_CAPABILITIES = 12;
export const MAX_CAPABILITY_CHARS = 48;
const MAX_POLICY_ENTRIES = 16;
const SEAT_ID = /^[A-Za-z0-9._-]{1,64}$/;

export type SeatFile = Readonly<{ schema_version: 1; seats: readonly SeatDeclaration[] }>;

const invalid = (message: string): Result<never> =>
  Object.freeze({ ok: false, error: Object.freeze({ code: "seat_file_invalid", message }) });

const unavailable = (message: string): Result<never> =>
  Object.freeze({ ok: false, error: Object.freeze({ code: "seat_file_unavailable", message }) });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isStringList = (value: unknown, maxEntries: number, maxChars: number): value is string[] =>
  Array.isArray(value) &&
  value.length <= maxEntries &&
  value.every((entry) => typeof entry === "string" && entry.trim().length > 0 && entry.length <= maxChars);

export const wordCount = (text: string): number => text.split(/\s+/).filter((word) => word.length > 0).length;

/** The first problem with one seat entry, or null when it is admissible. */
const seatProblem = (seat: unknown, index: number): string | null => {
  const where = `seats[${index}]`;
  if (!isRecord(seat)) return `${where} must be an object`;
  const { seat_id, name, description, capabilities, policy } = seat;
  if (typeof seat_id !== "string" || !SEAT_ID.test(seat_id)) return `${where}.seat_id must match ${SEAT_ID.source}`;
  if (typeof name !== "string" || name.trim().length === 0 || name.length > MAX_NAME_CHARS)
    return `${where}.name must be 1..${MAX_NAME_CHARS} characters`;
  if (typeof description !== "string" || description.trim().length === 0) return `${where}.description must be non-empty`;
  if (wordCount(description) > MAX_DESCRIPTION_WORDS || description.length > MAX_DESCRIPTION_CHARS)
    return `${where}.description must be at most ${MAX_DESCRIPTION_WORDS} words and ${MAX_DESCRIPTION_CHARS} characters`;
  if (!isStringList(capabilities, MAX_CAPABILITIES, MAX_CAPABILITY_CHARS))
    return `${where}.capabilities must be at most ${MAX_CAPABILITIES} non-empty strings of at most ${MAX_CAPABILITY_CHARS} characters`;
  if (!isRecord(policy)) return `${where}.policy must be an object`;
  if (!isStringList(policy.runtimes, MAX_POLICY_ENTRIES, 64) || policy.runtimes.length === 0)
    return `${where}.policy.runtimes must list at least one runtime`;
  if (!isStringList(policy.cwd_roots, MAX_POLICY_ENTRIES, 4096) || policy.cwd_roots.length === 0)
    return `${where}.policy.cwd_roots must list at least one directory`;
  if (!policy.cwd_roots.every((root) => isAbsolute(root))) return `${where}.policy.cwd_roots must be absolute paths`;
  return null;
};

const freezeSeat = (seat: SeatDeclaration): SeatDeclaration =>
  Object.freeze({
    seat_id: seat.seat_id,
    name: seat.name,
    description: seat.description,
    capabilities: Object.freeze([...seat.capabilities]),
    policy: Object.freeze({
      runtimes: Object.freeze([...seat.policy.runtimes]),
      cwd_roots: Object.freeze([...seat.policy.cwd_roots]),
    }),
  });

/** Admit and deep-freeze a seat file from an unknown value. Pure. */
export const admitSeatFile = (input: unknown): Result<SeatFile> => {
  if (!isRecord(input) || input.schema_version !== SEAT_FILE_SCHEMA_VERSION)
    return invalid(`schema_version must be exactly ${SEAT_FILE_SCHEMA_VERSION}`);
  if (!Array.isArray(input.seats)) return invalid("seats must be an array");
  const problem = input.seats.map(seatProblem).find((p) => p !== null);
  if (problem) return invalid(problem);
  const seats = (input.seats as SeatDeclaration[]).map(freezeSeat);
  const ids = seats.map((seat) => seat.seat_id);
  const duplicate = ids.find((id, i) => ids.indexOf(id) !== i);
  if (duplicate !== undefined) return invalid(`seat_id '${duplicate}' is declared more than once`);
  return Object.freeze({ ok: true, value: Object.freeze({ schema_version: 1, seats: Object.freeze(seats) }) });
};

export const parseSeatFile = (text: string): Result<SeatFile> => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    return invalid(`seat file is not valid JSON (${(error as Error).message})`);
  }
  return admitSeatFile(parsed);
};

export const loadSeatFile = (path: string): Result<SeatFile> => {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code ?? "read failed";
    return unavailable(`seat file ${path} could not be read (${code})`);
  }
  return parseSeatFile(text);
};

export const defaultSeatFilePath = (env: Readonly<Record<string, string | undefined>>): string =>
  join(env.XDG_CONFIG_HOME || join(env.HOME ?? "", ".config"), "gptqueue", "seats.json");
