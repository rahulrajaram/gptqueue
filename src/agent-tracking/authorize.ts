/** Bind authorization (AT9): only a declared seat, from an allowed runtime and cwd. */
import { posix } from "node:path";
import type { SeatFile } from "./seat-file.js";
import type { Result, SeatDeclaration, SeatErrorCode } from "./types.js";

export interface BindRequest {
  readonly seat_id: string;
  readonly runtime: string;
  readonly cwd: string;
}

const refuse = (code: SeatErrorCode, message: string): Result<never> =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });

/** Normalized absolute path without a trailing slash ("/" stays "/"). */
const normalize = (path: string): string => {
  const normalized = posix.normalize(path);
  return normalized.length > 1 && normalized.endsWith("/") ? normalized.slice(0, -1) : normalized;
};

/** True when `cwd` is `root` or below it, compared by whole path segments. Pure: symlinks are not resolved. */
export const cwdWithin = (cwd: string, root: string): boolean => {
  if (!posix.isAbsolute(cwd) || !posix.isAbsolute(root)) return false;
  const c = normalize(cwd);
  const r = normalize(root);
  return c === r || c.startsWith(r === "/" ? "/" : `${r}/`);
};

export const authorizeBind = (file: SeatFile, request: BindRequest): Result<SeatDeclaration> => {
  const seat = file.seats.find((candidate) => candidate.seat_id === request.seat_id);
  if (seat === undefined)
    return refuse("unknown_seat", `seat '${request.seat_id}' is not declared in the seat file`);
  if (!seat.policy.runtimes.includes(request.runtime))
    return refuse("runtime_not_allowed", `seat '${seat.seat_id}' does not allow runtime '${request.runtime}'`);
  if (!seat.policy.cwd_roots.some((root) => cwdWithin(request.cwd, root)))
    return refuse("cwd_not_allowed", `seat '${seat.seat_id}' does not allow working directory ${request.cwd}`);
  return Object.freeze({ ok: true, value: seat });
};
