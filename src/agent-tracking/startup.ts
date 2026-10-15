/**
 * Runtime-agnostic startup binding (AT1): a launcher names the seat in the
 * environment, and the runtime's startup hook calls startSeat. The model sees
 * only the resulting context text; it makes no tool call to bind.
 */
import { authorizeBind } from "./authorize.js";
import { renderSeatCard, renderSeatRefusal } from "./card.js";
import type { ProcessProbe } from "./ports.js";
import { defaultSeatFilePath, loadSeatFile, type SeatFile } from "./seat-file.js";
import type { SeatService } from "./service.js";
import type { Result, SeatDeclaration, SeatError, SeatTenure } from "./types.js";

/** Launcher env: the seat this session should hold. Unset means no seat, and nothing changes. */
export const SEAT_ENV = "GPTQ_SEAT";
/** Launcher env: the seat file path; defaults to $XDG_CONFIG_HOME/gptqueue/seats.json. */
export const SEAT_FILE_ENV = "GPTQ_SEAT_FILE";

export type SeatStartup =
  | Readonly<{ kind: "none" }>
  | Readonly<{ kind: "held"; seat: SeatDeclaration; tenure: SeatTenure; context: string }>
  | Readonly<{ kind: "refused"; seat_id: string; error: SeatError; context: string }>;

export interface SeatStartupInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly runtime: string;
  readonly session_id: string;
  readonly cwd: string;
  /** The runtime process that will hold the seat; null when the host could not identify it. */
  readonly process: Readonly<{ pid: number; start: string }> | null;
  readonly service: SeatService;
  readonly probe: ProcessProbe;
  /** Seat file loader; defaults to reading the path from SEAT_FILE_ENV. */
  readonly loadFile?: (path: string) => Result<SeatFile>;
}

const NONE: SeatStartup = Object.freeze({ kind: "none" });

export const startSeat = async (input: SeatStartupInput): Promise<SeatStartup> => {
  const seat_id = input.env[SEAT_ENV];
  if (!seat_id) return NONE;
  const refused = (error: SeatError): SeatStartup =>
    Object.freeze({ kind: "refused", seat_id, error, context: renderSeatRefusal(seat_id, error) });

  const file = (input.loadFile ?? loadSeatFile)(input.env[SEAT_FILE_ENV] || defaultSeatFilePath(input.env));
  if (!file.ok) return refused(file.error);
  const authorized = authorizeBind(file.value, { seat_id, runtime: input.runtime, cwd: input.cwd });
  if (!authorized.ok) return refused(authorized.error);
  if (input.process === null) {
    return refused({ code: "holder_unidentified", message: `the ${input.runtime} process for this session could not be identified` });
  }
  const local = input.probe.local();
  const bound = await input.service.bind(seat_id, {
    runtime: input.runtime,
    session_id: input.session_id,
    host: local.host,
    machine_id: local.machine_id,
    boot_id: local.boot_id,
    pidns: local.pidns,
    pid: input.process.pid,
    pid_start: input.process.start,
  });
  return bound.ok
    ? Object.freeze({ kind: "held", seat: authorized.value, tenure: bound.value.tenure, context: renderSeatCard(authorized.value, bound.value.tenure) })
    : refused(bound.error);
};
