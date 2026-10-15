/** Startup context text: the job card for a held seat, or a visible refusal. */
import type { SeatDeclaration, SeatError, SeatTenure } from "./types.js";

/** Upper bound on the card a seat adds to startup context (AT12). Admission limits keep real cards under it. */
export const SEAT_CARD_MAX_CHARS = 2600;
/** Caps on text echoed from the environment or from errors, which nobody vetted. */
const ECHO_ID_CHARS = 80;
const ECHO_MESSAGE_CHARS = 300;

const bounded = (text: string, max: number): string => (text.length <= max ? text : `${text.slice(0, max - 1)}…`);

/** Collapse all whitespace (including newlines) to single spaces, so text cannot imitate context structure. */
const oneLine = (text: string): string => text.replace(/\s+/g, " ").trim();

/** The job card. The launcher already bound the seat, so the card asks for nothing. */
export const renderSeatCard = (seat: SeatDeclaration, tenure: SeatTenure): string =>
  bounded(
    `gptqueue seat: this session holds seat ${JSON.stringify(seat.seat_id)} (${oneLine(seat.name)}), epoch ${tenure.epoch}` +
      `${tenure.bound_at ? `, since ${tenure.bound_at}` : ""}; the launcher bound it, nothing to do.\n` +
      `Job card: ${oneLine(seat.description)}\n` +
      `Capabilities: ${seat.capabilities.length > 0 ? seat.capabilities.map(oneLine).join(", ") : "none declared"}.`,
    SEAT_CARD_MAX_CHARS
  );

/** A refusal the model and the operator can both see; the session must not act as the seat. */
export const renderSeatRefusal = (seat_id: string, error: SeatError): string =>
  `gptqueue seat: this session does NOT hold seat ${JSON.stringify(bounded(oneLine(seat_id), ECHO_ID_CHARS))} ` +
  `(${error.code}: ${bounded(oneLine(error.message), ECHO_MESSAGE_CHARS)}). Do not act as that seat; tell the operator.`;
