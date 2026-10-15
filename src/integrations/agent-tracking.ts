/**
 * GPTQueue plugin seam for the agent-tracking library. This is the only file
 * that wires the library to GPTQueue: its Redis, its key namespace, and the
 * Claude SessionStart hook. With GPTQ_SEAT unset it does nothing.
 */
import {
  createNodeProcessProbe,
  createRedisSeatStore,
  createSeatService,
  renderSeatRefusal,
  SEAT_ENV,
  startSeat,
  type EvalClient,
} from "../agent-tracking/index.js";

/** GPTQueue's key namespace for seats. */
export const SEAT_KEY_PREFIX = "gptq:seat:";

export interface ClaudeSeatStartupInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly cwd: string;
  /** Claude's native session id from the hook payload. */
  readonly session_id: string;
  /** The Claude process the hook belongs to (inbox-probe Identity.claude). */
  readonly claude: Readonly<{ pid: number; start: string }> | null;
  readonly redis: EvalClient;
}

/** Startup context for a Claude session's seat, or null when no seat was requested. Never throws. */
export const claudeSeatStartupContext = async (input: ClaudeSeatStartupInput): Promise<string | null> => {
  const seatId = input.env[SEAT_ENV];
  if (!seatId) return null;
  try {
    return await bindClaudeSeat(input);
  } catch (error) {
    return renderSeatRefusal(seatId, { code: "store_unavailable", message: `seat binding failed: ${(error as Error).message}` });
  }
};

const bindClaudeSeat = async (input: ClaudeSeatStartupInput): Promise<string | null> => {
  const probe = createNodeProcessProbe();
  const service = createSeatService({
    store: createRedisSeatStore(input.redis, { prefix: SEAT_KEY_PREFIX }),
    clock: { now: () => new Date() },
    probe,
  });
  const started = await startSeat({
    env: input.env,
    runtime: "claude",
    session_id: input.session_id,
    cwd: input.cwd,
    process: input.claude,
    service,
    probe,
  });
  switch (started.kind) {
    case "none":
      return null;
    case "held":
    case "refused":
      return started.context;
    default: {
      const unreachable: never = started;
      throw new Error(`unhandled seat startup ${JSON.stringify(unreachable)}`);
    }
  }
};
