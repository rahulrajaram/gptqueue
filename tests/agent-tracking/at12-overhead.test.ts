/**
 * AT12 overhead non-regression. Agent tracking adds no model-visible tool,
 * leaves a no-seat session's startup context byte-identical, and bounds what
 * a seat adds to startup context.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { decideHook, renderDecision } from "../../src/claude/hook.js";
import {
  admitSeatFile,
  MAX_CAPABILITIES,
  MAX_CAPABILITY_CHARS,
  MAX_DESCRIPTION_CHARS,
  MAX_DESCRIPTION_WORDS,
  MAX_NAME_CHARS,
  renderSeatCard,
  SEAT_CARD_MAX_CHARS,
  type SeatTenure,
} from "../../src/agent-tracking/index.js";
import { SHELL_TOOL_NAMES } from "../../src/registered-shell/tool-names.js";
import { holder, seat } from "./fixtures.js";
import golden from "./session-start-golden.json";

const ROOT = resolve(__dirname, "..", "..");
const tsFiles = (dir: string): string[] =>
  readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? tsFiles(path) : path.endsWith(".ts") ? [path] : [];
  });

/** Model-visible tool registration sites per file, as of f4a22d0 (before agent tracking). */
const TOOL_SITES_BASELINE: Record<string, number> = {
  "src/experimental-wrapper/bridge.ts": 4,
  "src/experimental-wrapper/pi-tools.ts": 2,
  "src/registered-shell/diagnostic-tools.ts": 5,
  "src/registered-shell/pi-extension.ts": 2,
  "src/registered-shell/runtime-tools.ts": 7,
  "src/transports/setup-tools.ts": 18,
};
// Since f4a22d0 the only additions are the claim-protocol tool
// acknowledge_messages (subset ack), one site each in runtime-tools.ts and
// setup-tools.ts, and the sidecar's caller-bound dlq_requeue (runtime-tools.ts)
// and dlq_status (diagnostic-tools.ts); agent tracking itself still adds none.
const TOOL_SITE = /server\.tool\(|registerTool\(|\.tool\("/g;

describe("AT12 overhead non-regression", () => {
  it("adds no model-visible tool registration anywhere in src", () => {
    const sites = Object.fromEntries(
      tsFiles(join(ROOT, "src"))
        .map((file) => [relative(ROOT, file), (readFileSync(file, "utf8").match(TOOL_SITE) ?? []).length] as const)
        .filter(([, count]) => count > 0)
    );
    expect(sites).toEqual(TOOL_SITES_BASELINE);
  });

  it("keeps the registered-shell tool catalog unchanged", () => {
    expect([...SHELL_TOOL_NAMES]).toEqual([
      "claim_tasks", "acknowledge_tasks", "renew_claim", "bind_runtime", "get_runtime_status",
      "acknowledge_messages", "dlq_requeue", "find_agents", "get_agent_details", "get_delivery_status", "set_agent_profile",
      "dlq_status",
    ]);
  });

  it("leaves SessionStart context byte-identical for sessions without a seat", () => {
    const probe = { ok: true as const, counts: { unclaimed: 2, claimed_by_session: 0, claimed_by_others: 0, expired_by_others: 0, latest_event_id: "1-0" } };
    const base = {
      event: "session-start" as const, probe, stopHookActive: false, prior: null, watcherRunning: false,
      watchPolicy: "require" as const, watchCommand: "'/bin/gptqueue-inbox-peek' --watch --session 'sid'",
    };
    const confirmed = { agent: "claude-x", gptqueue_session_id: "sid-sha256:0123456789ab", source: "session_record" as const, note: null, claude: { pid: 1, start: "1" } };
    const unconfirmed = { agent: "claude-y", gptqueue_session_id: null, source: "derived" as const, note: "no session record", claude: null };
    for (const seatText of [undefined, null]) {
      expect(renderDecision("session-start", decideHook({ ...base, identity: confirmed, seat: seatText }))).toBe(golden.confirmed);
      expect(renderDecision("session-start", decideHook({ ...base, identity: unconfirmed, seat: seatText }))).toBe(golden.unconfirmed);
    }
  });

  it("bounds the card of the largest admissible seat by SEAT_CARD_MAX_CHARS, without truncating it", () => {
    const wordChars = Math.floor((MAX_DESCRIPTION_CHARS + 1) / MAX_DESCRIPTION_WORDS) - 1;
    const maximal = seat({
      seat_id: "x".repeat(64),
      name: "N".repeat(MAX_NAME_CHARS),
      description: Array(MAX_DESCRIPTION_WORDS).fill("d".repeat(wordChars)).join(" "),
      capabilities: Array.from({ length: MAX_CAPABILITIES }, (_, i) => `${i}`.padEnd(MAX_CAPABILITY_CHARS, "c")),
    });
    expect(admitSeatFile({ schema_version: 1, seats: [maximal] }).ok).toBe(true);
    const tenure: SeatTenure = {
      seat_id: maximal.seat_id, state: "held", epoch: 999_999, holder: holder(),
      bound_at: "2030-01-01T00:00:00.000Z", lease_expires_at: "2030-01-01T06:00:00.000Z",
    };
    const card = renderSeatCard(maximal, tenure);
    expect(card.length).toBeLessThanOrEqual(SEAT_CARD_MAX_CHARS);
    expect(card).toContain(maximal.description);
    expect(card).toContain(maximal.capabilities.at(-1));
  });
});
