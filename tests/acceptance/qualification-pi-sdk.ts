import { access } from "node:fs/promises";
import { constants } from "node:fs";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
import type { Availability, Participant, RouteAdapter, RouteSpec } from "./qualification-types.js";
import { startSdkParticipant } from "./qualification-pi.js";
import { nodePrefixPath } from "./local-tools.js";

export const piSdkQualificationRoute = "pi-sdk" as const;
const repo = resolve(import.meta.dirname, "../..");
const installed = nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist");
type SdkOptions = Parameters<typeof startSdkParticipant>[0];
type LaunchInput = Parameters<RouteAdapter["launch"]>[0];

const spec: RouteSpec = Object.freeze({
  id: piSdkQualificationRoute,
  host: "pi",
  modelBacked: true,
  availability: { kind: "setup_gap" as const, detail: "Pi SDK route preflight not run" },
});

const preflight = (options: SdkOptions, signal: AbortSignal): Promise<Availability> => (async () => {
  if (signal.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("Pi SDK preflight aborted");
  const root = options.installedRoot ?? installed;
  const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
  try {
    await access(join(root, "index.js"), constants.R_OK);
    await access(join(repo, "src/registered-shell/pi-extension.ts"), constants.R_OK);
    await access(join(agentDir, "auth.json"), constants.R_OK);
  } catch {
    return { kind: "setup_gap", detail: "Installed Pi SDK, GPTQueue source extension, or existing Pi auth is unavailable" };
  }
  return { kind: "available" };
})();

/** Actual Pi SDK session route. The SDK participant owns one session and one cwd. */
export const createPiSdkAdapter = (options: SdkOptions = {}): RouteAdapter => Object.freeze({
  spec,
  preflight: signal => preflight(options, signal),
  launch: (input: LaunchInput, signal: AbortSignal): Promise<Participant> => startSdkParticipant(options, input, signal),
});
