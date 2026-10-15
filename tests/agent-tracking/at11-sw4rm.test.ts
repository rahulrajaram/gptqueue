/**
 * AT11 sw4rm vocabulary conformance. The library's hand-written mapping is
 * checked against the published @sw4rm/js-sdk (devDependency only). The SDK's
 * ESM entry fails to load ("Dynamic require of fs"), so it is read via its
 * CommonJS entry.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";
import type { AgentDescriptor } from "@sw4rm/js-sdk";
import {
  MAX_DESCRIPTION_WORDS,
  SEAT_ERROR_TO_SW4RM,
  SW4RM_ERROR_CODE,
  SW4RM_SOURCE,
  toAgentDescriptor,
  type SeatErrorCode,
  type Sw4rmAgentDescriptorCore,
} from "../../src/agent-tracking/index.js";
import { seat } from "./fixtures.js";

const require = createRequire(import.meta.url);
const sdk = require("@sw4rm/js-sdk") as { ErrorCode: Record<string, string | number> };
// The package does not export ./package.json; its CJS entry is dist/cjs/index.cjs.
const sdkVersion = (JSON.parse(readFileSync(join(dirname(require.resolve("@sw4rm/js-sdk")), "..", "..", "package.json"), "utf8")) as { version: string }).version;

/** Numeric members of a TypeScript enum object (which also maps values back to names). */
const enumEntries = (e: Record<string, string | number>): Record<string, number> =>
  Object.fromEntries(Object.entries(e).filter((entry): entry is [string, number] => typeof entry[1] === "number"));

// Compile-time: the descriptor core is a structural subset of the SDK's AgentDescriptor.
type Core = Pick<AgentDescriptor, "agent_id" | "name" | "description"> & { capabilities: readonly string[] };
const _subset = (d: Sw4rmAgentDescriptorCore): Core => d;
void _subset;

describe("AT11 sw4rm vocabulary", () => {
  it("is pinned to the installed SDK version", () => {
    expect(SW4RM_SOURCE.sdk).toBe(`@sw4rm/js-sdk@${sdkVersion}`);
  });

  it("matches sw4rm ErrorCode exactly, name for name and value for value", () => {
    expect(SW4RM_ERROR_CODE).toEqual(enumEntries(sdk.ErrorCode));
  });

  it("maps every seat refusal code to a sw4rm ErrorCode", () => {
    const codes = Object.keys(SEAT_ERROR_TO_SW4RM) as SeatErrorCode[];
    expect(codes.length).toBeGreaterThan(0);
    for (const code of codes) expect(enumEntries(sdk.ErrorCode)).toHaveProperty(SEAT_ERROR_TO_SW4RM[code]);
    expect(SEAT_ERROR_TO_SW4RM.seat_held).toBe("ALREADY_IN_PROGRESS");
    expect(SEAT_ERROR_TO_SW4RM.stale_epoch).toBe("FORCED_PREEMPTION");
    expect(SEAT_ERROR_TO_SW4RM.unknown_seat).toBe("PERMISSION_DENIED");
  });

  it("renders a seat card as the sw4rm AgentDescriptor core fields", () => {
    const declared = seat();
    expect(toAgentDescriptor(declared)).toEqual({
      agent_id: declared.seat_id,
      name: declared.name,
      description: declared.description,
      capabilities: declared.capabilities,
    });
  });

  it("uses sw4rm's 200-word description limit", () => {
    expect(MAX_DESCRIPTION_WORDS).toBe(200);
  });
});
