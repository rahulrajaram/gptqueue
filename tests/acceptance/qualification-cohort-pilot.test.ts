import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters } from "./qualification-codex.js";
import { createPiAdapters } from "./qualification-pi.js";
import { createPiSdkAdapter } from "./qualification-pi-sdk.js";
import { createPiNativeChildAdapter, enterPiNativeChildProfileScope } from "./qualification-pi-child.js";
import { createPiInteractiveAdapter } from "./qualification-pi-interactive.js";
import { createGenericAdapters, type GenericTransportEvidence } from "./qualification-generic.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { CohortLeasePool, type CohortMember } from "./qualification-scheduler.js";
import { runMixedPair, runModelPair, type TypedEvidenceWriter } from "./qualification-driver.js";
import { extractGenericTraces, extractNativeTraces, type NativeConsumption, type NativeTrace } from "./qualification-evidence.js";
import { checkExchangeEvidence } from "./oracle.js";
import { sanitizeEvidence } from "./public-evidence.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import type { Availability, ModelParticipant, Participant, PairSpec, RawEvidenceRef, RouteAdapter, RuntimeStatus } from "./qualification-types.js";
import { CODEX_BIN, nodePrefixPath } from "./local-tools.js";

const stdioEnabled = process.env.GPTQUEUE_QUALIFICATION_COHORT_PILOT === "1";
const transportEnabled = process.env.GPTQUEUE_QUALIFICATION_TRANSPORT_PILOT === "1";
const crossEnabled = process.env.GPTQUEUE_QUALIFICATION_CROSS_PILOT === "1";
const routeVariant = process.env.GPTQUEUE_QUALIFICATION_ROUTE_VARIANT;
const enabled = stdioEnabled || transportEnabled || crossEnabled;
const pilotConsumption: NativeConsumption = "legacy_receive";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-cohort-pilot");
const sourceFiles = [
  "tests/acceptance/codex-appserver-history.ts", "tests/acceptance/codex-support.ts", "tests/acceptance/owned-redis.ts",
  "tests/acceptance/qualification-discovery.ts", "tests/acceptance/qualification-cohort-pilot.test.ts", "tests/acceptance/qualification-driver.ts", "tests/acceptance/qualification-evidence.ts", "tests/acceptance/qualification-types.ts", "tests/acceptance/qualification-scheduler.ts", "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-pi.ts", "tests/acceptance/qualification-pi-sdk.ts", "tests/acceptance/qualification-generic.ts", "tests/acceptance/oracle.ts", "src/experimental-wrapper/bridge.ts", "src/registered-shell/runtime.ts", "src/registered-shell/codex-socket.ts", "src/registered-shell/codex-history.ts", "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js", "dist/mcp-server/index.js", CODEX_BIN, nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist/index.js"), nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist/modes/rpc/rpc-client.js"),
] as const;
type Json = Record<string, unknown>;
type CleanupResult = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;
type PilotRow = Readonly<{ id: string; kind: "model" | "mixed"; sender: string; receiver: string; left: number; right: number }>;
type PilotVariant = "default" | "headless-sdk" | "pi-child" | "pi-child-codex" | "pi-child-sdk" | "pi-child-extended" | "codex-extended" | "pi-interactive" | "extended-mix" | "extended-generics";
type PilotPlan = Readonly<{ id: "stdio" | "transport" | "cross"; rows: readonly PilotRow[]; genericRoutes: readonly ("generic-stdio" | "generic-http" | "generic-stateless")[]; sourceFiles: readonly string[]; transportEvidence: boolean; codexRoute: "codex-appserver" | "codex-headless"; piRoute: "pi-rpc-cli" | "pi-sdk" | "pi-native-child"; variant: PilotVariant; modelRoutes: readonly string[]; actorCount: number }>;
type ParticipantMap = Readonly<Record<string, Participant>>;
type RowAccounting = Readonly<{ rows: readonly Json[]; unfinishedRows: readonly Json[] }>;
export const selectPilotRows = (rows: readonly PilotRow[], selector: string | undefined): readonly PilotRow[] => {
  if (selector === undefined) return rows;
  const requested = selector.split(",").map((value) => value.trim());
  if (requested.some((value) => value.length === 0)) throw new Error("qualification row selector contains an empty row id");
  if (new Set(requested).size !== requested.length) throw new Error("qualification row selector contains duplicate row ids");
  const known = new Set(rows.map((row) => row.id));
  const unknown = requested.find((id) => !known.has(id));
  if (unknown) throw new Error(`qualification row selector contains unknown row id: ${unknown}`);
  const selected = new Set(requested);
  return rows.filter((row) => selected.has(row.id));
};
const crossAliasRoutes = { "codex-1": "codex-appserver", "codex-2": "codex-headless", "pi-1": "pi-rpc-cli", "pi-2": "pi-sdk" } as const;
const specializePlanRows = (plan: PilotPlan, rows: readonly PilotRow[]): PilotPlan => {
  if (plan.id !== "cross") return { ...plan, rows };
  const aliases = new Set(rows.flatMap((row) => [row.sender, row.receiver]));
  const modelRoutes = (Object.keys(crossAliasRoutes) as (keyof typeof crossAliasRoutes)[]).filter((alias) => aliases.has(alias)).map((alias) => crossAliasRoutes[alias]);
  return { ...plan, rows, modelRoutes, actorCount: aliases.size };
};

const pilotRows: readonly PilotRow[] = [
  { id: "same-codex", kind: "model", sender: "codex-1", receiver: "codex-2", left: 17, right: 25 },
  { id: "same-pi", kind: "model", sender: "pi-1", receiver: "pi-2", left: 19, right: 23 },
  { id: "cross-codex-to-pi", kind: "model", sender: "codex-1", receiver: "pi-1", left: 31, right: 11 },
  { id: "cross-pi-to-codex", kind: "model", sender: "pi-2", receiver: "codex-2", left: 27, right: 14 },
  { id: "mixed-generic-to-codex", kind: "mixed", sender: "generic-1", receiver: "codex-1", left: 8, right: 34 },
  { id: "mixed-codex-to-generic", kind: "mixed", sender: "codex-2", receiver: "generic-1", left: 42, right: 7 },
  { id: "mixed-generic-to-pi", kind: "mixed", sender: "generic-2", receiver: "pi-1", left: 16, right: 29 },
  { id: "mixed-pi-to-generic", kind: "mixed", sender: "pi-2", receiver: "generic-2", left: 38, right: 6 },
];
const transportPilotRows: readonly PilotRow[] = [
  { id: "codex-to-http", kind: "mixed", sender: "codex-1", receiver: "http-1", left: 13, right: 29 },
  { id: "http-to-codex", kind: "mixed", sender: "http-2", receiver: "codex-2", left: 37, right: 5 },
  { id: "pi-to-http", kind: "mixed", sender: "pi-1", receiver: "http-2", left: 22, right: 18 },
  { id: "http-to-pi", kind: "mixed", sender: "http-1", receiver: "pi-2", left: 41, right: 9 },
  { id: "codex-to-stateless", kind: "mixed", sender: "codex-2", receiver: "stateless-1", left: 7, right: 35 },
  { id: "stateless-to-codex", kind: "mixed", sender: "stateless-2", receiver: "codex-1", left: 26, right: 16 },
  { id: "pi-to-stateless", kind: "mixed", sender: "pi-2", receiver: "stateless-2", left: 18, right: 24 },
  { id: "stateless-to-pi", kind: "mixed", sender: "stateless-1", receiver: "pi-1", left: 32, right: 12 },
];
const crossPilotRows: readonly PilotRow[] = [
  { id: "cross-appserver-to-headless", kind: "model", sender: "codex-1", receiver: "codex-2", left: 17, right: 25 },
  { id: "cross-headless-to-appserver", kind: "model", sender: "codex-2", receiver: "codex-1", left: 19, right: 23 },
  { id: "cross-rpc-to-sdk", kind: "model", sender: "pi-1", receiver: "pi-2", left: 31, right: 11 },
  { id: "cross-sdk-to-rpc", kind: "model", sender: "pi-2", receiver: "pi-1", left: 27, right: 14 },
  { id: "cross-appserver-to-sdk", kind: "model", sender: "codex-1", receiver: "pi-2", left: 8, right: 34 },
  { id: "cross-sdk-to-appserver", kind: "model", sender: "pi-2", receiver: "codex-1", left: 42, right: 7 },
  { id: "cross-rpc-to-headless", kind: "model", sender: "pi-1", receiver: "codex-2", left: 16, right: 29 },
  { id: "cross-headless-to-rpc", kind: "model", sender: "codex-2", receiver: "pi-1", left: 38, right: 6 },
];
const transportSourceFiles = [...sourceFiles, "dist/transports/http.js"] as const;
const childRows: readonly PilotRow[] = [
  { id: "child-to-child", kind: "model", sender: "child-1", receiver: "child-2", left: 17, right: 25 },
  { id: "child-to-generic-stdio", kind: "mixed", sender: "child-1", receiver: "generic-1", left: 19, right: 23 },
  { id: "child-to-generic-http", kind: "mixed", sender: "child-1", receiver: "http-1", left: 31, right: 11 },
  { id: "child-to-generic-stateless", kind: "mixed", sender: "child-1", receiver: "stateless-1", left: 27, right: 14 },
  { id: "generic-stdio-to-child", kind: "mixed", sender: "generic-2", receiver: "child-2", left: 8, right: 34 },
  { id: "generic-http-to-child", kind: "mixed", sender: "http-2", receiver: "child-2", left: 42, right: 7 },
  { id: "generic-stateless-to-child", kind: "mixed", sender: "stateless-2", receiver: "child-2", left: 16, right: 29 },
];
const childCodexRows: readonly PilotRow[] = [
  { id: "child-to-appserver", kind: "model", sender: "child-1", receiver: "codex-1", left: 17, right: 25 },
  { id: "appserver-to-child", kind: "model", sender: "codex-1", receiver: "child-1", left: 19, right: 23 },
  { id: "child-to-headless", kind: "model", sender: "child-1", receiver: "codex-2", left: 31, right: 11 },
  { id: "headless-to-child", kind: "model", sender: "codex-2", receiver: "child-1", left: 27, right: 14 },
];
const childSdkRows: readonly PilotRow[] = [
  { id: "child-to-rpc", kind: "model", sender: "child-1", receiver: "pi-1", left: 17, right: 25 },
  { id: "rpc-to-child", kind: "model", sender: "pi-1", receiver: "child-1", left: 19, right: 23 },
  { id: "child-to-sdk", kind: "model", sender: "child-1", receiver: "pi-2", left: 31, right: 11 },
  { id: "sdk-to-child", kind: "model", sender: "pi-2", receiver: "child-1", left: 27, right: 14 },
];
const childExtendedRows: readonly PilotRow[] = [
  { id: "child-to-interactive", kind: "model", sender: "child-1", receiver: "interactive-1", left: 17, right: 25 },
  { id: "interactive-to-child", kind: "model", sender: "interactive-1", receiver: "child-1", left: 19, right: 23 },
  { id: "child-to-fork", kind: "model", sender: "child-1", receiver: "fork-1", left: 31, right: 11 },
  { id: "fork-to-child", kind: "model", sender: "fork-1", receiver: "child-1", left: 27, right: 14 },
  { id: "child-to-piint", kind: "model", sender: "child-1", receiver: "piint-1", left: 16, right: 29 },
  { id: "piint-to-child", kind: "model", sender: "piint-1", receiver: "child-1", left: 38, right: 6 },
];
const childBridgeVariant = (variant: PilotVariant): boolean => variant === "pi-child-codex" || variant === "pi-child-sdk" || variant === "pi-child-extended";
const extendedPlanVariant = (variant: PilotVariant): boolean => variant === "codex-extended";
const codexExtendedRows: readonly PilotRow[] = [
  { id: "interactive-to-appserver", kind: "model", sender: "interactive-1", receiver: "codex-1", left: 17, right: 25 },
  { id: "appserver-to-interactive", kind: "model", sender: "codex-1", receiver: "interactive-1", left: 19, right: 23 },
  { id: "interactive-to-headless", kind: "model", sender: "interactive-1", receiver: "codex-2", left: 31, right: 11 },
  { id: "headless-to-interactive", kind: "model", sender: "codex-2", receiver: "interactive-1", left: 27, right: 14 },
  { id: "fork-to-appserver", kind: "model", sender: "fork-1", receiver: "codex-1", left: 16, right: 29 },
  { id: "appserver-to-fork", kind: "model", sender: "codex-1", receiver: "fork-1", left: 38, right: 6 },
  { id: "fork-to-headless", kind: "model", sender: "fork-1", receiver: "codex-2", left: 12, right: 30 },
  { id: "headless-to-fork", kind: "model", sender: "codex-2", receiver: "fork-1", left: 21, right: 20 },
];
const codexExtendedAliasRoutes = { "interactive-1": "codex-interactive", "fork-1": "codex-fork", "codex-1": "codex-appserver", "codex-2": "codex-headless" } as const;
const piInteractiveRows: readonly PilotRow[] = [
  { id: "piint-to-appserver", kind: "model", sender: "piint-1", receiver: "codex-1", left: 17, right: 25 },
  { id: "appserver-to-piint", kind: "model", sender: "codex-1", receiver: "piint-1", left: 19, right: 23 },
  { id: "piint-to-headless", kind: "model", sender: "piint-1", receiver: "codex-2", left: 31, right: 11 },
  { id: "headless-to-piint", kind: "model", sender: "codex-2", receiver: "piint-1", left: 27, right: 14 },
  { id: "piint-to-rpc", kind: "model", sender: "piint-1", receiver: "pi-1", left: 16, right: 29 },
  { id: "rpc-to-piint", kind: "model", sender: "pi-1", receiver: "piint-1", left: 38, right: 6 },
  { id: "piint-to-sdk", kind: "model", sender: "piint-1", receiver: "pi-2", left: 12, right: 30 },
  { id: "sdk-to-piint", kind: "model", sender: "pi-2", receiver: "piint-1", left: 21, right: 20 },
];
const piInteractiveAliasRoutes = { "piint-1": "pi-interactive", "codex-1": "codex-appserver", "codex-2": "codex-headless", "pi-1": "pi-rpc-cli", "pi-2": "pi-sdk" } as const;
const extendedMixRows: readonly PilotRow[] = [
  { id: "interactive-to-fork", kind: "model", sender: "interactive-1", receiver: "fork-1", left: 17, right: 25 },
  { id: "fork-to-interactive", kind: "model", sender: "fork-1", receiver: "interactive-1", left: 19, right: 23 },
  { id: "interactive-to-piint", kind: "model", sender: "interactive-1", receiver: "piint-1", left: 31, right: 11 },
  { id: "piint-to-interactive", kind: "model", sender: "piint-1", receiver: "interactive-1", left: 27, right: 14 },
  { id: "fork-to-piint", kind: "model", sender: "fork-1", receiver: "piint-1", left: 16, right: 29 },
  { id: "piint-to-fork", kind: "model", sender: "piint-1", receiver: "fork-1", left: 38, right: 6 },
  { id: "interactive-to-rpc", kind: "model", sender: "interactive-1", receiver: "pi-1", left: 12, right: 30 },
  { id: "rpc-to-interactive", kind: "model", sender: "pi-1", receiver: "interactive-1", left: 21, right: 20 },
  { id: "interactive-to-sdk", kind: "model", sender: "interactive-1", receiver: "pi-2", left: 15, right: 28 },
  { id: "sdk-to-interactive", kind: "model", sender: "pi-2", receiver: "interactive-1", left: 26, right: 18 },
  { id: "fork-to-rpc", kind: "model", sender: "fork-1", receiver: "pi-1", left: 33, right: 10 },
  { id: "rpc-to-fork", kind: "model", sender: "pi-1", receiver: "fork-1", left: 22, right: 21 },
  { id: "fork-to-sdk", kind: "model", sender: "fork-1", receiver: "pi-2", left: 9, right: 36 },
  { id: "sdk-to-fork", kind: "model", sender: "pi-2", receiver: "fork-1", left: 40, right: 5 },
];
const extendedMixAliasRoutes = { "interactive-1": "codex-interactive", "fork-1": "codex-fork", "piint-1": "pi-interactive", "pi-1": "pi-rpc-cli", "pi-2": "pi-sdk" } as const;
const extendedGenericRows: readonly PilotRow[] = [
  { id: "interactive-to-stdio", kind: "mixed", sender: "interactive-1", receiver: "generic-1", left: 17, right: 25 },
  { id: "stdio-to-interactive", kind: "mixed", sender: "generic-2", receiver: "interactive-1", left: 19, right: 23 },
  { id: "interactive-to-http", kind: "mixed", sender: "interactive-1", receiver: "http-1", left: 31, right: 11 },
  { id: "http-to-interactive", kind: "mixed", sender: "http-2", receiver: "interactive-1", left: 27, right: 14 },
  { id: "interactive-to-stateless", kind: "mixed", sender: "interactive-1", receiver: "stateless-1", left: 16, right: 29 },
  { id: "stateless-to-interactive", kind: "mixed", sender: "stateless-2", receiver: "interactive-1", left: 38, right: 6 },
  { id: "fork-to-stdio", kind: "mixed", sender: "fork-1", receiver: "generic-1", left: 12, right: 30 },
  { id: "stdio-to-fork", kind: "mixed", sender: "generic-2", receiver: "fork-1", left: 21, right: 20 },
  { id: "fork-to-http", kind: "mixed", sender: "fork-1", receiver: "http-1", left: 15, right: 28 },
  { id: "http-to-fork", kind: "mixed", sender: "http-2", receiver: "fork-1", left: 26, right: 18 },
  { id: "fork-to-stateless", kind: "mixed", sender: "fork-1", receiver: "stateless-1", left: 33, right: 10 },
  { id: "stateless-to-fork", kind: "mixed", sender: "stateless-2", receiver: "fork-1", left: 22, right: 21 },
  { id: "piint-to-stdio", kind: "mixed", sender: "piint-1", receiver: "generic-1", left: 9, right: 36 },
  { id: "stdio-to-piint", kind: "mixed", sender: "generic-2", receiver: "piint-1", left: 40, right: 5 },
  { id: "piint-to-http", kind: "mixed", sender: "piint-1", receiver: "http-1", left: 13, right: 32 },
  { id: "http-to-piint", kind: "mixed", sender: "http-2", receiver: "piint-1", left: 24, right: 19 },
  { id: "piint-to-stateless", kind: "mixed", sender: "piint-1", receiver: "stateless-1", left: 28, right: 17 },
  { id: "stateless-to-piint", kind: "mixed", sender: "stateless-2", receiver: "piint-1", left: 35, right: 8 },
];
const extendedGenericAliasRoutes = { "interactive-1": "codex-interactive", "fork-1": "codex-fork", "piint-1": "pi-interactive" } as const;
export const selectPilotVariant = (kind: "stdio" | "transport", requested: string | undefined): PilotVariant => {
  if (requested === undefined) return "default";
  if (requested !== "headless-sdk" && requested !== "pi-child" && requested !== "pi-child-codex" && requested !== "pi-child-sdk" && requested !== "pi-child-extended" && requested !== "codex-extended" && requested !== "pi-interactive" && requested !== "extended-mix" && requested !== "extended-generics") throw new Error(`unknown qualification route variant: ${requested}`);
  if (kind !== "transport") throw new Error(`${requested} route variant requires the transport gate`);
  return requested;
};
export const selectPilotMode = (stdio: boolean, transport: boolean, cross: boolean, child = false): "stdio" | "transport" | "cross" => {
  if (Number(stdio) + Number(transport) + Number(cross) + Number(child) !== 1) throw new Error("set exactly one qualification pilot gate");
  return cross ? "cross" : transport || child ? "transport" : "stdio";
};
const pilotPlan = (kind: "stdio" | "transport" | "cross", variant: PilotVariant = "default"): PilotPlan => {
  if (kind === "cross" && variant !== "default") throw new Error("route variant is incompatible with cross pilot");
  if (kind === "cross") return { id: "cross", rows: crossPilotRows, genericRoutes: [], sourceFiles, transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["codex-appserver", "codex-headless", "pi-rpc-cli", "pi-sdk"], actorCount: 4 };
  if ((variant === "headless-sdk" || variant === "pi-child") && kind !== "transport") throw new Error(`${variant} route variant requires the transport gate`);
  if (variant === "pi-child") return { id: "transport", rows: childRows, genericRoutes: ["generic-stdio", "generic-http", "generic-stateless"], sourceFiles: [...transportSourceFiles, "tests/acceptance/qualification-pi-child.ts"], transportEvidence: true, codexRoute: "codex-appserver", piRoute: "pi-native-child", variant, modelRoutes: ["pi-native-child", "pi-native-child"], actorCount: 4 };
  if (variant === "pi-child-codex" || variant === "pi-child-sdk") return { id: "transport", rows: variant === "pi-child-codex" ? childCodexRows : childSdkRows, genericRoutes: [], sourceFiles: [...sourceFiles, "tests/acceptance/qualification-pi-child.ts"], transportEvidence: false, codexRoute: variant === "pi-child-codex" ? "codex-appserver" : "codex-headless", piRoute: "pi-native-child", variant, modelRoutes: variant === "pi-child-codex" ? ["pi-native-child", "codex-appserver", "codex-headless"] : ["pi-native-child", "pi-rpc-cli", "pi-sdk"], actorCount: 3 };
  if (variant === "pi-child-extended") return { id: "transport", rows: childExtendedRows, genericRoutes: [], sourceFiles: [...sourceFiles, "tests/acceptance/qualification-pi-child.ts", "tests/acceptance/qualification-pi-interactive.ts"], transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-native-child", variant, modelRoutes: ["pi-native-child", "codex-interactive", "codex-fork", "pi-interactive"], actorCount: 4 };
  if (extendedPlanVariant(variant)) return { id: "transport", rows: codexExtendedRows, genericRoutes: [], sourceFiles, transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["codex-interactive", "codex-fork", "codex-appserver", "codex-headless"], actorCount: 4 };
  if (variant === "pi-interactive") return { id: "transport", rows: piInteractiveRows, genericRoutes: [], sourceFiles: [...sourceFiles, "tests/acceptance/qualification-pi-interactive.ts"], transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["pi-interactive", "codex-appserver", "codex-headless", "pi-rpc-cli", "pi-sdk"], actorCount: 5 };
  if (variant === "extended-mix") return { id: "transport", rows: extendedMixRows, genericRoutes: [], sourceFiles: [...sourceFiles, "tests/acceptance/qualification-pi-interactive.ts"], transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["codex-interactive", "codex-fork", "pi-interactive", "pi-rpc-cli", "pi-sdk"], actorCount: 5 };
  if (variant === "extended-generics") return { id: "transport", rows: extendedGenericRows, genericRoutes: ["generic-stdio", "generic-http", "generic-stateless"], sourceFiles: [...transportSourceFiles, "tests/acceptance/qualification-pi-interactive.ts"], transportEvidence: true, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["codex-interactive", "codex-fork", "pi-interactive"], actorCount: 9 };
  const headless = variant === "headless-sdk";
  return kind === "transport"
    ? { id: "transport", rows: [...pilotRows, ...transportPilotRows], genericRoutes: ["generic-stdio", "generic-http", "generic-stateless"], sourceFiles: transportSourceFiles, transportEvidence: true, codexRoute: headless ? "codex-headless" : "codex-appserver", piRoute: headless ? "pi-sdk" : "pi-rpc-cli", variant, modelRoutes: [headless ? "codex-headless" : "codex-appserver", headless ? "codex-headless" : "codex-appserver", headless ? "pi-sdk" : "pi-rpc-cli", headless ? "pi-sdk" : "pi-rpc-cli"], actorCount: 10 }
    : { id: "stdio", rows: pilotRows, genericRoutes: ["generic-stdio"], sourceFiles, transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant, modelRoutes: ["codex-appserver", "codex-appserver", "pi-rpc-cli", "pi-rpc-cli"], actorCount: 6 };
};
export const selectPilotKind = (stdio: boolean, transport: boolean): "stdio" | "transport" => {
  if (stdio === transport) throw new Error("set exactly one qualification pilot gate");
  return transport ? "transport" : "stdio";
};
export const resolvePilotAliases = (row: PilotRow, aliases: Readonly<Record<string, string>>, knownParticipantIds?: ReadonlySet<string>): readonly [string, string] => {
  const sender = aliases[row.sender], receiver = aliases[row.receiver];
  if (!sender || !receiver) throw new Error(`row ${row.id} references an unknown participant alias`);
  if (knownParticipantIds && (!knownParticipantIds.has(sender) || !knownParticipantIds.has(receiver))) throw new Error(`row ${row.id} references a stale participant alias`);
  if (sender === receiver) throw new Error(`row ${row.id} resolves sender and receiver to one participant`);
  return [sender, receiver];
};

export const materializeUnrunRows = (declaredRows: readonly PilotRow[], rows: readonly Json[], unfinishedRows: readonly Json[], reason: string): RowAccounting => {
  const accounted = new Set([...rows, ...unfinishedRows].flatMap((value) => typeof value.id === "string" ? [value.id] : []));
  const additions = declaredRows.filter((row) => !accounted.has(row.id)).map((row): Json => ({
    id: row.id, kind: row.kind, sender: row.sender, receiver: row.receiver, expected_decimal: row.left + row.right,
    status: "unfinished", execution: { status: "not_run" }, reason,
  }));
  return { rows, unfinishedRows: [...unfinishedRows, ...additions] };
};

export const sanitizeFailureEvidence = (error: unknown): unknown => {
  if (!error || typeof error !== "object" || Array.isArray(error)) return undefined;
  const evidence = (error as Json).evidence;
  return evidence === undefined ? undefined : sanitizeEvidence(evidence, { parseEmbeddedJson: true, redactSessionObjectIds: true });
};

vi.setConfig({ testTimeout: 1_800_000, hookTimeout: 60_000 });

const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const sourceHashes = async (files: readonly string[] = sourceFiles): Promise<Readonly<Record<string, string>>> => Object.fromEntries(await Promise.all(files.map(async (file) => [file, await digest(resolve(repo, file))] as const)));
const safeName = (value: string): string => createHash("sha256").update(value).digest("hex").slice(0, 16);
const identity = (participant: Participant): Readonly<Record<string, unknown>> => ({ participant_id: participant.identity.participantId, route: participant.identity.route, host_runtime_id: participant.identity.hostRuntimeId, agent: participant.identity.agent, cwd_hash: participant.identity.cwdHash, profile_hash: participant.identity.profileHash, epoch_hash: participant.identity.epochHash, provenance: "provenance" in participant ? participant.provenance : undefined });
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("qualification operation aborted");
const delay = (milliseconds: number, signal: AbortSignal): Promise<void> => new Promise((resolveDelay, reject) => {
  if (signal.aborted) { reject(abortError(signal)); return; }
  const timer = setTimeout(resolveDelay, milliseconds);
  const abort = () => { clearTimeout(timer); reject(abortError(signal)); };
  signal.addEventListener("abort", abort, { once: true });
  setTimeout(() => signal.removeEventListener("abort", abort), milliseconds + 1);
});
const boundedSignal = (parent: AbortSignal, milliseconds: number): Readonly<{ signal: AbortSignal; cancel: () => void }> => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`cohort readiness exceeded ${milliseconds}ms`)), milliseconds);
  const abort = () => controller.abort(parent.reason ?? new Error("cohort operation aborted"));
  if (parent.aborted) abort(); else parent.addEventListener("abort", abort, { once: true });
  return { signal: controller.signal, cancel: () => { clearTimeout(timer); parent.removeEventListener("abort", abort); } };
};
const model = (participant: Participant): ModelParticipant => { if (participant.kind !== "model") throw new Error(`expected model participant, got ${participant.kind}`); return participant; };
const asStatus = (value: RuntimeStatus): Json => ({ kind: value.kind, runtime_id: "runtimeId" in value ? value.runtimeId : undefined, detail: "detail" in value ? value.detail : undefined });
type GenericTransportEvidenceFactory = Pick<ReturnType<typeof createGenericAdapters>, "transportEvidence">;
const requireTransportEvidence = (factory: GenericTransportEvidenceFactory, phase: "before" | "after"): GenericTransportEvidence => {
  const evidence = factory.transportEvidence();
  if (evidence === null) throw new Error(`transport ${phase} evidence provider returned no owned HTTP snapshot`);
  if (evidence.transport !== "http" || !Number.isSafeInteger(evidence.child_pid) || evidence.child_pid <= 0 || !Number.isSafeInteger(evidence.port) || evidence.port < 1 || evidence.port > 65_535 || evidence.node_path.length === 0 || evidence.script_path.length === 0 || !/^[a-f0-9]{64}$/iu.test(evidence.node_executable_sha256) || !/^[a-f0-9]{64}$/iu.test(evidence.script_sha256) || evidence.startup_ready !== true) throw new Error(`transport ${phase} evidence has invalid owned HTTP identity/readiness`);
  if (phase === "before" && evidence.terminated !== false) throw new Error("transport before evidence must prove the HTTP server is running");
  if (phase === "after" && (evidence.terminated !== true || (evidence.exit_code === null && evidence.signal_code === null))) throw new Error("transport after evidence does not prove owned HTTP termination");
  return evidence;
};
const sameTransportIdentity = (before: GenericTransportEvidence, after: GenericTransportEvidence): boolean => before.child_pid === after.child_pid && before.port === after.port && before.node_executable_sha256 === after.node_executable_sha256 && before.script_sha256 === after.script_sha256;

describe("cohort pilot planning", () => {
  it("keeps eight ordered rows distinct and rejects stale alias collisions", () => {
    expect(new Set(pilotRows.map((row) => `${row.sender}->${row.receiver}`)).size).toBe(8);
    const aliases = { "codex-1": "codex-participant-1", "codex-2": "codex-participant-2", "pi-1": "pi-participant-1", "pi-2": "pi-participant-2", "generic-1": "generic-participant-1", "generic-2": "generic-participant-2" };
    expect(resolvePilotAliases(pilotRows[0]!, aliases)).toEqual(["codex-participant-1", "codex-participant-2"]);
    expect(() => resolvePilotAliases(pilotRows[0]!, { ...aliases, "codex-2": "codex-participant-1" })).toThrow(/one participant/);
    const known = new Set(Object.values(aliases));
    expect(() => resolvePilotAliases(pilotRows[0]!, { ...aliases, "codex-2": "stale-participant" }, known)).toThrow(/stale participant/);
  });

  it("declares the transport plan as sixteen distinct model/generic route rows", () => {
    const plan = pilotPlan("transport");
    expect(plan.rows).toHaveLength(16);
    expect(new Set(plan.rows.map((row) => `${row.sender}->${row.receiver}`)).size).toBe(16);
    expect(plan.genericRoutes).toEqual(["generic-stdio", "generic-http", "generic-stateless"]);
    expect(plan.sourceFiles).toContain("dist/transports/http.js");
    expect(plan.rows.slice(pilotRows.length).every((row) => row.kind === "mixed")).toBe(true);
  });

  it("rejects simultaneous or absent pilot gates", () => {
    expect(() => selectPilotKind(false, false)).toThrow(/exactly one/);
    expect(() => selectPilotKind(true, true)).toThrow(/exactly one/);
    expect(selectPilotKind(true, false)).toBe("stdio");
    expect(selectPilotKind(false, true)).toBe("transport");
  });

  it("selects declared row subsets in plan order and rejects malformed selectors", () => {
    expect(selectPilotRows(pilotRows, undefined)).toEqual(pilotRows);
    expect(selectPilotRows(pilotRows, "mixed-pi-to-generic,cross-pi-to-codex").map((row) => row.id)).toEqual(["cross-pi-to-codex", "mixed-pi-to-generic"]);
    expect(() => selectPilotRows(pilotRows, "")).toThrow(/empty/);
    expect(() => selectPilotRows(pilotRows, "same-codex,missing-row")).toThrow(/unknown/);
    expect(() => selectPilotRows(pilotRows, "same-codex,same-codex")).toThrow(/duplicate/);
  });

  it("narrows cross actor slots and routes to selected rows while preserving defaults", () => {
    const full = pilotPlan("cross");
    expect(specializePlanRows(full, full.rows)).toMatchObject({ actorCount: 4, modelRoutes: full.modelRoutes });
    const selected = selectPilotRows(full.rows, "cross-appserver-to-headless,cross-headless-to-appserver");
    expect(specializePlanRows(full, selected)).toMatchObject({ actorCount: 2, modelRoutes: ["codex-appserver", "codex-headless"], rows: selected });
  });

  it("selects the explicit headless-sdk route pair only behind transport", () => {
    expect(selectPilotVariant("transport", "headless-sdk")).toBe("headless-sdk");
    expect(pilotPlan("transport", "headless-sdk")).toMatchObject({ codexRoute: "codex-headless", piRoute: "pi-sdk", variant: "headless-sdk" });
    expect(() => selectPilotVariant("stdio", "headless-sdk")).toThrow(/transport gate/);
    expect(() => selectPilotVariant("transport", "unknown")).toThrow(/unknown/);
  });
  it("plans the two four-row child bridge batches with a four-slot actor budget", () => {
    const codex = pilotPlan("transport", "pi-child-codex");
    const sdk = pilotPlan("transport", "pi-child-sdk");
    expect(codex.rows.map(row => `${row.sender}->${row.receiver}`)).toEqual(["child-1->codex-1", "codex-1->child-1", "child-1->codex-2", "codex-2->child-1"]);
    expect(sdk.rows.map(row => `${row.sender}->${row.receiver}`)).toEqual(["child-1->pi-1", "pi-1->child-1", "child-1->pi-2", "pi-2->child-1"]);
    for (const plan of [codex, sdk]) expect(plan).toMatchObject({ actorCount: 3, genericRoutes: [], piRoute: "pi-native-child" });
    expect(codex.modelRoutes).toEqual(["pi-native-child", "codex-appserver", "codex-headless"]);
    expect(sdk.modelRoutes).toEqual(["pi-native-child", "pi-rpc-cli", "pi-sdk"]);
    expect(new Set(codex.rows.flatMap(row => [row.sender, row.receiver]).filter(alias => alias.startsWith("child-"))).size).toBe(1);
    expect(new Set(sdk.rows.flatMap(row => [row.sender, row.receiver]).filter(alias => alias.startsWith("child-"))).size).toBe(1);
  });
  it("fails closed for child bridge variants outside transport", () => {
    expect(selectPilotVariant("transport", "pi-child-codex")).toBe("pi-child-codex");
    expect(selectPilotVariant("transport", "pi-child-sdk")).toBe("pi-child-sdk");
    expect(() => selectPilotVariant("stdio", "pi-child-codex")).toThrow(/transport gate/);
    expect(() => selectPilotVariant("transport", "pi-child-nope")).toThrow(/unknown/);
  });
  it("plans the pi-child-extended batch with four actors and six directed rows against the three new routes", () => {
    const plan = pilotPlan("transport", "pi-child-extended");
    expect(plan.rows.map((row) => `${row.sender}->${row.receiver}`)).toEqual([
      "child-1->interactive-1", "interactive-1->child-1",
      "child-1->fork-1", "fork-1->child-1",
      "child-1->piint-1", "piint-1->child-1",
    ]);
    expect(plan).toMatchObject({ id: "transport", actorCount: 4, genericRoutes: [], transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-native-child", variant: "pi-child-extended" });
    expect(plan.modelRoutes).toEqual(["pi-native-child", "codex-interactive", "codex-fork", "pi-interactive"]);
    expect(plan.sourceFiles).toContain("tests/acceptance/qualification-pi-child.ts");
    expect(plan.sourceFiles).toContain("tests/acceptance/qualification-pi-interactive.ts");
    expect(new Set(plan.rows.flatMap((row) => [row.sender, row.receiver]).filter((alias) => alias.startsWith("child-"))).size).toBe(1);
    expect(new Set(plan.rows.flatMap((row) => [row.sender, row.receiver])).size).toBe(4);
    expect(selectPilotVariant("transport", "pi-child-extended")).toBe("pi-child-extended");
    expect(() => selectPilotVariant("stdio", "pi-child-extended")).toThrow(/transport gate/);
    expect(selectPilotRows(plan.rows, "child-to-piint,piint-to-child").map((row) => row.id)).toEqual(["child-to-piint", "piint-to-child"]);
  });
  it("plans the codex extended live-route batch with four actors and eight directed rows", () => {
    const plan = pilotPlan("transport", "codex-extended");
    expect(plan.rows.map((row) => `${row.sender}->${row.receiver}`)).toEqual([
      "interactive-1->codex-1", "codex-1->interactive-1", "interactive-1->codex-2", "codex-2->interactive-1",
      "fork-1->codex-1", "codex-1->fork-1", "fork-1->codex-2", "codex-2->fork-1",
    ]);
    expect(plan).toMatchObject({ id: "transport", actorCount: 4, genericRoutes: [], transportEvidence: false, codexRoute: "codex-appserver", variant: "codex-extended" });
    expect(plan.modelRoutes).toEqual(["codex-interactive", "codex-fork", "codex-appserver", "codex-headless"]);
    expect(selectPilotVariant("transport", "codex-extended")).toBe("codex-extended");
    expect(() => selectPilotVariant("stdio", "codex-extended")).toThrow(/transport gate/);
    const selected = selectPilotRows(plan.rows, "fork-to-appserver");
    expect(selected.map((row) => row.id)).toEqual(["fork-to-appserver"]);
  });
  it("plans the pi-interactive batch with five actors and eight directed rows", () => {
    const plan = pilotPlan("transport", "pi-interactive");
    expect(plan.rows.map((row) => `${row.sender}->${row.receiver}`)).toEqual([
      "piint-1->codex-1", "codex-1->piint-1", "piint-1->codex-2", "codex-2->piint-1",
      "piint-1->pi-1", "pi-1->piint-1", "piint-1->pi-2", "pi-2->piint-1",
    ]);
    expect(plan).toMatchObject({ id: "transport", actorCount: 5, genericRoutes: [], transportEvidence: false, codexRoute: "codex-appserver", piRoute: "pi-rpc-cli", variant: "pi-interactive" });
    expect(plan.modelRoutes).toEqual(["pi-interactive", "codex-appserver", "codex-headless", "pi-rpc-cli", "pi-sdk"]);
    expect(selectPilotVariant("transport", "pi-interactive")).toBe("pi-interactive");
    expect(() => selectPilotVariant("stdio", "pi-interactive")).toThrow(/transport gate/);
    expect(selectPilotRows(plan.rows, "piint-to-sdk,sdk-to-piint").map((row) => row.id)).toEqual(["piint-to-sdk", "sdk-to-piint"]);
  });
  it("plans the extended-mix batch with five actors and fourteen directed rows", () => {
    const plan = pilotPlan("transport", "extended-mix");
    expect(plan.rows).toHaveLength(14);
    expect(plan).toMatchObject({ id: "transport", actorCount: 5, genericRoutes: [], variant: "extended-mix" });
    expect(plan.modelRoutes).toEqual(["codex-interactive", "codex-fork", "pi-interactive", "pi-rpc-cli", "pi-sdk"]);
    const aliases: Record<string, string> = { "interactive-1": "codex-interactive", "fork-1": "codex-fork", "piint-1": "pi-interactive", "pi-1": "pi-rpc-cli", "pi-2": "pi-sdk" };
    expect(plan.rows.map((row) => `${aliases[row.sender]}->${aliases[row.receiver]}`.replace("pi-interactive", "piint")).sort()).toEqual([
      "codex-interactive->codex-fork", "codex-fork->codex-interactive",
      "codex-interactive->piint", "piint->codex-interactive",
      "codex-fork->piint", "piint->codex-fork",
      "codex-interactive->pi-rpc-cli", "pi-rpc-cli->codex-interactive",
      "codex-interactive->pi-sdk", "pi-sdk->codex-interactive",
      "codex-fork->pi-rpc-cli", "pi-rpc-cli->codex-fork",
      "codex-fork->pi-sdk", "pi-sdk->codex-fork",
    ].sort());
    expect(selectPilotVariant("transport", "extended-mix")).toBe("extended-mix");
    expect(() => selectPilotVariant("stdio", "extended-mix")).toThrow(/transport gate/);
  });
  it("plans the extended-generics batch with nine actors and eighteen mixed rows", () => {
    const plan = pilotPlan("transport", "extended-generics");
    expect(plan.rows).toHaveLength(18);
    expect(plan.rows.every((row) => row.kind === "mixed")).toBe(true);
    expect(plan).toMatchObject({ id: "transport", actorCount: 9, transportEvidence: true, variant: "extended-generics" });
    expect(plan.genericRoutes).toEqual(["generic-stdio", "generic-http", "generic-stateless"]);
    expect(plan.modelRoutes).toEqual(["codex-interactive", "codex-fork", "pi-interactive"]);
    expect(selectPilotVariant("transport", "extended-generics")).toBe("extended-generics");
    expect(() => selectPilotVariant("stdio", "extended-generics")).toThrow(/transport gate/);
  });
  it("selects the pi-child matrix with four child actor slots and seven ordered rows", () => {
    const plan = pilotPlan("transport", "pi-child");
    expect(plan.rows.map((row) => `${row.sender}->${row.receiver}`)).toEqual([
      "child-1->child-2", "child-1->generic-1", "child-1->http-1", "child-1->stateless-1",
      "generic-2->child-2", "http-2->child-2", "stateless-2->child-2",
    ]);
    expect(plan.rows).toHaveLength(7);
    expect(plan.actorCount).toBe(4);
    expect(plan.piRoute).toBe("pi-native-child");
    expect(plan.rows.every((row) => row.sender.startsWith("child-") || row.receiver.startsWith("child-") || row.kind === "mixed")).toBe(true);
    expect(plan.modelRoutes).toEqual(["pi-native-child", "pi-native-child"]);
    expect(selectPilotMode(false, true, false)).toBe("transport");
    expect(selectPilotMode(false, false, false, true)).toBe("transport");
    expect(() => selectPilotVariant("stdio", "pi-child")).toThrow(/transport gate/);
  });
  it("plans eight directed cross-cohort rows across four distinct model routes", () => {
    const plan = pilotPlan("cross");
    expect(plan.rows).toHaveLength(8);
    expect(plan.rows.every(row => row.kind === "model")).toBe(true);
    expect(plan.modelRoutes).toEqual(["codex-appserver", "codex-headless", "pi-rpc-cli", "pi-sdk"]);
    const routes = Object.fromEntries(["codex-1", "codex-2", "pi-1", "pi-2"].map((alias, index) => [alias, plan.modelRoutes[index]]));
    expect(plan.rows.map(row => `${routes[row.sender]}->${routes[row.receiver]}`).sort()).toEqual([
      "codex-appserver->codex-headless", "codex-headless->codex-appserver",
      "pi-rpc-cli->pi-sdk", "pi-sdk->pi-rpc-cli",
      "codex-appserver->pi-sdk", "pi-sdk->codex-appserver",
      "pi-rpc-cli->codex-headless", "codex-headless->pi-rpc-cli",
    ].sort());
    expect(selectPilotMode(false, false, true)).toBe("cross");
    expect(() => selectPilotMode(false, false, false)).toThrow(/exactly one/);
    expect(plan.genericRoutes).toEqual([]);
    expect(() => selectPilotMode(true, false, true)).toThrow(/exactly one/);
    expect(() => selectPilotMode(false, true, true)).toThrow(/exactly one/);
    expect(() => pilotPlan("cross", "headless-sdk")).toThrow(/incompatible/);
  });

  it("requires owned HTTP identity, readiness, and post-close termination evidence", async () => {
    const missing = { transportEvidence: () => null } satisfies GenericTransportEvidenceFactory;
    expect(() => requireTransportEvidence(missing, "before")).toThrow(/no owned HTTP snapshot/);
    const before: GenericTransportEvidence = { transport: "http", port: 3100, child_pid: 7, node_path: "/node", node_executable_sha256: "a".repeat(64), script_path: "dist/transports/http.js", script_sha256: "b".repeat(64), startup_ready: true, exit_code: null, signal_code: null, terminated: false };
    const after: GenericTransportEvidence = { ...before, exit_code: 0, terminated: true };
    expect(() => requireTransportEvidence({ transportEvidence: () => before }, "after")).toThrow(/termination/);
    expect(() => requireTransportEvidence({ transportEvidence: () => ({ ...before, terminated: true }) }, "after")).toThrow(/termination/);
    expect(requireTransportEvidence({ transportEvidence: () => after }, "after")).toEqual(after);
    expect(sameTransportIdentity(before, after)).toBe(true);
    expect(sameTransportIdentity(before, { ...after, child_pid: 8 })).toBe(false);
    expect(() => requireTransportEvidence({ transportEvidence: () => ({ ...before, startup_ready: false }) }, "before")).toThrow(/identity\/readiness/);
  });

  it("accounts for every declared row when startup fails before row execution", () => {
    const materialized = materializeUnrunRows(pilotRows, [], [], "initialization failed before row execution");
    expect(materialized.rows).toEqual([]);
    expect(materialized.unfinishedRows).toHaveLength(pilotRows.length);
    expect(materialized.unfinishedRows.map((row) => row.id)).toEqual(pilotRows.map((row) => row.id));
    expect(materialized.unfinishedRows.every((row) => row.status === "unfinished" && (row.execution as Json).status === "not_run")).toBe(true);
  });

  it("preserves partial rows and avoids duplicate materialization", () => {
    const partial = [{ id: "same-codex", status: "failed" }];
    const materialized = materializeUnrunRows(pilotRows, partial, [{ id: "same-pi", status: "unfinished" }], "startup failed");
    expect(materialized.rows).toBe(partial);
    expect(materialized.unfinishedRows.map((row) => row.id)).toEqual(["same-pi", "cross-codex-to-pi", "cross-pi-to-codex", "mixed-generic-to-codex", "mixed-codex-to-generic", "mixed-generic-to-pi", "mixed-pi-to-generic"]);
    expect(new Set(materialized.unfinishedRows.map((row) => row.id)).size).toBe(materialized.unfinishedRows.length);
    expect(materialized.rows.length + materialized.unfinishedRows.length).toBe(pilotRows.length);
  });

  it("sanitizes diagnostic attachments without discarding their shape", () => {
    const error = Object.assign(new Error("thread initialization failed"), { evidence: { thread: { id: "thread-1" }, runtime: { status: "error" }, authorization: "private" } });
    expect(sanitizeFailureEvidence(error)).toEqual({ thread: { id: "thread-1" }, runtime: { status: "error" }, authorization: "[redacted]" });
    expect(sanitizeFailureEvidence(new Error("without evidence"))).toBeUndefined();
  });
});

describe.skipIf(!enabled)("bounded qualification cohort pilot", () => {
  it("runs the selected fresh, independently adjudicated route rows", async () => {
    const mode = selectPilotMode(stdioEnabled, transportEnabled, crossEnabled);
    const planBase = pilotPlan(mode, selectPilotVariant(mode === "cross" ? "transport" : mode, routeVariant));
    const rowSelector = process.env.GPTQUEUE_QUALIFICATION_ROW_IDS;
    const plan = specializePlanRows(planBase, selectPilotRows(planBase.rows, rowSelector));
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    const rowsDir = join(artifactDir, "rows");
    await mkdir(rowsDir, { recursive: true, mode: 0o700 });
      const receipt: Json = { schema_version: 1, run_id: runId, row_selector: rowSelector ?? null, selected_row_ids: plan.rows.map((row) => row.id), declared_plan: { id: plan.id, variant: plan.variant, model_routes: plan.modelRoutes, rows: plan.rows, generic_routes: plan.genericRoutes, transport_evidence: plan.transportEvidence, child_actor_count: plan.variant === "pi-child" ? 4 : childBridgeVariant(plan.variant) ? 1 : 0, model_slot_count: childBridgeVariant(plan.variant) ? 4 : undefined, actor_count: plan.actorCount, codex_launches: plan.variant === "pi-child" || childBridgeVariant(plan.variant) ? 0 : undefined }, execution: { status: "running" }, passed: false, started_at: new Date().toISOString(), rows: [], unfinished_rows: [], retired_participants: [], source_hashes: {}, source_hashes_after: {}, cleanup: [], phases: [] };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true, redactSessionObjectIds: true }), null, 2)}\n`, { mode: 0o600 });
    };
    const before = await sourceHashes(plan.sourceFiles);
    receipt.source_hashes = before;
    await persist("source-hashes-before", before);
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let codexSet: ReturnType<typeof createCodexAdapters> | undefined;
    let extendedCodexSet: ReturnType<typeof createCodexAdapters> | undefined;
    let piintAdapter: RouteAdapter | undefined;
    let piSet: ReturnType<typeof createPiAdapters> | undefined;
    let piChildAdapter: RouteAdapter | undefined;
    let releaseChildProfile: (() => void) | undefined;
    let genericSet: ReturnType<typeof createGenericAdapters> | undefined;
    const participants: Participant[] = [];
    const prelaunched = new Map<string, Participant>();
    const bridgeAvailability: Availability[] = [];
    const discoveryTraces = new Map<string, readonly NativeTrace[]>();
    const retired = new Set<string>();
    let activeRow = "bootstrap";
    let artifactSequence = 0;
    let failure: unknown;
    let transportBefore: GenericTransportEvidence | undefined;
    const writer: TypedEvidenceWriter = {
      writeHistory: async ({ actor, serialized }, signal) => {
        if (signal.aborted) throw abortError(signal);
        const path = join(rowsDir, `${safeName(activeRow)}-${String(++artifactSequence).padStart(5, "0")}-${safeName(actor.agent)}.json`);
        await writeFile(path, serialized, { mode: 0o600 });
        return { path, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: before["tests/acceptance/qualification-driver.ts"] ?? "source", oracleRevision: before["tests/acceptance/oracle.ts"] ?? "oracle" };
      },
    };
    const participantMap = (): ParticipantMap => Object.freeze(Object.fromEntries(participants.map((participant) => [participant.identity.participantId, participant])));
    const rowParticipants = (row: PilotRow, map: ParticipantMap, aliases: Readonly<Record<string, string>>): readonly Participant[] => resolvePilotAliases(row, aliases, new Set(Object.keys(map))).map((participantId) => map[participantId]).filter((participant): participant is Participant => participant !== undefined);
    const waitReady = async (values: readonly Participant[], parent: AbortSignal): Promise<readonly Json[]> => {
      const bounded = boundedSignal(parent, 30_000);
      try {
        while (true) {
          const statuses = await Promise.all(values.map((participant) => participant.status(bounded.signal)));
          if (statuses.some((status) => status.kind === "terminated")) throw new Error(`cohort participant terminated while awaiting readiness: ${JSON.stringify(statuses.map(asStatus))}`);
          for (const [index, status] of statuses.entries()) {
            const participant = values[index];
            if (participant && status.kind !== "unknown" && "runtimeId" in status && status.runtimeId !== participant.identity.hostRuntimeId) throw new Error(`cohort runtime identity changed for ${participant.identity.agent}`);
          }
          if (!statuses.some((status) => status.kind === "busy")) return statuses.map(asStatus);
          await delay(250, bounded.signal);
        }
      } finally { bounded.cancel(); }
    };
    const initializeModel = async (participant: ModelParticipant, signal: AbortSignal): Promise<RawEvidenceRef> => {
      const prompt = participant.identity.route === "pi-native-child"
        ? `Call get_runtime_status exactly once and list_agents exactly once, then reply COHORT_RUNTIME_READY ${runId}. Use only the runtime and discovery tools for initialization; you may use send_message solely to return the controller result. Before qualification rows begin, follow this answer protocol for peer tasks: when a task content matches "qualification <nonce>: calculate <left>+<right>", independently calculate the decimal sum and reply with exactly "answer <nonce>: <computed-decimal>" using the requested result correlation fields. Do not infer or state expected numeric answers from this prompt.`
        : participant.identity.route === "pi-interactive"
          ? `Call get_runtime_status exactly once and list_agents exactly once, then reply COHORT_RUNTIME_READY ${runId}. Do not use any other GPTQueue tool. Before qualification rows begin, adopt this answer protocol for peer tasks: when a task content matches "qualification <nonce>: calculate <left>+<right>", independently calculate the decimal sum and reply with exactly "answer <nonce>: <computed-decimal>" with in_reply_to equal to the task message id. Consume peer tasks only with the native receive_message tool (timeout 60, call it again while it returns no_messages); never use claim_tasks or acknowledge_tasks. Do not infer or state expected numeric answers from this prompt.`
          : `Call get_runtime_status exactly once and list_agents exactly once, then reply COHORT_RUNTIME_READY ${runId}. Do not use any other GPTQueue tool.`;
      await participant.prompt(prompt, signal);
      const history = await participant.history(signal);
      const serialized = `${JSON.stringify(sanitizeEvidence(history, { parseEmbeddedJson: true, redactSessionObjectIds: true }) ?? null)}\n`;
      const historyRef = await writer.writeHistory({ actor: participant.identity, history, serialized }, signal);
      const traces = extractNativeTraces(history, participant.identity, historyRef);
      if (!traces.some((trace) => trace.name === "get_runtime_status" && trace.successful && trace.runtimeBound)) throw new Error(`missing verified runtime status for ${participant.identity.agent}`);
      if (!traces.some((trace) => trace.name === "list_agents" && trace.successful && trace.runtimeBound)) throw new Error(`missing verified list_agents discovery for ${participant.identity.agent}`);
      discoveryTraces.set(participant.identity.participantId, traces);
      return historyRef;
    };
    const initializeGeneric = async (participant: Participant, signal: AbortSignal): Promise<RawEvidenceRef> => {
      if (participant.kind !== "generic") throw new Error("generic discovery requires a generic participant");
      await participant.call("list_agents", {}, signal);
      const history = await participant.history(signal);
      const serialized = `${JSON.stringify(sanitizeEvidence(history, { parseEmbeddedJson: true, redactSessionObjectIds: true }) ?? null)}\n`;
      const historyRef = await writer.writeHistory({ actor: participant.identity, history, serialized }, signal);
      const traces = extractGenericTraces(history, participant.identity, historyRef);
      if (!traces.some((trace) => trace.name === "list_agents" && trace.successful && trace.runtimeBound)) throw new Error(`missing verified list_agents discovery for ${participant.identity.agent}`);
      discoveryTraces.set(participant.identity.participantId, traces);
      return historyRef;
    };
    try {
      const gateSignal = AbortSignal.timeout(1_650_000);
      redis = await startOwnedRedis();
      receipt.redis = { database: new URL(redis.url).pathname.slice(1), host: new URL(redis.url).hostname, port: new URL(redis.url).port };
      workspace = await mkdtemp(join(tmpdir(), "gptqueue-qualification-cohort-pilot-"));
      const codexRoot = join(workspace, "codex"), piRoot = join(workspace, "pi");
      await mkdir(codexRoot, { recursive: true }); await mkdir(piRoot, { recursive: true });
      codexSet = createCodexAdapters({ workspaceRoot: codexRoot, model: "gpt-5.6-luna" });
      piSet = createPiAdapters({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" });
      genericSet = createGenericAdapters({ repo });
      let childProfile: string | undefined;
      let childModelRuntime: any;
      if (plan.variant === "pi-child" || childBridgeVariant(plan.variant)) {
        const sdk = await import(pathToFileURL(nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist/index.js")).href) as any;
        childModelRuntime = await sdk.ModelRuntime.create({ allowModelNetwork: false });
        const modelValue = childModelRuntime.getModel("openrouter", "z-ai/glm-5.3-flash");
        if (!modelValue || !childModelRuntime.hasConfiguredAuth("openrouter")) throw new Error("Pi child cohort model/auth is unavailable");
        childProfile = join(workspace, "pi-child-profile");
        await mkdir(childProfile, { recursive: true, mode: 0o700 });
      }
      const childPlan = plan.variant === "pi-child" || childBridgeVariant(plan.variant);
      const childExtendedPlan = plan.variant === "pi-child-extended";
      const extendedPlan = plan.variant === "codex-extended";
      const piintPlan = plan.variant === "pi-interactive";
      const egPlan = plan.variant === "extended-generics";
      const mixPlan = plan.variant === "extended-mix";
      if (extendedPlan || mixPlan || egPlan || childExtendedPlan) extendedCodexSet = createCodexAdapters({ model: "gpt-5.6-luna", tuiTrustRoot: repo });
      if (piintPlan || mixPlan || egPlan || childExtendedPlan) piintAdapter = createPiInteractiveAdapter({ provider: "openrouter", model: "z-ai/glm-5.3-flash" });
      const codexAdapter = childPlan ? undefined : codexSet.adapters.find(({ spec }) => spec.id === plan.codexRoute);
      let piAdapter = childPlan ? piChildAdapter : plan.piRoute === "pi-sdk"
        ? createPiSdkAdapter({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" })
        : piSet.adapters.find(({ spec }) => spec.id === plan.piRoute);
      const bridgeAdapters = childBridgeVariant(plan.variant)
        ? (plan.variant === "pi-child-codex"
          ? [codexSet.adapters.find(({ spec }) => spec.id === "codex-appserver"), codexSet.adapters.find(({ spec }) => spec.id === "codex-headless")]
          : plan.variant === "pi-child-extended"
            ? [
              extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-interactive"),
              extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-fork"),
              piintAdapter,
            ]
            : [piSet.adapters.find(({ spec }) => spec.id === "pi-rpc-cli"), createPiSdkAdapter({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" })])
        : [];
      if (childBridgeVariant(plan.variant)) {
        const bridgePeers: readonly (readonly [string, RouteAdapter | undefined])[] = plan.variant === "pi-child-codex"
          ? [["codex-1", bridgeAdapters[0]], ["codex-2", bridgeAdapters[1]]]
          : plan.variant === "pi-child-extended"
            ? [["interactive-1", bridgeAdapters[0]], ["fork-1", bridgeAdapters[1]], ["piint-1", bridgeAdapters[2]]]
            : [["pi-1", bridgeAdapters[0]], ["pi-2", bridgeAdapters[1]]];
        for (const [alias, adapter] of bridgePeers) {
          if (!adapter) throw new Error(`missing child bridge adapter for ${alias}`);
          const availability = await adapter.preflight(gateSignal);
          if (availability.kind !== "available") throw new Error(`child bridge peer unavailable: ${availability.detail}`);
          bridgeAvailability.push(availability);
          const participant = await adapter.launch({ role: "receiver", pairId: `cohort-${alias}-${runId}`, nonce: runId, redisUrl: redis!.url }, gateSignal);
          participants.push(participant); prelaunched.set(alias, participant);
        }
        if (!childProfile || !childModelRuntime) throw new Error("child profile/runtime was not prepared");
        releaseChildProfile = enterPiNativeChildProfileScope(childProfile);
        await persist("pi-child-profile-scope", { profile: resolve(childProfile), selection: "owned process-lifetime profile", model_runtime: "preloaded before environment scope" });
        piAdapter = createPiNativeChildAdapter({ workspaceRoot: join(workspace, "pi-child"), provider: "openrouter", model: "z-ai/glm-5.3-flash", profileRoot: childProfile, modelRuntime: childModelRuntime });
      }
      const genericAdapterCandidates = plan.genericRoutes.map((route) => genericSet!.adapters.find(({ spec }) => spec.id === route));
      const egAdapters = egPlan ? [
        extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-interactive"),
        extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-fork"),
        piintAdapter,
      ] : [];
      const extendedAdapters = extendedPlan
        ? [
          extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-interactive"),
          extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-fork"),
          extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-appserver"),
          extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-headless"),
        ]
        : [];
      const piintAdapters = piintPlan ? [piintAdapter, codexSet.adapters.find(({ spec }) => spec.id === "codex-appserver"), codexSet.adapters.find(({ spec }) => spec.id === "codex-headless"), piSet.adapters.find(({ spec }) => spec.id === "pi-rpc-cli"), createPiSdkAdapter({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" })] : [];
      const mixAdapters = mixPlan ? [
        extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-interactive"),
        extendedCodexSet!.adapters.find(({ spec }) => spec.id === "codex-fork"),
        piintAdapter,
        piSet.adapters.find(({ spec }) => spec.id === "pi-rpc-cli"),
        createPiSdkAdapter({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" }),
      ] : [];
      if (extendedPlan) {
        if (extendedAdapters.some((adapter) => adapter === undefined)) throw new Error("pilot adapters are unavailable");
      } else if (piintPlan) {
        if (piintAdapters.some((adapter) => adapter === undefined)) throw new Error("pilot adapters are unavailable");
      } else if (mixPlan) {
        if (mixAdapters.some((adapter) => adapter === undefined)) throw new Error("pilot adapters are unavailable");
      } else if (egPlan) {
        if (egAdapters.some((adapter) => adapter === undefined) || genericAdapterCandidates.some((adapter) => adapter === undefined)) throw new Error("pilot adapters are unavailable");
      } else if ((!childPlan && !codexAdapter) || !piAdapter || bridgeAdapters.some((adapter) => adapter === undefined) || genericAdapterCandidates.some((adapter) => adapter === undefined)) throw new Error("pilot adapters are unavailable");
      const genericAdapters = genericAdapterCandidates.filter((adapter): adapter is RouteAdapter => adapter !== undefined);
      if (genericAdapters.length !== plan.genericRoutes.length) throw new Error("pilot adapters are unavailable");
      const crossAdapters = plan.id === "cross" ? [
        codexSet.adapters.find(({ spec }) => spec.id === "codex-appserver"), codexSet.adapters.find(({ spec }) => spec.id === "codex-headless"),
        piSet.adapters.find(({ spec }) => spec.id === "pi-rpc-cli"), createPiSdkAdapter({ workspaceRoot: piRoot, provider: "openrouter", model: "z-ai/glm-5.3-flash" }),
      ] : [];
      if (plan.id === "cross" && crossAdapters.some((adapter) => adapter === undefined)) throw new Error("cross cohort adapters are unavailable");
      const adapters: readonly RouteAdapter[] = plan.id === "cross" ? crossAdapters.filter((adapter): adapter is RouteAdapter => adapter !== undefined) : childPlan ? [piAdapter!, ...bridgeAdapters.filter((adapter): adapter is RouteAdapter => adapter !== undefined), ...genericAdapters] : extendedPlan ? extendedAdapters.flatMap((adapter) => (adapter ? [adapter] : [])) : piintPlan ? piintAdapters.flatMap((adapter) => (adapter ? [adapter] : [])) : mixPlan ? mixAdapters.flatMap((adapter) => (adapter ? [adapter] : [])) : egPlan ? [...egAdapters.flatMap((adapter) => (adapter ? [adapter] : [])), ...genericAdapters] : [codexAdapter!, piAdapter!, ...genericAdapters];
      const availability = childBridgeVariant(plan.variant)
        ? [...await Promise.all([piAdapter!, ...genericAdapters].map((adapter) => adapter.preflight(gateSignal))), ...bridgeAvailability]
        : await Promise.all(adapters.map((adapter) => adapter.preflight(gateSignal)));
      await persist("preflight", { plan: receipt.declared_plan, routes: adapters.map(({ spec }) => spec.id), availability });
      expect(availability.every((value) => value.kind === "available")).toBe(true);
      const launch = async (adapter: RouteAdapter, name: string, role: "sender" | "receiver"): Promise<Participant> => {
        const participant = await adapter.launch({ role, pairId: `cohort-${name}-${runId}`, nonce: runId, redisUrl: redis!.url }, gateSignal);
        participants.push(participant);
        return participant;
      };
      const aliases: Record<string, string> = {};
      if (plan.id === "cross") {
        for (const [alias, adapter, role] of [["codex-1", crossAdapters[0], "sender"], ["codex-2", crossAdapters[1], "receiver"], ["pi-1", crossAdapters[2], "sender"], ["pi-2", crossAdapters[3], "receiver"]] as const) {
          if (!plan.rows.some((row) => row.sender === alias || row.receiver === alias)) continue;
          if (!adapter) throw new Error(`missing cross adapter for ${alias}`);
          aliases[alias] = (await launch(adapter, alias, role)).identity.participantId;
        }
      } else if (childPlan) {
        aliases["child-1"] = (await launch(piAdapter!, "child-1", "sender")).identity.participantId;
        if (childBridgeVariant(plan.variant)) {
          for (const alias of plan.variant === "pi-child-codex" ? ["codex-1", "codex-2"] : plan.variant === "pi-child-extended" ? ["interactive-1", "fork-1", "piint-1"] : ["pi-1", "pi-2"]) {
            const participant = prelaunched.get(alias);
            if (!participant) throw new Error(`missing prelaunched child bridge peer for ${alias}`);
            aliases[alias] = participant.identity.participantId;
          }
        } else {
          aliases["child-2"] = (await launch(piAdapter!, "child-2", "receiver")).identity.participantId;
        }
      } else if (extendedPlan) {
        for (const [index, [alias, route]] of Object.entries(codexExtendedAliasRoutes).entries()) {
          const adapter = extendedAdapters[index];
          if (!adapter) throw new Error(`missing extended codex adapter for ${route}`);
          aliases[alias] = (await launch(adapter, alias, alias === "codex-1" ? "receiver" : "sender")).identity.participantId;
        }
      } else if (piintPlan) {
        for (const [index, [alias, route]] of Object.entries(piInteractiveAliasRoutes).entries()) {
          const adapter = piintAdapters[index];
          if (!adapter) throw new Error(`missing pi-interactive adapter for ${route}`);
          aliases[alias] = (await launch(adapter, alias, alias === "codex-1" ? "receiver" : "sender")).identity.participantId;
        }
      } else if (mixPlan) {
        for (const [index, [alias, route]] of Object.entries(extendedMixAliasRoutes).entries()) {
          const adapter = mixAdapters[index];
          if (!adapter) throw new Error(`missing extended-mix adapter for ${route}`);
          aliases[alias] = (await launch(adapter, alias, alias === "pi-1" || alias === "pi-2" ? "receiver" : "sender")).identity.participantId;
        }
      } else if (egPlan) {
        for (const [index, [alias, route]] of Object.entries(extendedGenericAliasRoutes).entries()) {
          const adapter = egAdapters[index];
          if (!adapter) throw new Error(`missing extended-generics adapter for ${route}`);
          aliases[alias] = (await launch(adapter, alias, "sender")).identity.participantId;
        }
      } else for (const [alias, adapter, role] of [["codex-1", codexAdapter!, "sender"], ["codex-2", codexAdapter!, "receiver"], ["pi-1", piAdapter!, "sender"], ["pi-2", piAdapter!, "receiver"]] as const) aliases[alias] = (await launch(adapter, alias, role)).identity.participantId;
      for (const [index, adapter] of genericAdapters.entries()) {
        const route = plan.genericRoutes[index]!;
        const aliasPrefix = route === "generic-stdio" ? "generic" : route.replace(/^generic-/, "");
        for (const ordinal of [1, 2] as const) {
          const alias = `${aliasPrefix}-${ordinal}`;
          aliases[alias] = (await launch(adapter, alias, ordinal === 1 ? "sender" : "receiver")).identity.participantId;
        }
      }
      await persist("launched", { plan: receipt.declared_plan, participants: participants.map(identity), aliases, redis_url: "redacted-private-loopback" });
      const models = participants.filter((participant): participant is ModelParticipant => participant.kind === "model");
      const runtimeRefs = await Promise.all(models.map((participant) => initializeModel(participant, gateSignal)));
      const genericRefs = await Promise.all(participants.filter((participant) => participant.kind === "generic").map((participant) => initializeGeneric(participant, gateSignal)));
      await persist("runtime-initialized", { participants: models.map(identity), raw: [...runtimeRefs, ...genericRefs], communication_consumption: pilotConsumption, discovery: "list_agents" });
      const map = participantMap();
      for (const row of plan.rows) {
        const rowValues = rowParticipants(row, map, aliases);
        const rowRecord: Json = { id: row.id, sender: row.sender, receiver: row.receiver, expected_decimal: row.left + row.right, nonce: `${runId}:${row.id}:${randomUUID()}`, status: "running" };
        activeRow = row.id;
        if (rowValues.length !== 2 || rowValues.some((participant) => retired.has(participant.identity.participantId))) {
          rowRecord.status = "unfinished"; rowRecord.reason = "participant retired or unavailable";
          (receipt.unfinished_rows as Json[]).push(rowRecord); await persist(`row-${row.id}-unfinished`, rowRecord); continue;
        }
        const rowSignal = boundedSignal(gateSignal, 190_000);
        try {
          rowRecord.readiness_before = await waitReady(rowValues, rowSignal.signal);
          const [senderId, receiverId] = resolvePilotAliases(row, aliases, new Set(Object.keys(map)));
          const sender = map[senderId]!;
          const receiver = map[receiverId]!;
          const senderTraces = discoveryTraces.get(sender.identity.participantId) ?? [];
          const receiverTraces = discoveryTraces.get(receiver.identity.participantId) ?? [];
          rowRecord.discovery = {
            sender: collectPeerDiscovery(sender.identity, receiver.identity, senderTraces),
            receiver: collectPeerDiscovery(receiver.identity, sender.identity, receiverTraces),
          };
          const pair: PairSpec = { pairId: `pilot-${row.id}-${runId}`, sender: map[senderId]!.identity.route, receiver: map[receiverId]!.identity.route, nonce: String(rowRecord.nonce) };
          const rowPool = new CohortLeasePool(rowValues.map((participant): CohortMember => ({ participant, identity: participant.identity })));
          const result = row.kind === "model"
            ? await runModelPair(rowPool, { pair, left: row.left, right: row.right, evidenceWriter: writer, consumption: pilotConsumption }, rowSignal.signal)
            : await runMixedPair(rowPool, { pair, left: row.left, right: row.right, evidenceWriter: writer }, rowSignal.signal);
          rowRecord.lease = result.lease; rowRecord.exchange = result.exchange;
          expect(result.lease.sender.participantId).toBe(senderId);
          expect(result.lease.receiver.participantId).toBe(receiverId);
          const verdict = checkExchangeEvidence(result.exchange!);
          rowRecord.status = verdict.outcome === "meets" ? "passed" : "failed";
          rowRecord.verdict = verdict; rowRecord.raw = result.raw; rowRecord.execution = result.execution;
          if (verdict.outcome !== "meets") throw new Error(`row ${row.id} did not meet exchange oracle`);
          rowRecord.readiness_after = await waitReady(rowValues, rowSignal.signal);
          (receipt.rows as Json[]).push(rowRecord); await persist(`row-${row.id}-passed`, rowRecord);
        } catch (error) {
          const exchangeSatisfied = rowRecord.verdict !== undefined && typeof rowRecord.verdict === "object" && (rowRecord.verdict as Json).outcome === "meets";
          rowRecord.status = exchangeSatisfied ? "passed" : "failed";
          rowRecord.error = String(error);
          const evidence = error && typeof error === "object" ? error as Json : {};
          if (evidence.raw !== undefined) rowRecord.raw = evidence.raw;
          if (evidence.histories !== undefined) rowRecord.failure_histories = evidence.histories;
          if (!exchangeSatisfied) for (const participant of rowValues) retired.add(participant.identity.participantId);
          try { rowRecord.readiness_after = await waitReady(rowValues, AbortSignal.timeout(30_000)); }
          catch (readinessError) {
            rowRecord.readiness_after_error = String(readinessError);
            for (const participant of rowValues) retired.add(participant.identity.participantId);
          }
          (receipt.rows as Json[]).push(rowRecord); await persist(`row-${row.id}-${exchangeSatisfied ? "passed-with-readiness-boundary" : "failed"}`, rowRecord);
        } finally { rowSignal.cancel(); }
      }
      receipt.retired_participants = [...retired];
      if ((receipt.rows as Json[]).some((row) => row.status !== "passed") || (receipt.unfinished_rows as Json[]).length > 0) throw new Error("cohort pilot has failed or unfinished rows");
      receipt.execution = { status: "completed" }; receipt.passed = true;
    } catch (error) {
      failure = error; receipt.execution = { status: "failed", detail: String(error) }; receipt.error = String(error);
      const diagnosticEvidence = sanitizeFailureEvidence(error);
      const accounted = materializeUnrunRows(plan.rows, receipt.rows as Json[], receipt.unfinished_rows as Json[], "initialization or pilot execution failed before this row ran");
      receipt.rows = accounted.rows;
      receipt.unfinished_rows = accounted.unfinishedRows;
      if (diagnosticEvidence !== undefined) {
        receipt.failure_evidence = diagnosticEvidence;
        await persist("startup-failure-evidence", diagnosticEvidence).catch(() => undefined);
      }
      await persist("pilot-failure", { error: String(error), rows: receipt.rows, unfinished_rows: receipt.unfinished_rows }).catch(() => undefined);
    } finally {
      const cleanup: CleanupResult[] = [];
      if (plan.transportEvidence) {
        try {
          if (!genericSet) throw new Error("transport evidence factory is unavailable");
          const evidence = requireTransportEvidence(genericSet, "before");
          transportBefore = evidence;
          receipt.transport_evidence_before_cleanup = evidence;
          await persist("transport-evidence-before-cleanup", evidence);
        } catch (error) {
          receipt.transport_evidence_before_cleanup_error = String(error);
          receipt.passed = false;
          receipt.execution = { status: "failed", detail: "transport evidence before cleanup failed" };
          failure ??= error;
          await persist("transport-evidence-before-cleanup-failed", { error: String(error) }).catch(() => undefined);
        }
      }
      await persist("cleanup-before", { participants: participants.map(identity), retired: [...retired] }).catch(() => undefined);
      const actions: readonly Readonly<{ name: string; run: () => Promise<void> }>[] = [
        ...[...participants].reverse().map((participant) => ({ name: `${participant.identity.agent}.close`, run: () => participant.close() })),
        { name: "generic-factory.close", run: async () => { await genericSet?.close(); } },
        { name: "pi-factory.close", run: async () => { await piSet?.close(); } },
        { name: "codex-factory.close", run: async () => { await codexSet?.close(); } },
        { name: "extended-codex-factory.close", run: async () => { await extendedCodexSet?.close(); } },
        { name: "redis.close", run: async () => { await redis?.close(); } },
        { name: "workspace.remove", run: async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); } },
      ];
      for (const action of actions) {
        try {
          await action.run();
          cleanup.push({ name: action.name, status: "fulfilled" });
          if (plan.transportEvidence && action.name === "generic-factory.close") {
            try {
              if (!genericSet) throw new Error("transport evidence factory is unavailable");
              const evidence = requireTransportEvidence(genericSet, "after");
              if (!transportBefore || !sameTransportIdentity(transportBefore, evidence)) throw new Error("transport identity changed between before and after snapshots");
              receipt.transport_evidence_after_close = evidence;
              await persist("transport-evidence-after-close", evidence);
            } catch (error) {
              receipt.transport_evidence_after_close_error = String(error);
              receipt.passed = false;
              receipt.execution = { status: "failed", detail: "transport evidence after close failed" };
              failure ??= error;
              await persist("transport-evidence-after-close-failed", { error: String(error) }).catch(() => undefined);
            }
          }
        } catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      if (cleanup.some((item) => item.status === "rejected")) { receipt.passed = false; receipt.execution = { status: "failed", detail: "ordered cleanup failed" }; failure ??= new Error("ordered cleanup failed"); }
      receipt.pi_child_profile_restored = !cleanup.some((item) => item.status === "rejected");
      if (receipt.pi_child_profile_restored === true) {
        releaseChildProfile?.();
        releaseChildProfile = undefined;
      }
      const after = await sourceHashes(plan.sourceFiles); receipt.source_hashes_after = after; receipt.source_hashes_match = JSON.stringify(before) === JSON.stringify(after);
      if (!receipt.source_hashes_match) { receipt.passed = false; receipt.execution = { status: "failed", detail: "source hashes changed during cohort pilot" }; failure ??= new Error("source hashes changed during cohort pilot"); }
      receipt.ended_at = new Date().toISOString(); await persist("cleanup-after", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
