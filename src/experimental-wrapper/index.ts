#!/usr/bin/env node

import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdir, realpath, stat, writeFile } from "node:fs/promises";
import { constants, homedir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { RedisClient } from "../mcp-server/redis-client.js";
import { startBoundBridge, type BoundBridge } from "./bridge.js";
import {
  acquireWrapperIdentityClaim,
  type WrapperIdentityClaim,
} from "./identity-claim.js";
import {
  buildCodexInvocation,
  buildPiInvocation,
  parseWrapperArgs,
  renderPiExtension,
  usage,
  type ChildInvocation,
  type CleanupMode,
} from "./config.js";

const REPOSITORY_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const CLEANUP_BUDGET_MS = 10_000;
const PRIVATE_PARENT_ENV = new Set([
  "GPTQ_SESSION_ID",
  "GPTQUEUE_HTTP_TOKEN",
  "REDIS_URL",
]);

interface ChildOutcome {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

type CleanupPhase = "bridge" | "registration" | "claim" | "redis";

interface CleanupReport {
  readonly failures: ReadonlyMap<CleanupPhase, Error>;
  readonly registrationReleased: boolean;
}

interface PiRuntimeFiles {
  readonly extensionPath: string;
  readonly adapterStateDirectory: string;
}

const asError = (error: unknown): Error =>
  error instanceof Error ? error : new Error(String(error));

const isInside = (root: string, candidate: string): boolean => {
  const rel = relative(root, candidate);
  return (
    rel === "" ||
    (!isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`))
  );
};

export const requireWorkspaceInsideRepository = async (
  workspace: string
): Promise<string> => {
  const [repository, candidate] = await Promise.all([
    realpath(REPOSITORY_ROOT),
    realpath(workspace).catch(() => null),
  ]);
  if (candidate === null) {
    throw new Error(`Workspace does not exist or is unreadable: ${workspace}`);
  }
  if (isInside(repository, candidate)) {
    const metadata = await stat(candidate).catch(() => null);
    if (metadata?.isDirectory()) return candidate;
    throw new Error(`Workspace does not exist or is not a directory: ${workspace}`);
  }
  throw new Error(
    `Experimental workspace must remain inside ${REPOSITORY_ROOT}; got ${workspace}`
  );
};

const piAdapterEntry = (): string =>
  process.env.GPTQ_PI_MCP_ADAPTER_ENTRY ||
  join(
    process.env.PI_CODING_AGENT_DIR || join(homedir(), ".pi", "agent"),
    "npm",
    "node_modules",
    "pi-mcp-adapter",
    "index.ts"
  );

const createPiExtension = async (
  workspace: string,
  agent: string
): Promise<PiRuntimeFiles> => {
  const adapter = piAdapterEntry();
  await access(adapter).catch(() => {
    throw new Error(
      `The experiment requires an already-installed pi-mcp-adapter at ${adapter}. ` +
        "Nothing was downloaded; set GPTQ_PI_MCP_ADAPTER_ENTRY to an inspected local installation."
    );
  });
  const agentKey = createHash("sha256").update(agent).digest("hex").slice(0, 16);
  const runtimeDir = join(workspace, ".gptqueue-wrapper", agentKey);
  const extensionPath = join(runtimeDir, "pi-gptqueue-extension.ts");
  await mkdir(runtimeDir, { recursive: true });
  await writeFile(extensionPath, renderPiExtension(adapter), {
    encoding: "utf8",
    mode: 0o600,
  });
  return Object.freeze({
    extensionPath,
    adapterStateDirectory: join(runtimeDir, "pi-adapter-state"),
  });
};

const inheritedChildEnvironment = (): NodeJS.ProcessEnv =>
  Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !PRIVATE_PARENT_ENV.has(name)
    )
  );

const launch = (
  invocation: ChildInvocation,
  cwd: string,
  bridge: BoundBridge,
  agent: string,
  piRuntime: PiRuntimeFiles | null
): ChildProcess =>
  spawn(invocation.command, [...invocation.args], {
    cwd,
    env: {
      ...inheritedChildEnvironment(),
      GPTQ_AGENT_NAME: agent,
      GPTQ_BRIDGE_URL: bridge.url,
      GPTQ_BRIDGE_TOKEN: bridge.bearerToken,
      ...(piRuntime
        ? { GPTQ_PI_ADAPTER_STATE_DIR: piRuntime.adapterStateDirectory }
        : {}),
    },
    stdio: "inherit",
  });

/**
 * Exit-code precedence after cleanup: a cleanup failure overrides a clean
 * child exit (with the signal's code if interrupted, else 1), and an
 * interrupt reports the signal even when the child itself exited 0.
 */
export const finalExitCode = (
  childCode: number,
  firstSignal: NodeJS.Signals | null,
  cleanupFailed: boolean
): number => {
  if (cleanupFailed) return firstSignal ? exitCodeFor({ code: null, signal: firstSignal }) : 1;
  if (firstSignal && childCode === 0) return exitCodeFor({ code: null, signal: firstSignal });
  return childCode;
};

export const exitCodeFor = (outcome: ChildOutcome): number => {
  if (outcome.code !== null) return outcome.code;
  if (outcome.signal) {
    return 128 + (constants.signals[outcome.signal] ?? 0);
  }
  return 1;
};

const childIsRunning = (child: ChildProcess | null): child is ChildProcess =>
  child !== null &&
  child.pid !== undefined &&
  child.exitCode === null &&
  child.signalCode === null;

const waitForChildExit = (
  child: ChildProcess,
  timeoutMilliseconds: number
): Promise<boolean> =>
  new Promise((resolveWait) => {
    if (!childIsRunning(child)) {
      resolveWait(true);
      return;
    }
    const finish = (exited: boolean): void => {
      clearTimeout(timer);
      child.off("exit", onExit);
      resolveWait(exited);
    };
    const onExit = (): void => finish(true);
    const timer = setTimeout(() => finish(false), timeoutMilliseconds);
    timer.unref();
    child.once("exit", onExit);
  });

const terminateChild = async (child: ChildProcess | null): Promise<void> => {
  if (!childIsRunning(child)) return;
  child.kill("SIGTERM");
  if (await waitForChildExit(child, 5_000)) return;
  child.kill("SIGKILL");
  await waitForChildExit(child, 1_000);
};

const observeChild = (child: ChildProcess): Promise<ChildOutcome> =>
  new Promise((resolveOutcome, reject) => {
    let spawned = false;
    child.once("spawn", () => {
      spawned = true;
    });
    child.on("error", (error) => {
      if (!spawned) {
        reject(error);
        return;
      }
      console.error(
        `[gptqueue-experiment] child process error after startup: ${asError(error).message}`
      );
    });
    child.once("exit", (code, signal) =>
      resolveOutcome(Object.freeze({ code, signal }))
    );
  });

const releaseRegistration = async (
  client: RedisClient,
  cleanup: CleanupMode,
  identityClaim: WrapperIdentityClaim | null
): Promise<Error | null> => {
  if (!client.registered) return null;
  if (cleanup === "unregister") {
    const sessionId = client.sessionId;
    if (!identityClaim || !sessionId) {
      await client.closeCurrentSession();
      throw new Error(
        "Destructive cleanup skipped because exclusive session ownership was unavailable."
      );
    }
    // Stop this process from recreating its heartbeat after the atomic delete.
    // The claim connection remains live long enough to perform the Lua step.
    client.forceDisconnect();
    const outcome = await identityClaim.unregisterExclusiveSession(sessionId);
    if (outcome === "session_closed_only") {
      return new Error(
        `Destructive cleanup skipped because agent ${client.agentName} gained another active session.`
      );
    }
    return null;
  }
  await client.closeCurrentSession();
  return null;
};

export async function runExperimentalWrapper(
  argv: readonly string[] = process.argv.slice(2)
): Promise<number> {
  const parsedOptions = parseWrapperArgs(argv);
  const workspace = await requireWorkspaceInsideRepository(
    parsedOptions.workspace
  );
  const options = Object.freeze({ ...parsedOptions, workspace });

  const redisClient = new RedisClient(null, options.redisUrl);
  let identityClaim: WrapperIdentityClaim | null = null;
  let bridge: BoundBridge | null = null;
  let child: ChildProcess | null = null;
  let registered = false;
  // Cleanup phases, in the order their ownership nests: the bridge serves the
  // registration, which the identity claim protects, all over one Redis
  // client. Ordering lives in this list rather than in guard flags.
  const released = new Set<CleanupPhase>();
  let cleanupInFlight: Promise<CleanupReport> | null = null;
  const cleanupFailures = new Map<CleanupPhase, Error>();
  let cleanupForced = false;

  const forceCleanupConnections = (): void => {
    if (cleanupForced) return;
    cleanupForced = true;
    identityClaim?.abandon();
    redisClient.forceDisconnect();
    released.add("redis");
  };

  interface CleanupStep {
    readonly phase: CleanupPhase;
    /** Whether this step may run yet on this attempt. */
    readonly ready: (finalAttempt: boolean) => boolean;
    /** True when there is nothing to release (counts as released). */
    readonly absent: () => boolean;
    /** Failure recorded instead of running once cleanup has been forced. */
    readonly abandonedMessage?: string;
    /** Release the resource; a returned Error is a non-fatal warning. */
    readonly release: () => Promise<Error | null | undefined | void>;
  }
  const steps: readonly CleanupStep[] = [
    {
      phase: "bridge",
      ready: () => true,
      absent: () => !bridge,
      release: () => bridge!.close(),
    },
    {
      phase: "registration",
      ready: () => true,
      absent: () => !redisClient.registered,
      abandonedMessage: "Registration cleanup was abandoned after its deadline.",
      release: () => releaseRegistration(redisClient, options.cleanup, identityClaim),
    },
    {
      phase: "claim",
      ready: (finalAttempt) => released.has("registration") || finalAttempt,
      absent: () => !identityClaim,
      abandonedMessage: "Identity claim remains fail-closed after cleanup timeout.",
      release: () => identityClaim!.release(),
    },
    {
      phase: "redis",
      ready: (finalAttempt) => (released.has("registration") && released.has("claim")) || finalAttempt,
      absent: () => false,
      release: () => redisClient.shutdown(),
    },
  ];

  const cleanup = (finalAttempt: boolean): Promise<CleanupReport> => {
    if (cleanupInFlight) return cleanupInFlight;
    cleanupInFlight = (async () => {
      for (const step of steps) {
        if (released.has(step.phase) || !step.ready(finalAttempt)) continue;
        if (step.absent()) {
          released.add(step.phase);
        } else if (step.abandonedMessage && cleanupForced) {
          cleanupFailures.set(step.phase, new Error(step.abandonedMessage));
        } else {
          try {
            const warning = await step.release();
            released.add(step.phase);
            if (warning instanceof Error) cleanupFailures.set(step.phase, warning);
            else cleanupFailures.delete(step.phase);
          } catch (error) {
            cleanupFailures.set(step.phase, asError(error));
          }
        }
      }
      return Object.freeze({
        failures: new Map(cleanupFailures),
        registrationReleased: released.has("registration"),
      });
    })().finally(() => {
      cleanupInFlight = null;
    });
    return cleanupInFlight;
  };

  let signalCount = 0;
  let firstSignal: NodeJS.Signals | null = null;
  let escalationTimer: ReturnType<typeof setTimeout> | null = null;
  const forwardSignal = (signal: NodeJS.Signals): void => {
    signalCount += 1;
    firstSignal ??= signal;
    if (!childIsRunning(child)) {
      if (signalCount >= 2) forceCleanupConnections();
      return;
    }
    child.kill(signalCount === 1 ? signal : "SIGKILL");
    if (signalCount === 1) {
      escalationTimer = setTimeout(() => {
        if (childIsRunning(child)) child.kill("SIGKILL");
      }, 5_000);
      escalationTimer.unref();
    }
  };
  const onInterrupt = () => forwardSignal("SIGINT");
  const onTerminate = () => forwardSignal("SIGTERM");
  process.on("SIGINT", onInterrupt);
  process.on("SIGTERM", onTerminate);

  let resultCode = 1;

  try {
    identityClaim = await acquireWrapperIdentityClaim(
      options.redisUrl,
      options.agent,
      options.cleanup === "unregister"
    );
    await redisClient.register("both", options.agent, options.description);
    registered = true;
    if (!redisClient.sessionId) {
      throw new Error("Registration returned without a bound GPTQueue session.");
    }
    await identityClaim.assertExclusiveSession(redisClient.sessionId);
    console.error(
      `[gptqueue-experiment] registered "${options.agent}" before ${options.client} startup (Redis db isolated)`
    );

    bridge = await startBoundBridge({
      agentName: options.agent,
      redisClient,
    });
    console.error(
      `[gptqueue-experiment] authenticated loopback bridge ready; launching ${options.client}`
    );

    if (firstSignal) {
      resultCode = exitCodeFor({ code: null, signal: firstSignal });
    } else {
      const piRuntime =
        options.client === "pi"
          ? await createPiExtension(options.workspace, options.agent)
          : null;
      const invocation = piRuntime
        ? buildPiInvocation(options, piRuntime.extensionPath)
        : buildCodexInvocation(options, bridge.url);
      // Re-resolve immediately before spawn so a swapped symlink cannot turn
      // the earlier canonical workspace into an out-of-repository child cwd.
      const launchWorkspace = await requireWorkspaceInsideRepository(
        options.workspace
      );
      if (firstSignal) {
        resultCode = exitCodeFor({ code: null, signal: firstSignal });
      } else {
        child = launch(
          invocation,
          launchWorkspace,
          bridge,
          options.agent,
          piRuntime
        );

        const outcome = await observeChild(child);
        resultCode = exitCodeFor(outcome);
      }
    }
  } finally {
    if (escalationTimer) clearTimeout(escalationTimer);
    try {
      await terminateChild(child);
      const cleanupDeadline = setTimeout(() => {
        console.error(
          `[gptqueue-experiment] cleanup exceeded ${CLEANUP_BUDGET_MS}ms; forcing Redis connections closed`
        );
        forceCleanupConnections();
      }, CLEANUP_BUDGET_MS);
      cleanupDeadline.unref();
      try {
        let report = await cleanup(false);
        if (report.failures.size > 0) report = await cleanup(true);
        for (const [phase, error] of report.failures) {
          console.error(
            `[gptqueue-experiment] cleanup ${phase} failed: ${error.message}`
          );
        }
        const childCode = resultCode;
        resultCode = finalExitCode(childCode, firstSignal, report.failures.size > 0);
        if (report.failures.size === 0 && !(firstSignal && childCode === 0) && registered && report.registrationReleased) {
          console.error(
            `[gptqueue-experiment] ${options.cleanup === "unregister" ? "unregistered" : "closed"} "${options.agent}" after ${options.client} exit`
          );
        }
      } finally {
        clearTimeout(cleanupDeadline);
        // Fast failures can finish both attempts before the deadline fires.
        // Close owned sockets even when a lost claim prevents safe deletion.
        identityClaim?.abandon();
        redisClient.forceDisconnect();
      }
    } finally {
      process.off("SIGINT", onInterrupt);
      process.off("SIGTERM", onTerminate);
    }
  }
  return resultCode;
}

export const runExperimentalWrapperCli = (): void => {
  runExperimentalWrapper()
    .then((code) => {
      process.exitCode = code;
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[gptqueue-experiment] ${message}`);
      console.error(usage());
      process.exitCode = 1;
    });
};
