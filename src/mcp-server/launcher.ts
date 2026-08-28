/**
 * Launcher: pure process-dispatch adapter for a durable actor's runtime launch
 * contract.
 *
 * This module holds the ONLY child_process.spawn call that GPTQueue makes when
 * waking an offline durable actor. It is deliberately isolated so that launch
 * policy is a thin, side-effect-free boundary:
 *   - It NEVER uses a shell (`shell: true` is forbidden) and never interpolates
 *     the command into a string.
 *   - It performs no logging and touches no Redis.
 *   - The child is spawned detached and unref'd so it outlives this server
 *     process.
 *
 * A contract that cannot be spawned (e.g. a nonexistent binary) resolves to a
 * `launch_failed` outcome rather than throwing. The caller decides what to do
 * with that outcome; the wake lease TTL independently expires an activation
 * that never came up.
 */

import { spawn } from "child_process";
import type { RuntimeLaunchContract } from "../core/actor-directory.js";

export interface LaunchOutcome {
  readonly dispatched: boolean;
  readonly pid?: number;
  readonly error?: { readonly code: "launch_failed"; readonly message: string };
}

/**
 * Dispatch a runtime launch from its contract. Resolves on the child's
 * "spawn" event with the pid, or on "error" with a `launch_failed` outcome.
 * Resolves on the first event and never throws.
 */
export const dispatchLaunch = async (
  contract: RuntimeLaunchContract
): Promise<LaunchOutcome> => {
  return new Promise<LaunchOutcome>((resolve) => {
    let child;
    try {
      // NO shell: command and args are passed verbatim and never interpolated.
      child = spawn(contract.command, [...contract.args], {
        cwd: contract.cwd ?? undefined,
        detached: true,
        stdio: "ignore",
      });
    } catch (error) {
      resolve({
        dispatched: false,
        error: {
          code: "launch_failed",
          message: error instanceof Error ? error.message : String(error),
        },
      });
      return;
    }

    let settled = false;
    const settle = (outcome: LaunchOutcome): void => {
      if (settled) return;
      settled = true;
      resolve(outcome);
    };

    child.once("spawn", () => {
      // Detach the child so an activated runtime outlives this server process.
      child.unref();
      settle({ dispatched: true, pid: child.pid });
    });
    child.once("error", (error: Error) => {
      settle({
        dispatched: false,
        error: { code: "launch_failed", message: error.message },
      });
    });
  });
};
