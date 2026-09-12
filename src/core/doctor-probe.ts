/**
 * D7: the doctor CLI's connection probe must gate `activation_ready` with
 * the SAME identity/binding conjunction as the MCP probe
 * (src/registered-shell/diagnostic-tools.ts): no error, the probed runtime
 * must self-report the expected agent identity, and — when a validated
 * runtime binding is known — must agree with it on runtime_id and epoch.
 * Previously the doctor reported `activation_ready: true` from
 * `status.activation_ready === true` alone, so the same probe response
 * yielded true from the doctor and false from the MCP tool.
 */

/** Minimal shape of the runtime's get_runtime_status structuredContent. */
export interface RuntimeStatusLike {
  agent?: unknown;
  activation_ready?: unknown;
  runtime?: { runtime_id?: unknown; epoch?: unknown };
}

/** The operator-side binding evidence, when one is recorded for the agent. */
export interface KnownBinding {
  runtime_id: string;
  epoch: string;
}

/**
 * Whether the probe proves activation readiness under the full conjunction.
 * `expectedAgent` gates only when supplied (the doctor's `--agent`); an
 * unknown binding (`null`) skips binding agreement, mirroring the doctor's
 * best-effort evidence read.
 */
export const probeActivationReady = (
  isError: boolean,
  status: RuntimeStatusLike | undefined,
  expectedAgent: string | undefined,
  binding: KnownBinding | null
): boolean => {
  if (isError || status?.activation_ready !== true) return false;
  if (typeof status.agent !== "string" || status.agent.length === 0) {
    return false;
  }
  if (expectedAgent !== undefined && status.agent !== expectedAgent) {
    return false;
  }
  if (
    binding &&
    (status.runtime?.runtime_id !== binding.runtime_id ||
      status.runtime?.epoch !== binding.epoch)
  ) {
    return false;
  }
  return true;
};

/**
 * Whether the probed runtime's identity agrees with the recorded binding.
 * `null` when no binding is known (agreement unverifiable, not disagreed).
 */
export const bindingAgrees = (
  status: RuntimeStatusLike | undefined,
  binding: KnownBinding | null
): boolean | null => {
  if (!binding) return null;
  return (
    status?.runtime?.runtime_id === binding.runtime_id &&
    status?.runtime?.epoch === binding.epoch
  );
};
