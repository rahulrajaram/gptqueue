import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import type { RuntimeLaunchContract } from "../../core/actor-directory.js";
import { bindSession } from "./session-binding.js";
import { actorRegisterResult } from "./actor-result.js";

export const actorRegisterSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  alias: z
    .string()
    .min(1)
    .describe("Human-facing address such as `metabuilder`"),
  activation_policy_mode: z
    .enum(["wake_if_offline", "store_only"])
    .describe(
      "Whether this actor may be woken when offline. wake_if_offline requires a launch_command."
    ),
  max_concurrency: z
    .number()
    .int()
    .min(1)
    .describe("Maximum simultaneous runtime limit (an integer of at least 1)"),
  launch_command: z
    .string()
    .optional()
    .describe(
      "Executable path/name used to launch the actor's runtime. Spawned WITHOUT a shell. Omit for store_only actors."
    ),
  launch_args: z
    .array(z.string())
    .optional()
    .describe("Arguments passed verbatim to launch_command"),
  launch_cwd: z
    .string()
    .optional()
    .describe("Working directory for the launched runtime"),
  capabilities: z
    .array(z.string())
    .optional()
    .describe("Structured routing claims; opaque descriptive strings"),
});

/**
 * Assemble a DurableActorProfile from the tool's sparse inputs. Fields the
 * profile requires but the tool does not collect are derived deterministically
 * from the registration context per the product thesis defaults:
 * `workspace_root` = the registration (server) directory, `working_directory`
 * = workspace_root, and `runtime` = the launch command (the registered
 * adapter), or "manual" for store_only actors with no managed launch.
 *
 * Note: the legacy `state_directory` field was dropped (review M2) — it was
 * only set here and never consumed downstream, so the template is removed
 * rather than confined.
 */
function buildProfileInput(
  actorId: string,
  params: z.infer<typeof actorRegisterSchema>
): unknown {
  const workspaceRoot = process.cwd();
  const launchCommand = params.launch_command?.trim() || "";
  return {
    actor_id: actorId,
    alias: params.alias,
    capabilities: params.capabilities ?? [],
    workspace_root: workspaceRoot,
    working_directory: workspaceRoot,
    runtime: launchCommand.length > 0 ? launchCommand : "manual",
    activation_policy: { mode: params.activation_policy_mode },
    max_concurrency: params.max_concurrency,
  };
}

/** Assemble a launch contract, or null when no command was provided. */
function buildLaunch(
  params: z.infer<typeof actorRegisterSchema>
): RuntimeLaunchContract | null {
  const command = params.launch_command?.trim();
  if (command === undefined || command.length === 0) return null;
  return {
    command,
    args: params.launch_args ?? [],
    ...(params.launch_cwd === undefined ? {} : { cwd: params.launch_cwd }),
  };
}

export async function actorRegister(
  client: RedisClient,
  params: z.infer<typeof actorRegisterSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(
    client,
    params.session_id
  );

  // H4 (identity discipline): the durable actor identity IS the calling
  // session's registered agent name. actor_id is derived, never caller-supplied,
  // so registry name, directory key, wake/presence key, and delivery/claim
  // identity can never diverge.
  const result = await client.actorDirectory.register({
    profile_input: buildProfileInput(actor_id, params),
    launch: buildLaunch(params),
    registered_by: session_id,
    registered_at: new Date().toISOString(),
  });

  return actorRegisterResult(result, { actor_id });
}