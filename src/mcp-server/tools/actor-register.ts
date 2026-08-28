import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import type { RuntimeLaunchContract } from "../../core/actor-directory.js";
import { ensureSessionBinding } from "./session-binding.js";
import { actorRegisterResult } from "./actor-result.js";

export const actorRegisterSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  actor_id: z
    .string()
    .min(1)
    .describe(
      "Stable durable actor identity. The session that registers an actor owns its profile."
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
 * = workspace_root, `state_directory` = a GPTQueue-managed actor path, and
 * `runtime` = the launch command (the registered adapter), or "manual" for
 * store_only actors with no managed launch.
 */
function buildProfileInput(params: z.infer<typeof actorRegisterSchema>): unknown {
  const workspaceRoot = process.cwd();
  const launchCommand = params.launch_command?.trim() || "";
  return {
    actor_id: params.actor_id,
    alias: params.alias,
    capabilities: params.capabilities ?? [],
    workspace_root: workspaceRoot,
    working_directory: workspaceRoot,
    state_directory: `${workspaceRoot}/.gptq/actors/${params.actor_id}/state`,
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
  await ensureSessionBinding(client, params.session_id);
  client.requireRegistered();
  const session_id = client.sessionId;
  if (!session_id) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const result = await client.actorDirectory.register({
    profile_input: buildProfileInput(params),
    launch: buildLaunch(params),
    registered_by: session_id,
    registered_at: new Date().toISOString(),
  });

  return actorRegisterResult(result, { actor_id: params.actor_id });
}