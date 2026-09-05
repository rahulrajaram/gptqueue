import type { RedisClient } from "../mcp-server/redis-client.js";
import { startInboxDispatcher, type InboxDispatcher } from "./inbox-dispatcher.js";
import { validateRuntimeBinding, type RuntimeAdapter, type RuntimeBinding } from "./runtime.js";
import type { RuntimeTools } from "./runtime-tools.js";
import { restoreRuntimeMailbox } from "./runtime-mailbox.js";

export const createRuntimeController = (
  client: RedisClient,
  expected: Readonly<{ client: "codex" | "pi"; working_directory: string }>,
  createAdapter: (binding: RuntimeBinding) => Promise<RuntimeAdapter>,
): RuntimeTools & { close(): Promise<void> } => {
  let binding: RuntimeBinding | undefined;
  let dispatcher: InboxDispatcher | undefined;
  let closed = false;
  let operation: Promise<unknown> = Promise.resolve();
  const status = (): Record<string, unknown> => ({
    status: "ok", agent: client.agentName, activation_ready: !!dispatcher && !closed,
    runtime: binding ?? null,
  });
  return {
    status,
    bind: (value) => {
      const work = operation.then(async () => {
        if (closed) return { status: "error", code: "runtime_controller_closed" };
        const validation = validateRuntimeBinding(value, expected);
        if (!validation.ok) return { status: "error", code: validation.code };
        // A sidecar belongs to one conversation. A new conversation gets its own connection.
        if (binding && binding.runtime_id !== validation.binding.runtime_id) {
          return { status: "error", code: "runtime_rebind_requires_new_connection" };
        }
        if (dispatcher && binding?.epoch === validation.binding.epoch) return status();
        const adapter = await createAdapter(validation.binding);
        if (closed) { await adapter.close(); return { status: "error", code: "runtime_controller_closed" }; }
        await dispatcher?.close();
        binding = validation.binding;
        try {
          await restoreRuntimeMailbox(client, binding);
          dispatcher = await startInboxDispatcher(client.adapterConnection, client.requireRegistered(), adapter);
          const active = dispatcher;
          void active.closed.finally(() => { if (dispatcher === active) dispatcher = undefined; });
          return status();
        } catch (error) { await adapter.close(); throw error; }
      });
      operation = work.catch(() => undefined);
      return work;
    },
    close: async () => {
      closed = true;
      await operation;
      await dispatcher?.close();
      dispatcher = undefined;
    },
  };
};
