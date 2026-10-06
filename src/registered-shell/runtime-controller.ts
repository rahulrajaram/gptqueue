import type { RedisClient } from "../mcp-server/redis-client.js";
import { startInboxDispatcher, type InboxDispatcher } from "./inbox-dispatcher.js";
import { subscribeStale, validateRuntimeBinding, type RuntimeAdapter, type RuntimeBinding } from "./runtime.js";
import type { RuntimeTools } from "./runtime-tools.js";
import { restoreRuntimeMailbox, RuntimeMailboxAdoptionRefused } from "./runtime-mailbox.js";

export const createRuntimeController = (
  client: RedisClient,
  expected: Readonly<{ client: "codex" | "pi"; working_directory: string }>,
  createAdapter: (binding: RuntimeBinding) => Promise<RuntimeAdapter>,
  /** Called once the bound runtime reports it was closed by its host; the owner should retire the session. */
  onRuntimeStale: (binding: RuntimeBinding) => void = () => undefined,
): RuntimeTools & { close(): Promise<void> } => {
  let binding: RuntimeBinding | undefined;
  let host: RuntimeAdapter | undefined;
  let dispatcher: InboxDispatcher | undefined;
  let closed = false;
  // DL-2: the bound mailbox a refused adoption targeted. While this client
  // still holds its provisional name, closing retires that name into it.
  let refusedTarget: string | undefined;
  let operation: Promise<unknown> = Promise.resolve();
  const status = (): Record<string, unknown> => ({
    status: "ok", agent: client.agentName, activation_ready: !!dispatcher && !closed,
    runtime: binding ?? null,
    ...(host?.describe && dispatcher && !closed ? { runtime_host: host.describe() } : {}),
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
          try {
            await restoreRuntimeMailbox(client, binding);
            refusedTarget = undefined;
          } catch (error) {
            if (error instanceof RuntimeMailboxAdoptionRefused) refusedTarget = error.target;
            throw error;
          }
          dispatcher = await startInboxDispatcher(client.adapterConnection, client.requireRegistered(), adapter);
          const active = dispatcher;
          host = adapter;
          void active.closed.finally(() => { if (dispatcher === active) dispatcher = undefined; });
          const bound = binding;
          // A closed host thread must not keep looking activation-ready, and must
          // release its dispatcher lease so a resumed session can take over.
          subscribeStale(adapter, () => {
            operation = operation.then(async () => {
              if (dispatcher !== active || binding !== bound) return;
              dispatcher = undefined;
              await active.close().catch(() => undefined);
              onRuntimeStale(bound);
            }).catch(() => undefined);
          });
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
      const provisional = client.agentName;
      if (refusedTarget && provisional && provisional !== refusedTarget) {
        // Hand anything a peer sent to the provisional name to the bound
        // mailbox and stop the name being discoverable, in one atomic step.
        await client.retireProvisionalMailbox(provisional, refusedTarget).catch(() => false);
      }
    },
  };
};
