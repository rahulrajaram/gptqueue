import { afterEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { createRuntimeController } from "../src/registered-shell/runtime-controller.js";
import type { RuntimeAdapter, RuntimeBinding } from "../src/registered-shell/runtime.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const WORKDIR = "/tmp/runtime-controller-test";

/** Drives the bind_runtime controller state machine end to end against Redis. */
describe("runtime controller (bind_runtime)", () => {
  const cleanup: Array<() => Promise<void>> = [];
  afterEach(async () => { for (const step of cleanup.splice(0).reverse()) await step().catch(() => undefined); });

  const setup = async () => {
    runtimeId = `rt-${randomUUID()}`;
    otherRuntimeId = `rt-${randomUUID()}`;
    const mappings = [SESSION_KEYS.runtimeMailbox("codex", runtimeId), SESSION_KEYS.runtimeMailbox("codex", otherRuntimeId)];
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", `rc-${randomUUID()}`);
    cleanup.push(async () => { await client.adapterConnection.del(...mappings); await client.unregister(); await client.shutdown(); });
    const adapters: Array<{ binding: RuntimeBinding; closed: boolean }> = [];
    const controller = createRuntimeController(client, { client: "codex", working_directory: WORKDIR }, async (binding) => {
      const record = { binding, closed: false };
      adapters.push(record);
      const adapter: RuntimeAdapter = {
        binding,
        activate: async () => ({ status: "queued" }),
        close: async () => { record.closed = true; },
      };
      return adapter;
    });
    cleanup.push(() => controller.close());
    return { client, controller, adapters };
  };
  // Unique runtime ids per test: a binding records a runtime->agent mapping that outlives the test.
  let runtimeId = "";
  let otherRuntimeId = "";
  const binding = (overrides: Partial<RuntimeBinding> = {}): RuntimeBinding =>
    ({ client: "codex", runtime_id: runtimeId, epoch: "e1", working_directory: WORKDIR, ...overrides });

  it("starts unbound, binds once, and is idempotent for the same epoch", async () => {
    const { controller, adapters } = await setup();
    expect(controller.status()).toMatchObject({ activation_ready: false, runtime: null });
    expect(await controller.bind(binding())).toMatchObject({ activation_ready: true, runtime: binding() });
    expect(await controller.bind(binding())).toMatchObject({ activation_ready: true });
    expect(adapters).toHaveLength(1);
  });

  it("replaces the adapter on a new epoch of the same runtime", async () => {
    const { controller, adapters } = await setup();
    await controller.bind(binding());
    expect(await controller.bind(binding({ epoch: "e2" }))).toMatchObject({ runtime: { epoch: "e2" } });
    expect(adapters).toHaveLength(2);
  });

  it("refuses a different runtime, a mismatched or malformed binding, and binds after close", async () => {
    const { controller } = await setup();
    await controller.bind(binding());
    expect(await controller.bind(binding({ runtime_id: otherRuntimeId }))).toEqual({ status: "error", code: "runtime_rebind_requires_new_connection" });
    expect(await controller.bind(binding({ working_directory: "/elsewhere" }))).toEqual({ status: "error", code: "runtime_binding_mismatch" });
    expect(await controller.bind({ client: "codex" })).toEqual({ status: "error", code: "invalid_runtime_binding" });
    await controller.close();
    expect(await controller.bind(binding({ epoch: "e3" }))).toEqual({ status: "error", code: "runtime_controller_closed" });
  });
});
