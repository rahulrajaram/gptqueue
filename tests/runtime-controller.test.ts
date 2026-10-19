import { testRedisUrl } from "./helpers/test-redis.js";
import { afterEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { createRuntimeController } from "../src/registered-shell/runtime-controller.js";
import type { RuntimeAdapter, RuntimeBinding } from "../src/registered-shell/runtime.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";

const TEST_REDIS_URL = testRedisUrl();
const WORKDIR = "/tmp/runtime-controller-test";

/** Drives the bind_runtime controller state machine end to end against Redis. */
describe("runtime controller (bind_runtime)", () => {
  const cleanup: Array<() => Promise<unknown>> = [];
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

  it("resumes an offline mailbox with expired claims and preserves their recovery", async () => {
    const { client: original, controller: previous } = await setup();
    const nativeBinding = binding();
    const canonical = original.agentName!;
    await previous.bind(nativeBinding);
    await previous.close();
    const envelope = {
      id: randomUUID(), from: "synthetic-peer", to: canonical,
      timestamp: new Date().toISOString(), type: "task" as const,
      payload: { content: "unfinished task", in_reply_to: randomUUID() },
    };
    await original.sendMessage(envelope);
    // The claim is backdated so it is already expired: a simulated clock.
    const claims = new TaskClaimStore(original.adapterConnection, { clock: "caller" });
    const claimed = await claims.claim({
      actor_id: canonical, session_id: original.sessionId!, max_batch: 1,
      ttl_seconds: 1, now: new Date(Date.now() - 60_000).toISOString(),
    });
    expect(claimed.ok && claimed.claim?.tasks).toEqual([JSON.stringify(envelope)]);
    await original.closeCurrentSession();

    const resumed = new RedisClient(null, TEST_REDIS_URL);
    await resumed.register("both", `rc-resumed-${randomUUID()}`);
    cleanup.push(async () => { await resumed.unregister(); await resumed.shutdown(); });
    const current = createRuntimeController(resumed,
      { client: "codex", working_directory: WORKDIR }, async (identity) => ({
        binding: identity, activate: async () => ({ status: "queued" }),
        close: async () => undefined,
      }));
    cleanup.push(() => current.close());
    expect(await current.bind(nativeBinding)).toMatchObject({
      agent: canonical, activation_ready: true, runtime: nativeBinding,
    });
    expect(resumed.agentName).toBe(canonical);
    const recovered = await new TaskClaimStore(resumed.adapterConnection).claim({
      actor_id: canonical, session_id: resumed.sessionId!, max_batch: 1,
      ttl_seconds: 60, now: new Date().toISOString(),
    });
    expect(recovered.ok && recovered.claim?.tasks).toEqual([JSON.stringify(envelope)]);
  });

  it("refuses a different runtime, a mismatched or malformed binding, and binds after close", async () => {
    const { controller } = await setup();
    await controller.bind(binding());
    expect(await controller.bind(binding({ runtime_id: otherRuntimeId }))).toEqual({ status: "error", code: "runtime_rebind_requires_new_connection" });
    expect(await controller.bind(binding({ working_directory: "/elsewhere" }))).toEqual({ status: "error", code: "runtime_binding_mismatch" });
    expect(await controller.bind({ client: "codex" } as unknown as Parameters<typeof controller.bind>[0])).toEqual({ status: "error", code: "invalid_runtime_binding" });
    await controller.close();
    expect(await controller.bind(binding({ epoch: "e3" }))).toEqual({ status: "error", code: "runtime_controller_closed" });
  });

  it("refuses resume takeover while the old owner is live, and allows it once the old thread is closed", async () => {
    const { client: original } = await setup();
    const canonical = original.agentName!;
    const nativeBinding = binding();
    let fireStale: () => void = () => undefined;
    const retired: RuntimeBinding[] = [];
    const previous = createRuntimeController(original, { client: "codex", working_directory: WORKDIR }, async (identity) => ({
      binding: identity, activate: async () => ({ status: "busy" }), close: async () => undefined,
      onStale: (listener: () => void) => { fireStale = listener; return () => undefined; },
      describe: () => ({ host_state: "notLoaded", attached: false }),
    }), (stale) => { retired.push(stale); void original.closeCurrentSession(); });
    cleanup.push(() => previous.close());
    expect(await previous.bind(nativeBinding)).toMatchObject({ agent: canonical, activation_ready: true,
      runtime_host: { host_state: "notLoaded", attached: false } });

    const resumed = new RedisClient(null, TEST_REDIS_URL);
    await resumed.register("both", `rc-takeover-${randomUUID()}`);
    cleanup.push(async () => { await resumed.unregister(); await resumed.shutdown(); });
    const adapter = async (identity: RuntimeBinding): Promise<RuntimeAdapter> =>
      ({ binding: identity, activate: async () => ({ status: "queued" }), close: async () => undefined });
    const refused = createRuntimeController(resumed, { client: "codex", working_directory: WORKDIR }, adapter);
    cleanup.push(() => refused.close());
    await expect(refused.bind(nativeBinding)).rejects.toThrow();
    expect(resumed.agentName).not.toBe(canonical);

    fireStale();
    await expect.poll(() => retired.length).toBe(1);
    await expect.poll(async () => (await original.adapterConnection.exists(SESSION_KEYS.runtimeBinding(canonical)))).toBe(0);
    expect(previous.status()).toMatchObject({ activation_ready: false });
    expect(previous.status()).not.toHaveProperty("runtime_host");

    const takeover = createRuntimeController(resumed, { client: "codex", working_directory: WORKDIR }, adapter);
    cleanup.push(() => takeover.close());
    await expect.poll(async () => (await takeover.bind(nativeBinding).catch(() => ({ agent: undefined }))) .agent, { timeout: 5_000 }).toBe(canonical);
  });
});

