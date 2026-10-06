/**
 * ID-1 / ID-5: two sessions of one runtime in one directory get distinct
 * mailboxes, and a live name cannot be silently taken.
 */
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { afterEach, describe, expect, it } from "vitest";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import { registerClaudeAware } from "../../src/claude/mcp-record.js";
import { shellIdentity, startRegisteredShell } from "../../src/registered-shell/server.js";
import { createOpenCodePlugin, type OpenCodeNativeClient } from "../../src/registered-shell/opencode-plugin-factory.js";
import { opencodeAgentName } from "../../src/registered-shell/opencode-backend.js";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { REDIS_URL, startSender, waitFor } from "./harness.js";

const open: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close().catch(() => undefined); });
const client = (): RedisClient => { const c = new RedisClient(null, REDIS_URL); open.push(() => c.shutdown()); return c; };

/** Each name is registered, and mail to one never lands in the other's mailbox. */
const expectIsolatedMailboxes = async (names: readonly [string, string]) => {
  expect(names[0]).not.toBe(names[1]);
  const sender = await startSender("id"); open.push(() => sender.close());
  const [a, b] = [await sender.send(names[0], "for-a"), await sender.send(names[1], "for-b")];
  const redis = client();
  const queued = async (name: string) => (await redis.adapterConnection.lrange(SESSION_KEYS.queue(name), 0, -1)).map((raw) => JSON.parse(raw).id as string);
  expect(await queued(names[0])).toEqual([a.message_id]);
  expect(await queued(names[1])).toEqual([b.message_id]);
};

describe("ID-1/ID-5 same-directory sessions", () => {
  it("claude: a live name is refused (ID-1), and a second session falls back to a distinct name (ID-5)", async () => {
    const name = `e2e-claude-dir-${randomUUID().slice(0, 8)}`;
    const first = client(); await first.register("both", name);
    open.push(() => first.closeCurrentSession());
    const second = client();
    const savedShared = process.env.GPTQ_AGENT_SHARED; delete process.env.GPTQ_AGENT_SHARED;
    try {
      await expect(second.register("both", name)).rejects.toMatchObject({ code: expect.stringMatching(/^agent_name_held$/iu) });
      const registered = await registerClaudeAware(second, name, { GPTQ_CLAUDE_SESSION_RECORD: "1", CLAUDE_SESSION_ID: "5e55ion2-abcd" });
      open.push(() => second.closeCurrentSession());
      expect(registered.name).toBe(`${name}-5e55ion2`);
      expect(first.agentName).toBe(name);
      await expectIsolatedMailboxes([name, registered.name]);
    } finally { if (savedShared !== undefined) process.env.GPTQ_AGENT_SHARED = savedShared; }
  });

  it("pi: two registered sidecars started in one directory hold distinct mailboxes", async () => {
    const cwd = mkdtempSync(join(tmpdir(), "gptq-e2e-id-pi-"));
    const start = async () => {
      const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
      const shell = await startRegisteredShell({ client: "pi", redisUrl: REDIS_URL, cwd }, new AbortController().signal, () => serverTransport);
      const mcp = new Client({ name: "id-test", version: "0" }); await mcp.connect(clientTransport);
      open.push(async () => { await mcp.close().catch(() => undefined); await shell.close(); });
      return shell.agentName;
    };
    const names = [await start(), await start()] as const;
    await expectIsolatedMailboxes(names);
  });

  it("codex: shell identities for one directory are distinct and both register", async () => {
    // startRegisteredShell("codex") would dial the real Codex app-server socket; the
    // name comes from shellIdentity, so register those exact names instead.
    const cwd = mkdtempSync(join(tmpdir(), "gptq-e2e-id-codex-"));
    const ids = [shellIdentity("codex", cwd), shellIdentity("codex", cwd)];
    for (const id of ids) { const c = client(); await c.register("both", id.name, "codex shell", id); open.push(() => c.closeCurrentSession()); }
    await expectIsolatedMailboxes([ids[0]!.name, ids[1]!.name]);
  });

  it("opencode: two native sessions in one directory are bound to distinct mailboxes", async () => {
    const directory = mkdtempSync(join(tmpdir(), "gptq-e2e-id-oc-"));
    const sessions = [`ses_${randomUUID().replace(/-/gu, "")}`, `ses_${randomUUID().replace(/-/gu, "")}`];
    const sdk: OpenCodeNativeClient = { session: {
      get: async (options: { path: { id: string } }) => ({ id: options.path.id, directory }),
      status: async () => ({}), messages: async () => [], promptAsync: async () => undefined, list: async () => [],
    } };
    const plugin = createOpenCodePlugin({ client: sdk, directory }, { redisUrl: REDIS_URL, bootstrapLimit: 0 });
    open.push(() => plugin.dispose());
    for (const id of sessions) await plugin.event({ event: { type: "session.created", properties: { info: { id, directory } } } });
    const names = [opencodeAgentName(sessions[0]!), opencodeAgentName(sessions[1]!)] as const;
    const redis = client();
    await waitFor(async () => (await Promise.all(names.map((n) => redis.adapterConnection.hexists(SESSION_KEYS.registry, n)))).every(Boolean), 5_000, "both registered");
    await expectIsolatedMailboxes(names);
  });
});
