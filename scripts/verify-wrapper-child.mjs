#!/usr/bin/env node
// Observation shim used only by verify-registered-wrapper.mjs.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFile, writeFile, access } from 'node:fs/promises';
import { constants } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { WRAPPER_VISIBLE_TOOLS } from '../dist/experimental-wrapper/bridge.js';
import { SESSION_KEYS } from '../dist/core/keys.js';

const kind = process.env.GPTQ_VERIFY_KIND;
assert(['codex', 'pi'].includes(kind));
const run = process.env.GPTQ_VERIFY_RUN;
const name = process.env.GPTQ_AGENT_NAME;
assert(run && name?.startsWith('gptqueue-experiment-'));
const privateNames = ['GPTQ_SESSION_ID', 'GPTQUEUE_HTTP_TOKEN', 'REDIS_URL'];
assert(privateNames.every(key => !(key in process.env)), 'Private environment leaked');
const redis = new Redis('redis://127.0.0.1:6379/14', { maxRetriesPerRequest: 3 });
try {
  assert.equal(await redis.hexists(SESSION_KEYS.registry, name), 1);
  const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
  assert.equal(sessions.length, 1);
  assert.equal(await redis.hget(SESSION_KEYS.session(sessions[0]), 'agent_name'), name);
  assert.equal(await redis.exists(SESSION_KEYS.lease(sessions[0])), 1);
} finally { await redis.quit(); }
const url = new URL(process.env.GPTQ_BRIDGE_URL);
assert.equal(url.hostname, '127.0.0.1');
const port = Number(url.port).toString(16).toUpperCase().padStart(4, '0');
const listeners = (await readFile('/proc/net/tcp', 'utf8')).split('\n').slice(1)
  .map(line => line.trim().split(/\s+/))
  .filter(fields => fields[3] === '0A' && fields[1]?.endsWith(':' + port))
  .map(fields => fields[1]);
assert.deepEqual(listeners, ['0100007F:' + port]);
const client = new Client({ name: 'wrapper-startup-verifier', version: '1' });
const transport = new StreamableHTTPClientTransport(url, {
  requestInit: { headers: { authorization: 'Bearer ' + process.env.GPTQ_BRIDGE_TOKEN } },
});
try {
  await client.connect(transport);
  const catalog = await client.listTools();
  assert.deepEqual(catalog.tools.map(tool => tool.name), [...WRAPPER_VISIBLE_TOOLS]);
  assert(catalog.tools.every(tool => !('session_id' in (tool.inputSchema.properties ?? {}))));
} finally {
  await transport.terminateSession().catch(() => {});
  await client.close();
}
await writeFile(join(run, kind + '.ready.json'), JSON.stringify({
  name, registry: true, active_sessions: 1, lease: true,
  private_env_absent: privateNames, listeners, bridge_tools: WRAPPER_VISIBLE_TOOLS,
}), { mode: 0o600 });
const gateDeadline = Date.now() + 30_000;
while (!(await access(join(run, 'go')).then(() => true, () => false))) {
  assert(Date.now() < gateDeadline, 'Both-agent registration gate timed out');
  await delay(50);
}
const args = process.argv.slice(2);
const child = spawn(kind, kind === 'pi' ? [args[0], '--mode', 'json', ...args.slice(1)] : args, {
  stdio: 'inherit',
});
const interrupt = () => child.kill('SIGINT');
const terminate = () => child.kill('SIGTERM');
process.on('SIGINT', interrupt);
process.on('SIGTERM', terminate);
const [code, signal] = await once(child, 'exit');
process.off('SIGINT', interrupt);
process.off('SIGTERM', terminate);
process.exitCode = code ?? 128 + (constants.signals[signal] ?? 0);
