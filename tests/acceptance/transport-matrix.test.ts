import { it, expect } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { root, runRoot, redisUrl, until } from './pi-support.js';
import { checkExchangeEvidence } from './oracle.js';

/** A protocol-only matrix: model initiative and native activation are not claimed. */
it.skipIf(process.env.GPTQUEUE_TRANSPORT_ACCEPTANCE !== '1')('every ordered stdio/HTTP/explicit-session pair preserves all message types', async () => {
  const run = randomUUID(), directory = join(runRoot, 'transport', run);
  await mkdir(directory, { recursive: true });
  const clients: Client[] = [], edges: unknown[] = [];
  let server: ChildProcess | undefined;
  const receipt: Record<string, unknown> = { run, passed: false, revision: 2, kind: 'synthetic-MCP-wire-and-correlated-exchange', edges };
  const call = async (client: Client, name: string, args: Record<string, unknown> = {}) => {
    const r = await client.callTool({ name, arguments: args });
    if (r.isError) throw new Error(`MCP ${name} failed`);
    return r.structuredContent as Record<string, any>;
  };
  try {
    // Port 8199 is exclusively owned by this run; never attach to an existing server.
    const port = 8199;
    const net = await import('node:net');
    const probe = net.createServer();
    await new Promise<void>((resolve, reject) => { probe.once('error', reject); probe.listen(port, '127.0.0.1', resolve); });
    await new Promise<void>(resolve => probe.close(() => resolve()));
    server = spawn(process.execPath, [join(root, 'dist/transports/http.js'), '--port', String(port)],
      { cwd: directory, env: { ...process.env, REDIS_URL: redisUrl }, stdio: ['ignore', 'ignore', 'ignore'] });
    await until(async () => {
      if (server!.exitCode !== null) throw new Error('Owned HTTP fixture exited');
      try { return (await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(500) })).ok; } catch { return false; }
    }, value => value);
    const connect = async (stdio: boolean) => {
      const client = new Client({ name: `acceptance-${run}`, version: '1' }); clients.push(client);
      await client.connect(stdio ? new StdioClientTransport({ command: process.execPath,
        args: [join(root, 'dist/mcp-server/index.js')], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> })
        : new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`)));
      return client;
    };
    const peers = [];
    for (const instance of [1, 2]) for (const route of ['stdio', 'http', 'explicit-session']) {
      const owner = await connect(route === 'stdio'), name = `acceptance-${route}-${instance}-${run}`;
      const registration = await call(owner, 'register_agent', { name, role: 'both', description: 'synthetic transport matrix' });
      const wire = route === 'explicit-session' ? await connect(false) : owner;
      const session = route === 'explicit-session' ? { session_id: registration.session_id } : {};
      peers.push({ route, name, owner, wire, session });
    }
    for (const sender of peers) for (const recipient of peers) if (sender !== recipient) {
      for (const type of ['task', 'result', 'status', 'error', 'ping']) {
        const content = `wire-${randomUUID()}`;
        const sent = await call(sender.wire, 'send_message', { ...sender.session, to: recipient.name, type, content, idempotency_key: randomUUID() });
        const received = await call(recipient.wire, 'receive_message', { ...recipient.session, timeout: 1 });
        expect(received.message).toMatchObject({ id: sent.message_id, from: sender.name, to: recipient.name, type, payload: { content } });
        const expected = `reply:${content}`;
        const replied = await call(recipient.wire, 'send_message', { ...recipient.session, to: sender.name,
          type: 'result', content: expected, in_reply_to: received.message.id, idempotency_key: randomUUID() });
        const returned = (await call(sender.wire, 'receive_message', { ...sender.session, timeout: 1 })).message;
        expect(returned.id).toBe(replied.message_id);
        const verdict = checkExchangeEvidence({ sender: sender.name, recipient: recipient.name,
          expected_reply_content: expected, execution: { status: 'completed' },
          request: { id: received.message.id, from: received.message.from, to: received.message.to, content: received.message.payload.content },
          reply: { id: returned.id, from: returned.from, to: returned.to,
            in_reply_to: returned.payload.in_reply_to, content: returned.payload.content },
          request_consumption: { message_id: received.message.id, actor: recipient.name, consumed: true, acknowledged: false },
          reply_consumption: { message_id: returned.id, actor: sender.name, consumed: true, acknowledged: false },
          request_requires_ack: false, reply_requires_ack: false,
        });
        expect(verdict.outcome).toBe('meets');
        edges.push({ from: sender.route, to: recipient.route, sender: sender.name, recipient: recipient.name,
          type, message_id: sent.message_id, reply_id: returned.id, exact_received: true, exchange_verdict: verdict });
      }
    }
    expect(edges).toHaveLength(150); receipt.passed = true;
    for (const peer of peers) await call(peer.owner, 'close_session');
  } catch (error) { receipt.error = String(error); throw error; }
  finally {
    for (const client of clients.reverse()) await client.close().catch(() => undefined);
    if (server && server.exitCode === null) {
      server.kill('SIGTERM');
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => { server!.kill('SIGKILL'); resolve(); }, 3000);
        server!.once('exit', () => { clearTimeout(timer); resolve(); });
      });
    }
    await writeFile(join(directory, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600 });
  }
}, 90_000);
