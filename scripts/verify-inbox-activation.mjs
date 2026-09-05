#!/usr/bin/env node
// Real native Codex turns; all queue writes are confined to Redis db15.
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { resolve, join } from 'node:path';
import { homedir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { CodexSocketClient } from '../dist/registered-shell/codex-socket.js';
import { bindCodexHook } from '../dist/registered-shell/codex-hook.js';
import { CodexThreadReader } from '../dist/registered-shell/codex-history.js';

const root = resolve(import.meta.dirname, '..');
const automaticHook = process.argv.includes('--automatic-hook');
const nonce = randomUUID();
const directory = join(root, '.gptqueue/automatic-shell/event-delivery', `native-${nonce}`);
await mkdir(directory, { recursive: true });
const rpc = new CodexSocketClient(undefined, 15_000);
const redis = new Redis('redis://127.0.0.1:6379/15');
const owned = [];
const started = Date.now();
const receipt = { nonce, started_at: new Date().toISOString(), database: 15, automatic_hook: automaticHook, roles: {}, passed: false };
const call = (method, params) => rpc.request(method, params, AbortSignal.timeout(20_000));
const tool = (threadId, name, args = {}) => call('mcpServer/tool/call', { threadId, server: 'gptqueue-shared', tool: name, arguments: args });
const configPath = join(process.env.CODEX_HOME ?? join(homedir(), '.codex'), 'config.toml');
const serverNames = JSON.parse(execFileSync('python3', ['-c',
  'import json,sys,tomllib;print(json.dumps(list(tomllib.load(open(sys.argv[1],"rb")).get("mcp_servers",{}))))', configPath], { encoding: 'utf8' }));
const config = Object.fromEntries(serverNames.flatMap((name) => [
  [`mcp_servers.${name}.enabled`, false], [`mcp_servers.${name}.required`, false],
]));
Object.assign(config, {
  'mcp_servers.gptqueue-shared.enabled': true,
  'mcp_servers.gptqueue-shared.required': true,
  'mcp_servers.gptqueue-shared.command': process.execPath,
  'mcp_servers.gptqueue-shared.args': [join(root, 'bin/gptqueue-session'), '--client', 'codex', '--redis-url', 'redis://127.0.0.1:6379/15'],
  'model_reasoning_effort': 'low',
});
const readers = new Map();
const history = async (threadId) => {
 if (!readers.has(threadId)) readers.set(threadId, new CodexThreadReader(rpc));
 return readers.get(threadId).read(threadId, AbortSignal.timeout(10000));
};
try {
  for (const role of ['sender', 'recipient']) {
    const cwd = join(directory, role);
    await mkdir(cwd);
    const result = await call('thread/start', { cwd, model: 'gpt-5.6-luna', approvalPolicy: 'on-request', approvalsReviewer: 'auto_review', sandbox: 'workspace-write', config,
      developerInstructions: 'You are an isolated GPTQueue integration test participant. Use only the gptqueue-shared tools. ' +
        'Claim queued tasks, send a correlated result for a task using a stable idempotency key, and acknowledge only after processing. ' +
        'For results, acknowledge without sending another reply, then say SENDER_RECEIVED_42 if the result is 42. ' +
        'Do not access files, run shell commands, or contact any other agents.' });
    const id = result.thread.id;
    owned.push(id);
    receipt.roles[role] = { thread_id: id, cwd };
    console.log(JSON.stringify({ event: 'native_thread_created', role, thread_id: id }));
    const hookRpc = { close: () => rpc.close(), request: async (...args) => {
      try { const result = await rpc.request(...args); if (result.isError) receipt.binding_error = result.structuredContent ?? result.content; return result; }
      catch (error) { receipt.binding_error = String(error); throw error; }
    } };
    if (automaticHook) {
      // Exercise normal first-prompt lifecycle; thread/start alone allocates an idle thread.
      await call('turn/start', { threadId: id, input: [{ type: 'text', text:
        'Initialize this isolated integration-test session. Reply READY without calling any queue tools.' }] });
      let ready = false;
      for (let retry = 0; retry < 60; retry++) {
        const current = await tool(id, 'get_runtime_status');
        if (current.structuredContent?.activation_ready) { ready = true; break; }
        await delay(500);
      }
      if (!ready) throw new Error(`Automatic SessionStart hook did not bind ${role}`);
    } else if (!await bindCodexHook({ session_id: id, cwd, hook_event_name: 'SessionStart' }, hookRpc)) throw new Error(`Binding failed for ${role}`);
    const status = (await tool(id, 'get_runtime_status')).structuredContent;
    if (!status?.activation_ready) throw new Error(`${role} is not activation ready`);
    receipt.roles[role] = { thread_id: id, agent: status.agent, runtime: status.runtime };
    console.log(JSON.stringify({ event: 'native_runtime_ready', role, thread_id: id, agent: status.agent }));
  }
  const sent = (await tool(receipt.roles.sender.thread_id, 'send_message', {
    to: receipt.roles.recipient.agent, type: 'task', content: 'Compute 17 + 25 and reply with the number only.',
    idempotency_key: `native-proof-${nonce}`,
  })).structuredContent;
  if (sent?.status !== 'sent') throw new Error('Task was not accepted');
  receipt.message_id = sent.message_id;
  console.log(JSON.stringify({ event: 'task_sent', message_id: sent.message_id }));
  for (let attempt = 0; attempt < 120; attempt++) {
    const sender = await history(receipt.roles.sender.thread_id);
    const recipient = await history(receipt.roles.recipient.thread_id);
    const summary = (thread) => thread.turns.map((turn) => ({ id: turn.id, status: turn.status,
      items: turn.items?.map((item) => ({ type: item.type, id: item.id, clientId: item.clientId,
        tool: item.tool, server: item.server, text: item.type === 'agentMessage' ? item.text : undefined })) }));
    receipt.sender_turns = summary(sender);
    receipt.recipient_turns = summary(recipient);
    const senderItems = sender.turns.flatMap((turn) => turn.items ?? []);
    const recipientItems = recipient.turns.flatMap((turn) => turn.items ?? []);
    const marker = senderItems.some((item) => item.type === 'agentMessage' && item.text?.includes('SENDER_RECEIVED_42'));
    const claimed = (items) => items.some((item) => item.type === 'mcpToolCall' && item.tool === 'claim_tasks');
    const acked = (items) => items.some((item) => item.type === 'mcpToolCall' && item.tool === 'acknowledge_tasks' && item.status === 'completed');
    const depths = await Promise.all(Object.values(receipt.roles).map((role) => redis.llen(`gptq:q:${role.agent}`)));
    if (marker && claimed(senderItems) && claimed(recipientItems) && acked(senderItems) && acked(recipientItems) && depths.every((value) => value === 0)) {
      receipt.passed = true; break;
    }
    if (attempt % 10 === 0) console.log(JSON.stringify({ event: 'native_proof_waiting', elapsed_ms: Date.now() - started,
      sender_turns: sender.turns.length, recipient_turns: recipient.turns.length, depths }));
    await delay(1_000);
  }
  if (!receipt.passed) throw new Error('Native message-to-turn-to-reply acceptance did not complete');
} catch (error) {
  receipt.error = String(error);
  process.exitCode = 1;
} finally {
  receipt.elapsed_ms = Date.now() - started;
  receipt.traces = {};
  for (const [role, value] of Object.entries(receipt.roles)) if (value.agent) receipt.traces[role] = await redis.xrange(`gptq:inbox-trace:${value.agent}`, '-', '+');
  await writeFile(join(directory, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  for (const threadId of owned) {
    try {
      const thread = await history(threadId);
      for (const turn of thread.turns) if (turn.status === 'inProgress') await call('turn/interrupt', { threadId, turnId: turn.id });
      await call('thread/archive', { threadId });
    } catch { /* Exact owned IDs remain recorded for targeted recovery. */ }
  }
  await redis.quit(); await rpc.close();
  console.log(JSON.stringify({ passed: receipt.passed, elapsed_ms: receipt.elapsed_ms, receipt: join(directory, 'receipt.json'), error: receipt.error }));
}
