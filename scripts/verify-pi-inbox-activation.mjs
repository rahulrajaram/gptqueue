#!/usr/bin/env node
// Native Pi SDK lifecycle with deterministic local inference; no credentials or Redis.
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { createRegisteredPiExtension, createPiExtension, GPTQUEUE_TOOLS, RUNTIME_TOOL_NAMES } from '../dist/registered-shell/pi-extension.js';
const installed = process.env.PI_NODE_MODULES ?? '/home/rahul/nodeenv2251-311/lib/node_modules';
const moduleAt = (pkg, file) => import(pathToFileURL(join(installed, '@earendil-works', pkg, 'dist', file)).href);
const { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } = await moduleAt('pi-coding-agent', 'index.js');
const { getModel } = await moduleAt('pi-ai', 'compat.js');
const { AssistantMessageEventStream } = await moduleAt('pi-ai', 'utils/event-stream.js');
const realSidecar = process.argv.includes('--real-sidecar');
const root = process.cwd();
const { RedisClient } = await import('../dist/mcp-server/redis-client.js');
const { randomUUID } = await import('node:crypto');
let sender;
const temporary = await mkdtemp(join(tmpdir(), 'gptqueue-native-pi-'));
let session, handler, binding, closed = false;
const receipt = { kind: 'native-pi-sdk-local-inference', passed: false, external_model_calls: 0, real_sidecar: realSidecar, turns: 0 };
const client = {
  listTools: async () => ({ tools: [...GPTQUEUE_TOOLS, ...RUNTIME_TOOL_NAMES].map(name => ({ name, inputSchema: { type: 'object', properties: {} } })) }),
  getInstructions: () => 'Isolated GPTQueue native lifecycle verification.',
  setActivationHandler: value => { handler = value; },
  callTool: async ({ name, arguments: args }) => { if (name === 'bind_runtime') binding = args; return { content: [], structuredContent: { activation_ready: true } }; },
  close: async () => { closed = true; },
};
const waitFor = async check => { for (let i=0; i<200; i++) { if (await check()) return; await delay(25); } throw new Error('Native Pi lifecycle condition timed out'); };
try {
  const modelRuntime = await ModelRuntime.create({ authPath: join(temporary, 'auth.json'), modelsPath: null,
    modelsStorePath: join(temporary, 'models-cache.json'), allowModelNetwork: false, refreshOnCreate: false });
  // The inference boundary is explicitly substituted, while all Pi lifecycle machinery is native.
  modelRuntime.hasConfiguredAuth = () => true;
  const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
  const loader = new DefaultResourceLoader({ cwd: temporary, agentDir: temporary, settingsManager,
    noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [realSidecar ? createRegisteredPiExtension({ redisUrl: 'redis://127.0.0.1:6379/15', nodePath: process.execPath, sidecarPath: join(root, 'bin/gptqueue-session') }) : createPiExtension(async () => client, 1000, { runtimeEnabled: true })] });
  if (realSidecar) process.chdir(temporary);
  await loader.reload();
  assert.equal(loader.getExtensions().errors.length, 0);
  ({ session } = await createAgentSession({ cwd: temporary, agentDir: temporary, modelRuntime,
    model: getModel('openai', 'gpt-4o'), thinkingLevel: 'off', settingsManager,
    sessionManager: SessionManager.inMemory(temporary), resourceLoader: loader, noTools: 'builtin' }));
  session.agent.streamFunction = (model, context) => {
    receipt.turns++;
    const stream = new AssistantMessageEventStream();
    let content = [{ type: 'text', text: 'native fixture completed' }];
    if (realSidecar) {
      const claimResult = context.messages.filter(x => x.role === 'toolResult' && x.toolName === 'claim_tasks').at(-1);
      const claim = claimResult ? JSON.parse(claimResult.content.find(x => x.type === 'text').text).claim : null;
      const task = claim ? JSON.parse(claim.tasks[0]) : null;
      const command = receipt.turns === 1 ? { name: 'claim_tasks', arguments: { max_batch: 1 } }
        : receipt.turns === 2 ? { name: 'send_message', arguments: { to: task.from, type: 'result', content: '42', in_reply_to: task.id, idempotency_key: task.id } }
        : receipt.turns === 3 ? { name: 'acknowledge_tasks', arguments: { claim_id: claim.claim_id } } : null;
      if (command) content = [{ type: 'toolCall', id: `native-call-${receipt.turns}`, ...command }];
    }
    const message = { role: 'assistant', content,
      api: model.api, provider: model.provider, model: model.id, timestamp: Date.now(), stopReason: content[0].type === 'toolCall' ? 'toolUse' : 'stop',
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    setTimeout(() => { stream.push({ type: 'done', reason: message.stopReason, message }); stream.end(message); }, 100);
    return stream;
  };
  const errors = [];
  await session.bindExtensions({ onError: error => errors.push(String(error)) });
  if (realSidecar) {
    const statusTool = session.agent.state.tools.find(x => x.name === 'get_runtime_status');
    const result = await statusTool.execute('probe-status', {}, new AbortController().signal);
    const status = result.details.structuredContent;
    assert.equal(status.activation_ready, true);
    assert.equal(status.runtime.runtime_id, session.sessionManager.getSessionId());
    sender = new RedisClient(null, 'redis://127.0.0.1:6379/15');
    await sender.register('both', `native-pi-sender-${randomUUID()}`);
    const message = { id: randomUUID(), from: sender.agentName, to: status.agent, type: 'task', timestamp: new Date().toISOString(), payload: { content: 'Compute 17 + 25' } };
    await sender.sendMessage(message);
    await waitFor(() => receipt.turns >= 4 && !session.isStreaming);
    const reply = await sender.receiveMessage(1);
    assert.equal(reply.payload.in_reply_to, message.id);
    assert.equal(reply.payload.content, '42');
    assert.equal(await sender.getQueueDepth(status.agent), 0);
    receipt.recipient = status.agent;
    receipt.message_id = message.id;
    receipt.reply_id = reply.id;
    assert.deepEqual(errors, []);
    receipt.passed = true;
  } else {
  assert.equal(binding.runtime_id, session.sessionManager.getSessionId());
  assert.equal(binding.working_directory, temporary);
  const request = id => ({ operation_id: id, prompt: `native fixture ${id}` });
  const signal = new AbortController().signal;
  assert.equal((await handler(binding, request('idle'), signal)).status, 'queued');
  await waitFor(() => receipt.turns === 1);
  assert.equal((await handler(binding, request('busy'), signal)).status, 'queued');
  await waitFor(() => receipt.turns === 2 && !session.isStreaming);
  await waitFor(async () => (await handler(binding, { ...request('idle'), recover_only: true }, signal)).status === 'completed');
  assert.equal((await handler(binding, { ...request('busy'), recover_only: true }, signal)).status, 'completed');
  assert.equal(receipt.turns, 2);
  assert.deepEqual(errors, []);
  await session.extensionRunner.emit({ type: 'session_before_switch', reason: 'new' });
  assert.equal(closed, true);
  assert.equal((await handler(binding, request('stale'), signal)).status, 'unavailable');
  receipt.passed = true;
  }
} catch (error) { receipt.error = String(error); receipt.tool_results = session?.agent.state.messages.filter(x => x.role === 'toolResult'); process.exitCode = 1; }
finally {
  if (session) { await session.abort(); await session.extensionRunner.emit({ type: 'session_shutdown' }); session.dispose(); }
  if (sender) { await sender.closeCurrentSession(); await sender.shutdown(); }
  process.chdir(root);
  await rm(temporary, { recursive: true, force: true });
  const output = resolve('.gptqueue/automatic-shell/event-delivery/' + (realSidecar ? 'native-pi-sidecar-receipt.json' : 'native-pi-receipt.json'));
  await mkdir(resolve('.gptqueue/automatic-shell/event-delivery'), { recursive: true });
  await writeFile(output, JSON.stringify(receipt, null, 2)+'\n');
  console.log(JSON.stringify({ ...receipt, receipt: output }));
}
