#!/usr/bin/env node
// Real Pi SDK, live inference and registered sidecars; queue writes use db15 only.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { createRegisteredPiExtension } from '../dist/registered-shell/pi-extension.js';

import { execFileSync } from 'node:child_process';
import { homedir } from 'node:os';
import { CodexSocketClient } from '../dist/registered-shell/codex-socket.js';
import { CodexThreadReader } from '../dist/registered-shell/codex-history.js';
const installed = process.env.PI_NODE_MODULES ?? join(dirname(dirname(process.execPath)), 'lib', 'node_modules');
const { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } =
  await import(pathToFileURL(join(installed, '@earendil-works/pi-coding-agent/dist/index.js')).href);
const root = resolve(import.meta.dirname, '..');
const previousCwd = process.cwd();
const directory = join(root, '.gptqueue/automatic-shell/event-delivery', `native-cross-live-${randomUUID()}`);
await mkdir(directory, { recursive: true });
const started = Date.now();
const receipt = { started_at: new Date().toISOString(), passed: false, database: 15,
  inference: 'live-provider', provider: 'openrouter', model: 'z-ai/glm-5.3-flash', roles: {}, exchanges: [] };
const sessions = new Map();
const errors = [];
const publicContent = content => Array.isArray(content)
  ? content.filter(c => c.type === 'text' || c.type === 'toolCall') : content;
const redis = new Redis('redis://127.0.0.1:6379/15');
const tool = async (session, name, args = {}) => {
  const definition = session.agent.state.tools.find(item => item.name === name);
  assert.ok(definition, `Missing ${name}`);
  return (await definition.execute(`probe-${randomUUID()}`, args, AbortSignal.timeout(15000))).details.structuredContent;
};
const messages = session => session.agent.state.messages;
const results = session => messages(session).filter(item => item.role === 'toolResult');
const decoded = item => item.details?.structuredContent ?? JSON.parse(item.content.find(c => c.type === 'text').text);

const rpc = new CodexSocketClient(undefined, 15000);
const reader = new CodexThreadReader(rpc);
const call = (method, params) => rpc.request(method, params, AbortSignal.timeout(20000));
let threadId;
const codexTool = async (name, args = {}) => (await call('mcpServer/tool/call', {
  threadId, server: 'gptqueue-shared', tool: name, arguments: args })).structuredContent;
const history = () => reader.read(threadId, AbortSignal.timeout(10000));
const trace = async client => (await redis.xrange(`gptq:inbox-trace:${receipt.roles[client].agent}`, '-', '+'))
  .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 },
    (_, i) => [fields[2 * i], fields[2 * i + 1]])));
const exactAck = (traces, id) => {
  const claim = traces.find(t => t.stage === 'task_claimed' && t.message_id === id);
  return claim && traces.some(t => t.stage === 'task_acknowledged' && t.claim_id === claim.claim_id);
};
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

try {
  // Use Pi's normal configured authentication without copying or exposing credentials.
  const modelRuntime = await ModelRuntime.create({ allowModelNetwork: false });
  const model = modelRuntime.getModel(receipt.provider, receipt.model);
  assert.ok(model, 'Requested model is absent from the local Pi catalog');
  assert.ok(modelRuntime.hasConfiguredAuth(receipt.provider), 'Requested provider has no configured authentication');
  process.chdir(directory);
  for (const role of ['pi']) {
    const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
    const loader = new DefaultResourceLoader({ cwd: directory, agentDir: directory, settingsManager,
      noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      systemPrompt: 'You are an isolated GPTQueue integration-test participant. Use only GPTQueue tools. ' +
        'When activated, claim tasks. For a task, compute the requested answer, send the number only as a result to task.from, ' +
        'with in_reply_to equal to task.id and idempotency_key equal to task.id, then acknowledge the claim. ' +
        'For a result, acknowledge the claim without replying and say SENDER_RECEIVED_42 if its content is 42. ' +
        'Do not contact agents except the sender of your claimed task. Do not access files or execute commands.',
      extensionFactories: [createRegisteredPiExtension({ redisUrl: 'redis://127.0.0.1:6379/15',
        nodePath: process.execPath, sidecarPath: join(root, 'bin/gptqueue-session') })] });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, []);
    const { session } = await createAgentSession({ cwd: directory, agentDir: directory, modelRuntime,
      model, thinkingLevel: 'low', settingsManager, sessionManager: SessionManager.inMemory(directory),
      resourceLoader: loader, noTools: 'builtin' });
    sessions.set(role, session);
    session.subscribe(event => {
      if (event.type === 'message_end') console.log(JSON.stringify({ event: 'pi_message_end', role,
        message_role: event.message.role, tool: event.message.toolName,
        stop_reason: event.message.stopReason, is_error: event.message.isError }));
    });
    // Deliberately retain the SDK's native streamFunction: no inference substitution.
    await session.bindExtensions({ onError: error => errors.push({ role, error: String(error) }) });
    const status = await tool(session, 'get_runtime_status');
    assert.equal(status.activation_ready, true);
    assert.equal(status.runtime.runtime_id, session.sessionManager.getSessionId());
    receipt.roles[role] = { agent: status.agent, runtime: status.runtime };
    console.log(JSON.stringify({ event: 'pi_runtime_ready', role, ...receipt.roles[role] }));
  }

  const cwd = join(directory, 'codex');
  await mkdir(cwd);
  const created = await call('thread/start', { cwd, model: 'gpt-5.6-luna',
    approvalPolicy: 'on-request', approvalsReviewer: 'auto_review', sandbox: 'workspace-write', config,
    developerInstructions: 'You are an isolated GPTQueue integration test participant. Use only GPTQueue tools. ' +
      'Claim queued tasks. For a task compute its answer, send the number only as a result to task.from, ' +
      'with in_reply_to and idempotency_key both equal to task.id, then acknowledge the claim. ' +
      'For a correlated reply acknowledge without replying and say SENDER_RECEIVED_42 if its content is 42. ' +
      'Do not access files, run commands or contact agents other than the sender of a claimed task.' });
  threadId = created.thread.id;
  receipt.roles.codex = { thread_id: threadId, model: 'gpt-5.6-luna' };
  await call('turn/start', { threadId, input: [{ type: 'text', text:
    'Initialize this isolated integration-test session. Reply READY without calling queue tools.' }] });
  for (let i = 0; i < 60; i++) {
    const status = await codexTool('get_runtime_status');
    if (status?.activation_ready) { Object.assign(receipt.roles.codex, status); break; }
    await delay(500);
  }
  assert.equal(receipt.roles.codex.activation_ready, true);
  console.log(JSON.stringify({ event: 'cross_runtime_ready', roles: receipt.roles }));
  for (const [sender, recipient] of [['codex', 'pi'], ['pi', 'codex']]) {
    const exchange = { sender, recipient, passed: false, started_at: new Date().toISOString() };
    receipt.exchanges.push(exchange);
    const start = Date.now();
    const piMessageOffset = messages(sessions.get('pi')).length;
    const previousTurns = new Set((await history()).turns.map(turn => turn.id));
    const send = sender === 'codex' ? codexTool : (name, args) => tool(sessions.get('pi'), name, args);
    const sent = await send('send_message', { to: receipt.roles[recipient].agent, type: 'task',
      content: 'Compute 17 + 25 and reply with the number only.', idempotency_key: randomUUID() });
    assert.equal(sent.status, 'sent');
    exchange.message_id = sent.message_id;
    for (let attempt = 0; attempt < 180; attempt++) {
      const recipientTrace = await trace(recipient), senderTrace = await trace(sender);
      const reply = recipientTrace.find(t => t.stage === 'reply_sent' && t.in_reply_to === exchange.message_id);
      const piSession = sessions.get('pi');
      const codexHistory = await history();
      const piMessages = messages(piSession);
      const piClaims = results(piSession).filter(item => item.toolName === 'claim_tasks' && !item.isError)
        .flatMap(item => decoded(item).claim?.tasks ?? []).map(item => typeof item === 'string' ? JSON.parse(item) : item);
      const piMessage = piClaims.find(item => item.id === (sender === 'pi' ? reply?.message_id : exchange.message_id));
      const marker = sender === 'pi'
        ? piMessages.slice(piMessageOffset).some(item => item.role === 'assistant' && item.content.some(c => c.type === 'text' && c.text.includes('SENDER_RECEIVED_42')))
        : codexHistory.turns.some(t => !previousTurns.has(t.id) && t.status === 'completed' && t.items.some(item => item.type === 'agentMessage' && item.text?.includes('SENDER_RECEIVED_42')));
      const depths = await Promise.all(Object.values(receipt.roles).map(role => redis.llen(`gptq:q:${role.agent}`)));
      if (reply && exactAck(recipientTrace, exchange.message_id) && exactAck(senderTrace, reply.message_id) &&
          marker && piMessage && !piSession.isStreaming && !codexHistory.turns.some(t => t.status === 'inProgress') && depths.every(d => d === 0)) {
        assert.equal(piMessage.from, receipt.roles.codex.agent);
        if (sender === 'pi') {
          assert.equal(piMessage.payload.content, '42');
          assert.equal(piMessage.payload.in_reply_to, exchange.message_id);
        } else {
          const replyCall = piMessages.slice(piMessageOffset).filter(m => m.role === 'assistant')
            .flatMap(m => m.content).find(c => c.type === 'toolCall' && c.name === 'send_message' &&
              c.arguments.in_reply_to === exchange.message_id);
          assert.ok(replyCall, 'Pi must send the exact correlated reply');
          assert.equal(replyCall.arguments.content, '42');
          assert.equal(replyCall.arguments.to, receipt.roles.codex.agent);
          const replyResult = results(piSession).find(m => m.toolCallId === replyCall.id && !m.isError);
          assert.ok(replyResult);
          assert.equal(decoded(replyResult).message_id, reply.message_id);
        }
        const responses = piMessages.filter(item => item.role === 'assistant');
        assert.ok(responses.length > 0 && responses.every(m => m.provider === receipt.provider && m.model === receipt.model));
        exchange.reply_id = reply.message_id;
        exchange.elapsed_ms = Date.now() - start;
        exchange.passed = true;
        exchange.sender_trace = senderTrace;
        exchange.recipient_trace = recipientTrace;
        console.log(JSON.stringify({ event: 'cross_exchange_passed', sender, recipient, elapsed_ms: exchange.elapsed_ms }));
        break;
      }
      const failure = piMessages.find(m => m.role === 'assistant' && m.stopReason === 'error');
      if (failure) throw new Error(failure.errorMessage ?? 'Pi live inference failed');
      if (attempt % 10 === 0) console.log(JSON.stringify({ event: 'cross_waiting', sender, recipient, elapsed_ms: Date.now() - start, depths }));
      await delay(1000);
    }
    assert.equal(exchange.passed, true, `${sender} to ${recipient} timed out`);
  }
  assert.deepEqual(errors, []);
  receipt.passed = true;
} catch (error) {
  receipt.error = String(error);
  process.exitCode = 1;
} finally {
  receipt.elapsed_ms = Date.now() - started;
  receipt.errors = errors;
  await writeFile(join(directory, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  for (const [role, session] of sessions) {
    await session.abort();
    receipt.roles[role] ??= {};
    receipt.roles[role].messages = messages(session).map(item => ({ role: item.role, toolName: item.toolName, toolCallId: item.toolCallId,
      isError: item.isError, model: item.model, provider: item.provider, stopReason: item.stopReason,
      content: publicContent(item.content) }));
    await session.extensionRunner.emit({ type: 'session_shutdown' });
    session.dispose();
  }
  if (threadId) {
    try {
      const thread = await history();
      receipt.codex_turns = thread.turns;
      for (const turn of thread.turns) if (turn.status === 'inProgress') await call('turn/interrupt', { threadId, turnId: turn.id });
      await call('thread/archive', { threadId });
    } catch (error) { receipt.cleanup_error = String(error); }
  }
  process.chdir(previousCwd);
  await redis.quit();
  await rpc.close();
  const output = join(directory, 'receipt.json');
  await writeFile(output, JSON.stringify(receipt, null, 2) + '\n');
  console.log(JSON.stringify({ passed: receipt.passed, elapsed_ms: receipt.elapsed_ms, receipt: output, error: receipt.error }));
}
