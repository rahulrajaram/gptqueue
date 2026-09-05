#!/usr/bin/env node
// Real Pi SDK, live inference and registered sidecars; queue writes use db15 only.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { createRegisteredPiExtension } from '../dist/registered-shell/pi-extension.js';

const installed = process.env.PI_NODE_MODULES ?? '/home/rahul/nodeenv2251-311/lib/node_modules';
const { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } =
  await import(pathToFileURL(join(installed, '@earendil-works/pi-coding-agent/dist/index.js')).href);
const root = resolve(import.meta.dirname, '..');
const previousCwd = process.cwd();
const directory = join(root, '.gptqueue/automatic-shell/event-delivery', `native-pi-live-${randomUUID()}`);
await mkdir(directory, { recursive: true });
const started = Date.now();
const receipt = { started_at: new Date().toISOString(), passed: false, database: 15,
  inference: 'live-provider', provider: 'openrouter', model: 'z-ai/glm-5.3-flash', roles: {} };
const sessions = new Map();
const errors = [];
const publicContent = content => Array.isArray(content)
  ? content.filter(c => c.type === 'text' || c.type === 'toolCall') : content;
process.on('exit', code => {
  writeFileSync(join(directory, 'exit.json'), JSON.stringify({ code, receipt, errors,
    sessions: Object.fromEntries([...sessions].map(([role, session]) => [role,
      session.agent.state.messages.map(item => ({ role: item.role, toolName: item.toolName,
        isError: item.isError, stopReason: item.stopReason, errorMessage: item.errorMessage,
        content: publicContent(item.content) }))])) }, null, 2));
});
const redis = new Redis('redis://127.0.0.1:6379/15');
const tool = async (session, name, args = {}) => {
  const definition = session.agent.state.tools.find(item => item.name === name);
  assert.ok(definition, `Missing ${name}`);
  return (await definition.execute(`probe-${randomUUID()}`, args, AbortSignal.timeout(15000))).details.structuredContent;
};
const messages = session => session.agent.state.messages;
const results = session => messages(session).filter(item => item.role === 'toolResult');
const decoded = item => item.details?.structuredContent ?? JSON.parse(item.content.find(c => c.type === 'text').text);
try {
  // Use Pi's normal configured authentication without copying or exposing credentials.
  const modelRuntime = await ModelRuntime.create({ allowModelNetwork: false });
  const model = modelRuntime.getModel(receipt.provider, receipt.model);
  assert.ok(model, 'Requested model is absent from the local Pi catalog');
  assert.ok(modelRuntime.hasConfiguredAuth(receipt.provider), 'Requested provider has no configured authentication');
  process.chdir(directory);
  for (const role of ['sender', 'recipient']) {
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
  assert.notEqual(receipt.roles.sender.agent, receipt.roles.recipient.agent);
  const sent = await tool(sessions.get('sender'), 'send_message', { to: receipt.roles.recipient.agent,
    type: 'task', content: 'Compute 17 + 25 and reply with the number only.', idempotency_key: randomUUID() });
  assert.equal(sent.status, 'sent');
  receipt.message_id = sent.message_id;
  console.log(JSON.stringify({ event: 'task_sent', message_id: sent.message_id }));
  for (let attempt = 0; attempt < 180; attempt++) {
    const sender = sessions.get('sender'), recipient = sessions.get('recipient');
    const senderResults = results(sender), recipientResults = results(recipient);
    const acknowledged = list => list.some(item => item.toolName === 'acknowledge_tasks' && !item.isError);
    const claimed = list => list.some(item => item.toolName === 'claim_tasks' && !item.isError);
    const marker = messages(sender).some(item => item.role === 'assistant' &&
      item.content.some(c => c.type === 'text' && c.text.includes('SENDER_RECEIVED_42')));
    const depths = await Promise.all(Object.values(receipt.roles).map(role => redis.llen(`gptq:q:${role.agent}`)));
    if (marker && claimed(senderResults) && claimed(recipientResults) && acknowledged(senderResults) &&
        acknowledged(recipientResults) && depths.every(depth => depth === 0) && !sender.isStreaming && !recipient.isStreaming) {
      const claimedReply = senderResults.filter(item => item.toolName === 'claim_tasks' && !item.isError)
        .flatMap(item => decoded(item).claim?.tasks ?? []).map(item => typeof item === 'string' ? JSON.parse(item) : item)
        .find(item => item.payload?.in_reply_to === receipt.message_id);
      assert.ok(claimedReply, 'Sender must claim the exact correlated reply');
      assert.equal(claimedReply.from, receipt.roles.recipient.agent);
      assert.equal(claimedReply.payload.content, '42');
      receipt.reply_id = claimedReply.id;
      for (const [role, session] of sessions) {
        const expectedId = role === 'sender' ? receipt.reply_id : receipt.message_id;
        const claim = results(session).filter(item => item.toolName === 'claim_tasks' && !item.isError)
          .map(item => decoded(item).claim).find(item => item?.tasks.some(task =>
            (typeof task === 'string' ? JSON.parse(task) : task).id === expectedId));
        assert.ok(claim, `${role} must claim the exact message`);
        const traces = (await redis.xrange(`gptq:inbox-trace:${receipt.roles[role].agent}`, '-', '+'))
          .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 },
            (_, index) => [fields[index * 2], fields[index * 2 + 1]])));
        assert.ok(traces.some(trace => trace.stage === 'task_acknowledged' && trace.claim_id === claim.claim_id),
          `${role} must acknowledge the exact claim`);
        const responses = messages(session).filter(item => item.role === 'assistant');
        assert.ok(responses.length > 0);
        assert.ok(responses.every(item => item.provider === receipt.provider && item.model === receipt.model),
          `${role} must use the requested live model`);
      }
      assert.deepEqual(errors, []);
      receipt.passed = true;
      break;
    }
    const failed = [...sessions.values()].flatMap(messages).find(item => item.role === 'assistant' && item.stopReason === 'error');
    if (failed) throw new Error(failed.errorMessage ?? 'Live model request failed');
    if (attempt % 10 === 0) console.log(JSON.stringify({ event: 'pi_proof_waiting', elapsed_ms: Date.now() - started, depths }));
    await delay(1000);
  }
  assert.equal(receipt.passed, true, 'Live Pi task/reply/ack roundtrip timed out');
} catch (error) {
  receipt.error = String(error);
  process.exitCode = 1;
} finally {
  receipt.elapsed_ms = Date.now() - started;
  receipt.errors = errors;
  receipt.traces = {};
  await writeFile(join(directory, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  for (const [role, session] of sessions) {
    await session.abort();
    // Retain only this isolated arithmetic conversation, without hidden reasoning or auth data.
    receipt.roles[role] ??= {};
    receipt.roles[role].messages = messages(session).map(item => ({ role: item.role, toolName: item.toolName,
      isError: item.isError, model: item.model, provider: item.provider, stopReason: item.stopReason,
      usage: item.usage, content: publicContent(item.content) }));
    const agent = receipt.roles[role].agent;
    if (agent) receipt.traces[role] = await redis.xrange(`gptq:inbox-trace:${agent}`, '-', '+');
    await session.extensionRunner.emit({ type: 'session_shutdown' });
    session.dispose();
  }
  process.chdir(previousCwd);
  await redis.quit();
  const output = join(directory, 'receipt.json');
  await writeFile(output, JSON.stringify(receipt, null, 2) + '\n');
  console.log(JSON.stringify({ passed: receipt.passed, model: receipt.model, elapsed_ms: receipt.elapsed_ms,
    receipt: output, error: receipt.error }));
}
