#!/usr/bin/env node
// Opt-in Linux live verification. Uses installed CLIs/adapter and Redis db14 only.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, writeFile, readFile, stat, readdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { WRAPPER_VISIBLE_TOOLS } from '../dist/experimental-wrapper/bridge.js';
import { verificationChecks } from './wrapper-verification-checks.mjs';

assert.deepEqual(process.argv.slice(2), ['--live'], 'Usage: node scripts/verify-registered-wrapper.mjs --live');
const repo = dirname(dirname(fileURLToPath(import.meta.url)));
const tag = randomUUID();
const run = join(repo, '.gptqueue', 'wrapper-verification', tag);
const workspace = join(repo, '.gptqueue', 'registered-wrapper-workspace');
await mkdir(run, { recursive: true });
await mkdir(workspace, { recursive: true });
const names = Object.freeze({ codex: 'gptqueue-experiment-codex-' + tag, pi: 'gptqueue-experiment-pi-' + tag });
const nonce = 'wrapper-proof-' + tag;
const fingerprint = async path => {
  try {
    const metadata = await stat(path);
    return { mtime_ms: metadata.mtimeMs, sha256: createHash('sha256').update(await readFile(path)).digest('hex') };
  } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
};
const files = async path => {
  const entries = await readdir(path, { withFileTypes: true });
  return (await Promise.all(entries.map(entry => entry.isDirectory()
    ? files(join(path, entry.name)) : [join(path, entry.name)]))).flat();
};
const piHome = process.env.PI_CODING_AGENT_DIR || join(homedir(), '.pi', 'agent');
const paths = Object.freeze({
  pi_cache: join(piHome, 'mcp-cache.json'), pi_config: join(piHome, 'mcp.json'),
  pi_settings: join(piHome, 'settings.json'),
  codex_config: join(process.env.CODEX_HOME || join(homedir(), '.codex'), 'config.toml'),
});
const snapshot = async () => Object.fromEntries(await Promise.all(Object.entries(paths)
  .map(async ([key, path]) => [key, await fingerprint(path)])));
const before = await snapshot();
const localBefore = await files(workspace);
const redis = new Redis('redis://127.0.0.1:6379/14', { maxRetriesPerRequest: 3, commandTimeout: 5_000 });
const common = 'This disposable integration test is explicitly authorized. Use only the four GPTQueue tools. Do not use shell tools, skills, files, or other capabilities. First call list_agents and get_queue_status. Both peers are registered. Never register or supply session_id. ';
const prompts = Object.freeze({
  codex: common + `Send exactly one ping to ${names.pi}, content ${nonce}, idempotency_key ${nonce}. Receive with timeout 60 up to four times for the reply. Verify content ack-${nonce} and in_reply_to equal to your sent ID. Return only JSON with sent_message_id and reply (the complete received message).`,
  pi: common + `Receive with timeout 60 up to four times until a ping from ${names.codex} arrives. Verify content ${nonce}. Send exactly one result to ${names.codex}, content ack-${nonce}, in_reply_to equal to the received message id, idempotency_key exactly ${nonce}. Report the result.`,
});
const launch = kind => {
  const child = spawn(process.execPath, ['bin/gptqueue-experiment', kind, '--agent', names[kind],
    '--workspace', workspace, '--redis-url', 'redis://127.0.0.1:6379/14', '--cleanup', 'unregister', '--', prompts[kind]], {
    cwd: repo, detached: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, GPTQ_SESSION_ID: 'verification-session-sentinel', GPTQUEUE_HTTP_TOKEN: 'verification-token-sentinel',
      REDIS_URL: 'redis://127.0.0.1:6379/15', GPTQ_VERIFY_KIND: kind, GPTQ_VERIFY_RUN: run,
      GPTQ_EXPERIMENT_CODEX_BIN: join(repo, 'scripts/verify-wrapper-child.mjs'),
      GPTQ_EXPERIMENT_PI_BIN: join(repo, 'scripts/verify-wrapper-child.mjs') },
  });
  const output = { stdout: '', stderr: '' };
  for (const stream of ['stdout', 'stderr']) child[stream].on('data', chunk => {
    if (output[stream].length + chunk.length <= 2_000_000) output[stream] += chunk;
    else child.kill('SIGTERM');
  });
  const done = new Promise(resolve => {
    child.once('error', error => resolve({ kind, code: null, error: String(error), ...output }));
    child.once('close', (code, signal) => resolve({ kind, code, signal, ...output }));
  });
  return { kind, child, done };
};
const terminate = jobs => jobs.forEach(({ child }) => child.kill('SIGTERM'));
const killOwnedGroups = jobs => jobs.forEach(({ child }) => {
  if (!child.pid) return;
  try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
});
let jobs = [], results = [], failure = null, proof = null, started = false;
let deadline, escalation;
const stop = () => {
  failure ??= 'Verification interrupted or timed out';
  terminate(jobs);
  escalation ??= setTimeout(() => killOwnedGroups(jobs), 12_000);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
try {
  assert.equal(await redis.dbsize(), 0, 'db14 must start empty; no cleanup or flush attempted');
  started = true;
  jobs = ['codex', 'pi'].map(launch);
  deadline = setTimeout(stop, 240_000);
  const gateDeadline = Date.now() + 25_000;
  while (!(await Promise.all(['codex', 'pi'].map(kind => stat(join(run, kind + '.ready.json')).then(() => true, () => false)))).every(Boolean)) {
    assert(!failure && Date.now() < gateDeadline, 'Registration gate did not become ready');
    await delay(50);
  }
  await writeFile(join(run, 'go'), 'Both startup probes passed');
  console.log(JSON.stringify({ run, names, phase: 'both_registered_before_model_start' }));
  results = await Promise.all(jobs.map(job => job.done));
  assert(!failure, failure);
  assert(results.every(result => result.code === 0), 'Both wrappers must exit zero');
  const codex = JSON.parse(results.find(result => result.kind === 'codex').stdout.trim().replace(/^```json\s*|\s*```$/g, ''));
  const pi = results.find(result => result.kind === 'pi');
  const events = pi.stdout.split('\n').filter(Boolean).flatMap(line => {
    try { return [JSON.parse(line)]; } catch { return []; }
  });
  const calls = events.filter(event => event.type === 'tool_execution_start');
  assert(calls.every(call => WRAPPER_VISIBLE_TOOLS.includes(call.toolName)), 'Unexpected Pi tool invocation');
  assert(WRAPPER_VISIBLE_TOOLS.every(name => calls.some(call => call.toolName === name)), 'All four Pi tools must be exercised');
  assert.equal(calls.filter(call => call.toolName === 'send_message').length, 1, 'Pi must send exactly one reply');
  assert.equal(calls.find(call => call.toolName === 'send_message').args.idempotency_key, nonce, 'Pi must use the planned reply retry key');
  const completed = events.filter(event => event.type === 'tool_execution_end');
  assert(completed.every(event => !event.isError), 'Pi tool execution failed');
  const message = JSON.parse(completed.find(event => event.toolName === 'receive_message' && event.result.content[0].text.includes(nonce)).result.content[0].text);
  const sent = JSON.parse(completed.find(event => event.toolName === 'send_message').result.content[0].text);
  assert.equal(message.from, names.codex);
  assert.equal(message.to, names.pi);
  assert.equal(message.payload.content, nonce);
  assert.equal(message.id, codex.sent_message_id);
  assert.equal(codex.reply.id, sent.message_id);
  assert.equal(codex.reply.from, names.pi);
  assert.equal(codex.reply.to, names.codex);
  assert.equal(codex.reply.payload.content, 'ack-' + nonce);
  assert.equal(codex.reply.payload.in_reply_to, message.id);
  const catalogLine = pi.stderr.split('\n').find(line => line.startsWith('[gptqueue-experiment] Pi active tools: '));
  assert(catalogLine, 'No Pi active-catalog attestation before inference');
  const active = JSON.parse(catalogLine.slice(catalogLine.indexOf(': ') + 2));
  assert.deepEqual(active, [...WRAPPER_VISIBLE_TOOLS]);
  proof = { sent_id: message.id, reply_id: sent.message_id, exact_content: true, in_reply_to: true, pi_active_tools: active };
} catch (error) {
  failure = String(error);
  stop();
} finally {
  results = await Promise.all(jobs.map(job => job.done));
  clearTimeout(deadline); clearTimeout(escalation);
  process.off('SIGINT', stop); process.off('SIGTERM', stop);
  for (const result of results) for (const stream of ['stdout', 'stderr']) {
    await writeFile(join(run, result.kind + '.' + stream), result[stream], { mode: 0o600 });
  }
  try {
    const exactKeys = Object.fromEntries(Object.entries(names).map(([kind, name]) =>
      [kind, 'gptq:idempotency:' + name + ':' + encodeURIComponent(nonce)]));
    const retryIds = Object.fromEntries(await Promise.all(Object.entries(exactKeys).map(async ([kind, key]) =>
      [kind, started ? await redis.get(key) : null])));
    const deletedKeys = Object.entries(exactKeys).filter(([kind]) => retryIds[kind] !== null).map(([, key]) => key);
    if (deletedKeys.length) await redis.del(...deletedKeys);
    const remaining = await redis.keys('*');
    const after = await snapshot();
    const changes = Object.keys(paths).filter(key => JSON.stringify(before[key]) !== JSON.stringify(after[key]));
    const contentChanges = Object.keys(paths).filter(key => before[key]?.sha256 !== after[key]?.sha256);
    const newFiles = (await files(workspace)).filter(path => !localBefore.includes(path));
    const piRuntime = join(workspace, '.gptqueue-wrapper', createHash('sha256').update(names.pi).digest('hex').slice(0, 16)) + '/';
    const checks = verificationChecks({ failure, proof, exits: results, remaining, before, after, newFiles, piRuntime, retryId: retryIds.codex, replyRetryId: retryIds.pi });
    const passed = Object.values(checks).every(Boolean);
    const evidence = { run, names, nonce, failure, proof, checks, passed, exits: results.map(({ kind, code, signal, error }) => ({ kind, code, signal, error })),
      exact_retry_keys_deleted: deletedKeys, retry_message_ids: retryIds, db14_remaining: remaining,
      global_before: before, global_after: after, metadata_changes: changes, content_changes: contentChanges, new_workspace_files: newFiles };
    await writeFile(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
    console.log(JSON.stringify(evidence));
    assert(passed, 'Verification failed: ' + JSON.stringify(checks) + (failure ?? ''));
  } finally { await redis.quit(); }
}
