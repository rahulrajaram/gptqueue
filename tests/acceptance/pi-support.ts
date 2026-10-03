import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { SESSION_KEYS } from '../../src/core/keys.js';

const installed = nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist");
export const root = resolve(import.meta.dirname, '../..');
export const runRoot = resolve(root, '.gptqueue/acceptance/20260912-evaluation');
export const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379/15';

export async function until<T>(read: () => Promise<T>, accept: (value: T) => boolean, timeout = 60_000): Promise<T> {
  const end = Date.now() + timeout;
  do { const value = await read(); if (accept(value)) return value; await delay(250); } while (Date.now() < end);
  throw new Error(`Acceptance observation deadline exceeded (${timeout}ms)`);
}

export { sanitizeEvidence as publicEvidence } from './public-evidence.js';
import { sanitizeEvidence as publicEvidence } from './public-evidence.js';
import { nodePrefixPath } from "./local-tools.js";

export async function startPiPeer(cwd: string, instructions?: string) {
  await mkdir(cwd, { recursive: true });
  const extension = join(cwd, 'gptqueue-evaluation.ts');
  await writeFile(extension,
    `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(root, 'dist/registered-shell/pi-extension.js')).href)};\n` +
    `export default createRegisteredPiExtension(${JSON.stringify({ redisUrl, nodePath: process.execPath, sidecarPath: join(root, 'bin/gptqueue-session') })});\n`, { mode: 0o600 });
  const { RpcClient } = await import(pathToFileURL(join(installed, 'modes/rpc/rpc-client.js')).href);
  const client = new RpcClient({ cliPath: join(installed, 'cli.js'), cwd,
    env: { PI_OFFLINE: '1', GPTQ_LOG_DIR: join(cwd, 'lifecycle') },
    args: ['--offline', '--no-extensions', '--extension', extension, '--no-skills', '--no-prompt-templates',
      '--no-themes', '--no-context-files', '--no-builtin-tools', '--session-dir', join(cwd, 'sessions'),
      ...(instructions ? ['--append-system-prompt', instructions] : [])] });
  const events: unknown[] = [];
  client.onEvent((event: unknown) => events.push(publicEvidence(event)));
  const redis = new Redis(redisUrl);
  try {
    await client.start();
    const agent = await until(async () => {
      const registry = await redis.hgetall(SESSION_KEYS.registry);
      return Object.entries(registry).find(([, raw]) => {
        try { return JSON.parse(raw).metadata?.working_directory === cwd; } catch { return false; }
      })?.[0];
    }, value => typeof value === 'string');
    if (!agent) throw new Error('Pi registration unavailable');
    const state = await client.getState();
    const binding = JSON.parse(await redis.get(`gptq:runtime-binding:${agent}`) ?? 'null');
    return {
      route: 'pi-rpc-cli', agent, client, events, state, binding,
      prompt: (text: string) => client.promptAndWait(text, undefined, 300_000),
      messages: () => client.getMessages(),
      close: async () => { await client.stop(); await redis.quit(); },
    };
  } catch (error) { await client.stop(); await redis.quit(); throw error; }
}
