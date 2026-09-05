#!/usr/bin/env node
// Read-only by default. Continuity apply is an explicit local operator operation.
import { readFile, writeFile } from 'node:fs/promises';
import { Redis } from 'ioredis';
import { AgentDiagnostics } from '../dist/core/agent-diagnostics.js';
import { prepareContinuity, applyContinuity } from '../dist/core/mailbox-continuity.js';
import { CodexSocketClient } from '../dist/registered-shell/codex-socket.js';

const [command, ...args] = process.argv.slice(2);
const allowed = ['agent', 'find', 'delivery', 'connection', 'continuity-plan', 'continuity-apply'];
if (!allowed.includes(command)) {
  console.log('Usage: node scripts/gptqueue-doctor.mjs <agent|find|delivery|connection|continuity-plan|continuity-apply> --redis-url redis://host/db [--agent NAME] [--message-id ID] [--query TEXT] [--thread-id ID] [--client codex|pi --runtime-id ID --cwd PATH --legacy yes --out PLAN] [--plan PLAN --apply yes]');
  process.exit(command === '--help' ? 0 : 1);
}
const options = {};
for (let i = 0; i < args.length; i += 2) {
  if (!args[i]?.startsWith('--') || args[i + 1] === undefined || args[i] in options) throw new Error('Expected unique option/value pairs');
  options[args[i]] = args[i + 1];
}
const required = name => { if (!options[name]) throw new Error(`Required ${name}`); return options[name]; };
const redis = new Redis(required('--redis-url'));
const diagnostics = new AgentDiagnostics(redis);
try {
  let result;
  switch (command) {
    case 'agent': result = await diagnostics.details(required('--agent')); break;
    case 'find': result = await diagnostics.find({ query: options['--query'], working_directory: options['--cwd'] }); break;
    case 'delivery': result = await diagnostics.delivery(required('--agent'), required('--message-id')); break;
    case 'connection': {
      const rpc = new CodexSocketClient();
      try {
        const response = await rpc.request('mcpServer/tool/call', { threadId: required('--thread-id'),
          server: 'gptqueue-shared', tool: 'get_runtime_status', arguments: {} }, AbortSignal.timeout(10000));
        const status = response.structuredContent;
        const agent = status?.agent ?? options['--agent'];
        result = { probe: response.isError ? 'runtime_tool_unavailable' : 'runtime_tool_available',
          exact_thread_id: options['--thread-id'], identity_verified: typeof status?.agent === 'string',
          expected_identity_matches: options['--agent'] && status?.agent ? options['--agent'] === status.agent : null,
          activation_ready: status?.activation_ready === true,
          details: agent ? await diagnostics.details(agent) : null,
          action: response.isError ? 'Refresh this legacy MCP connection; review exact mailbox continuity first. Global reload is not performed.' : null };
      } finally { await rpc.close(); }
      break;
    }
    case 'continuity-plan': {
      result = await prepareContinuity(redis, { client: required('--client'), runtime_id: required('--runtime-id'),
        epoch: options['--epoch'] ?? required('--runtime-id'), working_directory: required('--cwd') },
      required('--agent'), { allowLegacy: options['--legacy'] === 'yes' });
      await writeFile(required('--out'), JSON.stringify(result, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
      break;
    }
    case 'continuity-apply':
      if (options['--apply'] !== 'yes') throw new Error('Operator approval required: --apply yes');
      result = { status: await applyContinuity(redis, JSON.parse(await readFile(required('--plan'), 'utf8'))) };
      break;
  }
  console.log(JSON.stringify(result, null, 2));
} finally { await redis.quit(); }
