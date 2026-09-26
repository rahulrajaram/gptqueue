#!/usr/bin/env node
// Read-only by default. Continuity apply is an explicit local operator operation.
import { readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { AgentDiagnostics } from '../dist/core/agent-diagnostics.js';
import { prepareContinuity, applyContinuity } from '../dist/core/mailbox-continuity.js';
import { probeActivationReady, bindingAgrees } from '../dist/core/doctor-probe.js';
import { CodexSocketClient } from '../dist/registered-shell/codex-socket.js';

const [command, ...args] = process.argv.slice(2);
const allowed = ['agent', 'find', 'delivery', 'connection', 'config', 'continuity-plan', 'continuity-apply'];
if (!allowed.includes(command)) {
  console.log('Usage: node scripts/gptqueue-doctor.mjs <agent|find|delivery|connection|config|continuity-plan|continuity-apply> --redis-url redis://host/db [--agent NAME] [--message-id ID] [--query TEXT] [--thread-id ID] [--client codex|pi --runtime-id ID --cwd PATH --legacy yes --out PLAN] [--plan PLAN --apply yes] [--tier base|sandbox-auto|sandbox-max]');
  process.exit(command === '--help' ? 0 : 1);
}
const options = {};
for (let i = 0; i < args.length; i += 2) {
  if (!args[i]?.startsWith('--') || args[i + 1] === undefined || args[i] in options) throw new Error('Expected unique option/value pairs');
  options[args[i]] = args[i + 1];
}
const required = name => { if (!options[name]) throw new Error(`Required ${name}`); return options[name]; };
// `config` lints the local Codex capability baseline only; no Redis involved.
const configOnly = command === 'config';
const redis = configOnly ? null : new Redis(required('--redis-url'));
const diagnostics = configOnly ? null : new AgentDiagnostics(redis);
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
        // D7: read the recorded runtime binding (if any) so readiness uses
        // the same identity/binding conjunction as the MCP probe.
        let binding = null;
        if (agent) {
          try {
            const raw = await redis.get(`gptq:runtime-binding:${agent}`);
            if (raw) {
              const parsed = JSON.parse(raw);
              if (typeof parsed?.runtime_id === 'string' && typeof parsed?.epoch === 'string') {
                binding = { runtime_id: parsed.runtime_id, epoch: parsed.epoch };
              }
            }
          } catch { binding = null; }
        }
        result = { probe: response.isError ? 'runtime_tool_unavailable' : 'runtime_tool_available',
          exact_thread_id: options['--thread-id'], identity_verified: typeof status?.agent === 'string',
          expected_identity_matches: options['--agent'] && status?.agent ? options['--agent'] === status.agent : null,
          binding_matches: bindingAgrees(status, binding),
          activation_ready: probeActivationReady(response.isError, status, options['--agent'], binding),
          details: agent ? await diagnostics.details(agent) : null,
          action: response.isError ? 'Refresh this legacy MCP connection; review exact mailbox continuity first. Global reload is not performed.' : null };
      } finally { await rpc.close(); }
      break;
    }
    case 'config': {
      // Static lint of the agent-execution baseline (docs/AGENT_EXECUTION_BASELINE.md).
      // Behavioral proof lives in the acceptance matrix (GPTQUEUE_CODEX_APPROVAL=1).
      const home = process.env.CODEX_HOME || join(homedir(), '.codex');
      const tier = options['--tier'] ?? 'base';
      const text = await readFile(join(home, 'config.toml'), 'utf8');
      const checks = [
        { key: 'sandbox_mode="workspace-write"', ok: /^sandbox_mode\s*=\s*"workspace-write"/mu.test(text) },
        { key: '[sandbox_workspace_write] network_access=true',
          ok: /^\[sandbox_workspace_write\]/mu.test(text) && /^network_access\s*=\s*true/mu.test(text) },
        { key: 'mcp_servers."gptqueue-shared".default_tools_approval_mode="approve"',
          ok: /^\[mcp_servers\."gptqueue-shared"\]/mu.test(text) && /^default_tools_approval_mode\s*=\s*"approve"/mu.test(text) },
      ];
      if (tier !== 'base') {
        const profile = await readFile(join(home, `${tier}.config.toml`), 'utf8');
        checks.push({ key: `${tier}: approval_policy="never"`, ok: /^approval_policy\s*=\s*"never"/mu.test(profile) });
        if (tier === 'sandbox-max') checks.push({ key: `${tier}: sandbox_mode="danger-full-access"`, ok: /^sandbox_mode\s*=\s*"danger-full-access"/mu.test(profile) });
      }
      const failed = checks.filter(check => !check.ok);
      result = { tier, codex_home: home, status: failed.length ? 'baseline_not_met' : 'ok', checks,
        action: failed.length ? 'Apply the missing keys per docs/AGENT_EXECUTION_BASELINE.md. Approval keys are operator-only; installers never write them.' : null };
      if (failed.length) process.exitCode = 1;
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
} finally { if (redis) await redis.quit(); }
