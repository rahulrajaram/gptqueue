# Experimental pre-registered CLI wrapper

This repository-local experiment launches Codex or Pi with a GPTQueue identity
that exists **before the coding agent starts**. It does not modify the installed
`codex` or `pi` wrappers, their global MCP configuration, or the live GPTQueue
HTTP server on port 8101.

## Lifecycle

1. `gptqueue-experiment` atomically claims the requested name, then registers
   it directly against an explicitly non-db0 Redis database. Every mode
   refuses an active same-name agent; non-destructive `close` mode may reuse
   an offline registry identity and its retained mailbox. The wrapper verifies
   that its new session is still the only active session before launching and
   atomically re-verifies that ownership in the same Redis transaction that
   removes a disposable session and mailbox. If another session appeared, the
   transaction closes only the wrapper's session and preserves shared state.
2. It starts an authenticated MCP bridge on an ephemeral loopback port. The
   bridge owns the registered GPTQueue session; the app-level `session_id`
   never enters the agent prompt, command line, or environment. Ambient
   `REDIS_URL` and live-server token variables are also removed from the child
   environment; the CLI receives only the per-launch bridge URL and token.
3. Codex receives a per-launch MCP configuration override. Pi receives a
   one-run local extension backed by the already-installed `pi-mcp-adapter`.
4. Only messaging and inspection tools are exposed. `register_agent` and
   `unregister_agent` are deliberately absent from the model-visible catalog.
5. When the CLI exits, the bridge stops and the wrapper closes the session.
   `--cleanup unregister` additionally deletes the named test mailbox and is
   intended only for disposable test identities. That mode also requires the
   registry name to be fresh, so it cannot adopt and later delete an existing
   offline identity.

Bridge teardown force-closes lingering HTTP connections after a short grace
period. Overall cleanup has a 10-second budget; if Redis stops responding, the
wrapper breaks its local connections and exits nonzero while leaving the
identity claim fail-closed for operator inspection. A second termination
signal also forces those connections closed.

Both child integrations are isolated:

- Codex runs with `--ignore-user-config` and `--ephemeral`; its normal auth is
  still read by Codex, but global MCP servers and saved sessions are not used.
- Pi runs headlessly with ambient extension discovery, built-in tools, skills,
  prompts, and context-file discovery disabled. The experiment explicitly
  loads the locally installed `pi-mcp-adapter` through an in-memory MCP config.
  Its adapter metadata cache, generated extension, and session directory are
  redirected to a per-agent subdirectory beneath the experiment workspace;
  no package is downloaded or installed and global Pi MCP state is not changed.

## Build and run

Create a disposable workspace inside the repository and build first:

```sh
mkdir -p .gptqueue/registered-wrapper-workspace
npm run build
```

Codex example:

```sh
node bin/gptqueue-experiment codex \
  --agent gptqueue-experiment-codex \
  --workspace .gptqueue/registered-wrapper-workspace \
  -- 'Call list_agents and report whether gptqueue-experiment-codex is present.'
```

Pi example:

```sh
node bin/gptqueue-experiment pi \
  --agent gptqueue-experiment-pi \
  --workspace .gptqueue/registered-wrapper-workspace \
  -- 'Call list_agents and report whether gptqueue-experiment-pi is present.'
```

The wrapper defaults to Redis db14 so a live experiment does not collide with
this repository's db15 test cleanup, and it refuses db0 even when explicitly
requested. The `REDIS_URL` environment variable is not used as an implicit
override; pass `--redis-url` when selecting another isolated database. The
destructive `--cleanup unregister` mode additionally requires a fresh name
starting with `gptqueue-experiment-`; the wrapper refuses a pre-existing name.
All modes refuse a name already claimed by another wrapper or backed by an
active GPTQueue session/heartbeat. Claims fail closed:
an uncatchable process kill in the tiny pre-registration window can leave a
claim key in the selected isolated database, which an operator must inspect
before removing. The bridge also limits `receive_message` to 1-60 seconds so a
client cannot pin the wrapper's Redis subscriber with an unbounded receive.

## Pi adapter provenance

The experiment uses the existing `pi-mcp-adapter` installation only. On this
machine it was installed from npm as version 2.21.1, with its resolved tarball
and integrity recorded in `~/.pi/agent/npm/package-lock.json`; its package
manifest identifies `https://github.com/nicobailon/pi-mcp-adapter` and the MIT
license. Override the inspected local entrypoint with
`GPTQ_PI_MCP_ADAPTER_ENTRY` when necessary. The wrapper never invokes
`pi install`, `npm install`, or any discovery/install lane.
