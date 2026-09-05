# Experimental pre-registered CLI wrapper

This repository-local experiment launches Codex or Pi with a GPTQueue identity
that exists **before the coding agent starts**. It supplies per-launch MCP
configuration and uses the installed `codex` and `pi` CLIs. Their own startup
code can still write global runtime state; configuration-content and cache
isolation are measured by the verifier below. The experiment does not restart
the live GPTQueue HTTP server on port 8101.

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
4. The bridge exposes exactly `send_message`, `receive_message`, `list_agents`,
   and `get_queue_status`. Pi disables adapter scripting and filters tool
   registration/activation, including the adapter's fallback proxy. Before
   inference it waits for discovery and checks the actual active catalog;
   a missing or unexpected tool terminates the child. These are restrictions
   on the model's tools, not an operating-system sandbox for trusted extensions.
5. When the CLI exits, the bridge stops and the wrapper closes the session.
   `--cleanup unregister` additionally deletes the named test mailbox and is
   intended only for disposable test identities. That mode also requires the
   registry name to be fresh, so it cannot adopt and later delete an existing
   offline identity.

Bridge teardown force-closes lingering HTTP connections after a short grace
period. After child termination, Redis/bridge cleanup has a 10-second budget;
expiry forces the wrapper's Redis connections closed and produces a nonzero
exit while preserving the identity claim for inspection. Child termination
can add up to six seconds. A second signal kills a running child, or forces
cleanup connections closed when the child has already exited. Initial Redis
claim acquisition is not covered by this cleanup deadline.

Child configuration and state scope:

- Codex runs with `--ignore-user-config` and `--ephemeral`; its normal auth is
  still read by Codex, but global MCP servers and saved sessions are not used.
- Pi runs headlessly with ambient extension discovery, built-in tools, skills,
  prompts, and context-file discovery disabled. The experiment explicitly
  loads the locally installed `pi-mcp-adapter` through an in-memory MCP config.
  Its adapter metadata cache, generated extension, and session directory are
  redirected to a per-agent subdirectory beneath the experiment workspace;
  no package is downloaded or installed. The verifier checks the global Pi
  cache's hash and mtime and configuration-file hashes. Codex's config mtime
  has changed during observed launches despite identical file contents.
  Global logs, telemetry, authentication files, and other caches are outside
  this verification claim.

## Receive cancellation and delivery

Every bridge receive owns a separate blocking Redis connection. MCP request
cancellation, transport-session termination, or bridge shutdown closes that
connection without interrupting other receives or registration heartbeats.
The cancelled request does not issue further pops or reconnect/replay a pop.

Delivery remains **at most once**: a Redis pop that has already executed when
cancellation or a network failure races its response may have consumed the
message without the caller receiving it. The wrapper does not requeue an
uncertain result or claim acknowledgement-based delivery guarantees.

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
an uncatchable process kill at any point while a claim is held can leave a
claim key in the selected isolated database, which an operator must inspect
before removing. The bridge limits `receive_message` to 1-60 seconds.
If the claim disappears or is replaced during shutdown, the wrapper exits
nonzero and closes its connections without deleting state it no longer owns.

## Repeatable verification

Run the focused regression suite on db15 through Overwatch:

```sh
npm run build
overwatch run --profile npm_test --stream -- \
  env REDIS_URL=redis://127.0.0.1:6379/15 "$(command -v node)" \
  node_modules/vitest/vitest.mjs run tests/experimental-wrapper*.test.ts
```

The explicit Node path uses your shell's runtime even if the Overwatch daemon
has an older Node on its PATH.

The suite covers cancellation followed by delayed delivery, concurrent receive
isolation, transport deletion, bridge shutdown, the Pi tool filter and startup
gate, forged session IDs, explicitly seeded private environment variables, and
connection cleanup after identity-claim loss.

With permission to use the installed CLIs/model providers and disposable db14
identities, run the Linux-local live verifier:

```sh
node scripts/verify-registered-wrapper.mjs --live
```

It requires db14 to start empty, gates both real CLI launches on registration
and loopback/catalog checks, then verifies a correlated Codex-to-Pi-to-Codex
exchange and Pi's active tools. It uses the installed default models, checks
global configuration hashes and Pi cache metadata, and checks that new Pi
files remain in the expected runtime directory. Both sends use explicit retry
keys; the verifier checks their message IDs and deletes only those two exact
keys. It requires db14 to return to empty and never flushes it.
The verification shim adds Pi's JSON output mode for tool-call evidence.

Evidence and transcripts are saved under `.gptqueue/wrapper-verification/`.
Configuration-content changes, Pi cache metadata changes, unexpected tools,
bad correlation, nonzero wrapper exits, or Redis residue fail verification.
Codex configuration mtime changes with identical contents are recorded without
failing. The verifier bounds live runs to four minutes plus termination time;
forced termination can leave fail-closed identity residue requiring inspection.

## Pi adapter provenance

The experiment uses the existing `pi-mcp-adapter` installation only. On this
machine it was installed from npm as version 2.21.1, with its resolved tarball
and integrity recorded in `~/.pi/agent/npm/package-lock.json`; its package
manifest identifies `https://github.com/nicobailon/pi-mcp-adapter` and the MIT
license. Override the inspected local entrypoint with
`GPTQ_PI_MCP_ADAPTER_ENTRY` when necessary. The wrapper never invokes
`pi install`, `npm install`, or any discovery/install lane.
