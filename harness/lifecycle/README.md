# GPTQueue communication-lifecycle harness runners (Node 18, sandbox-confined)

This directory holds the deterministic scenario runners cited as evidence
actions by `harness/metabuilder/communication-lifecycle-v1/MODULE.json`.

Execution context (verified against the MetaBuilder sandboxed-command
profile): no network (TCP loopback unavailable), read-only target workspace,
writable tmpfs `/tmp`, fixed environment, `/outputs` for declared files only.

Consequences encoded here:

- The isolated GPTQueue HTTP server is spawned as a child process listening on
  a Unix domain socket under `/tmp` (opt-in `GPTQUEUE_HTTP_SOCKET`), with an
  in-sandbox `redis-server` (`/usr/bin/redis-server --unixsocket ... --port 0`)
  reached through `REDIS_URL=/tmp/...sock` (ioredis socket-path support).
- MCP clients use `node:http` with `socketPath` (never `fetch`, whose undici
  WASM cannot instantiate under the sandbox address-space limit).
- Every scenario is bounded: deterministic actor names, per-edge unique
  idempotency keys, deadline polling instead of sleeps, and exit 0/1 with a
  single-line JSON result summary on stdout.

Scenario entry point: `run-scenario.mjs <topology|idempotency|continuity|cleanup|backpressure> --rounds N`.
