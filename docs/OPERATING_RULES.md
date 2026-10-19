# GPTQueue operating rules

The contract a consumer of this server operates under. These are behavior
guarantees and obligations, not suggestions; each is enforced or measured
by tests in this repository.

## 1. Delivery is at-least-once on the claim path

`claim_tasks` -> process -> `acknowledge_tasks` is the durable path. A task
is redelivered whenever its claim expires without acknowledgement. Your
handlers must therefore be idempotent or tolerate duplicate processing —
this is YOUR obligation, not the server's.

Producer-side dedup is a different thing: `send_message` with an
`idempotency_key` prevents the same sender creating duplicate *tasks* on
retry (bounded to 24 hours). It does not make your handler idempotent.

## 2. Two consumption paths exist; do not mix them on one inbox

- `receive_message` — **deprecated** (decision D3). Destructive BLPOP,
  at-most-once: a message popped by a session that then fails is lost. It
  still works for plain agents, but every runtime should consume with
  `claim_tasks`/`acknowledge_tasks` instead.
- `claim_tasks`/`acknowledge_tasks` — at-least-once, for durable actors.

Durable actors (agents with an actor-directory record) are rejected from
`receive_message` at the tool boundary. Never build a runtime that uses
both paths against the same inbox.

## 3. Recovery semantics

- A claim expires at `ttl_seconds` (default 300, max 3600) after which the
  next claim on that inbox lazily recovers its unacknowledged tasks to the
  tail.
- The original `claim_id` is invalidated on recovery: a late ack from the
  slow worker fails with `unknown_claim`.
- Renewed claims stay live until the lifetime budget
  (`CLAIM_LIFETIME_BUDGET_SECONDS`, provisional 24h) is exhausted, after
  which recovery treats them like any expired claim.

## 4. Dead-lettering

A task recovered more than `RECOVER_CAP` times (provisional 5) without an
ack is moved to the actor's DLQ instead of the inbox. DLQ entries are
visible with `dlq_status` and re-queueable with `dlq_requeue` (fresh
recovery budget). The DLQ is bounded (`DLQ_MAX_LENGTH`, provisional 1000;
oldest dropped). Dead-lettered work is NOT automatically retried — an
operator (you) decides.

## 5. Ordering

Recovery re-enters tasks at the tail of the inbox. Per-claim task order is
preserved; absolute ordering across recoveries is not guaranteed. If your
workflow depends on strict FIFO across failures, sequence explicitly with
correlation ids in payloads.

## 6. Identity

- A durable actor's identity IS its registered agent name — `actor_register`
  derives it from the calling session. Divergent identities cannot be
  created.
- Sends to names that are neither registered agents nor durable actors are
  rejected with `unknown_recipient`; no queue keys are created.
- A session_id is the bearer credential for its agent. Protect it; anyone
  holding it can act as that agent (single-user deployment trust model).

## 7. Wake (offline activation)

- The message is persisted BEFORE any wake is attempted: a failed or lost
  launch never loses accepted work.
- One wake lease per actor: concurrent sends coalesce onto a single launch.
- Launch contracts are governed by the operator allowlist
  (`~/.config/gptqueue/launch-allowlist.json` or `GPTQUEUE_LAUNCH_ALLOWLIST`,
  v2 exact argv templates); dispatch
  re-checks it fail-closed. Shells and interpreter inline-code flags are
  rejected regardless of the allowlist.
- A launched runtime proves itself by registering under the actor's name —
  that clears the wake lease (`runtime_ready`). A lease whose spawned pid
  dies is reconciled back to offline at the next presence read.
- Wake leases are 60s (provisional): a runtime that has not registered
  within that window returns the actor to offline and the next send
  re-dispatches.

## 8. Custody is orthogonal to messaging

Custody records govern worktree ownership between sessions; they do not
gate message delivery (that routing is a tracked future decision, not a
current behavior). Take custody before working a shared tree; a dead
custodian's claim is forfeited on lease expiry and takeover requires an
explicit inventory.

## 9. Provisional constants (calibrate from live data, not by editing casually)

| Constant | Value | Meaning |
|---|---|---|
| `RECOVER_CAP` | 5 | recoveries before dead-letter |
| `DLQ_MAX_LENGTH` | 1000 | per-actor DLQ bound (oldest dropped) |
| `RECOVER_COUNTER_TTL_SECONDS` | 604800 | orphan counter lifetime (7d) |
| `CLAIM_LIFETIME_BUDGET_SECONDS` | 86400 | max total claim lifetime (24h) |
| wake lease | 60 | offline-activation window |
| claim TTL default | 300 | per-claim lease, max 3600 |

## 10. Testing etiquette

The live server uses the production Redis (TCP `127.0.0.1:6379`, db0). Tests
never connect to it, or to any TCP Redis.

### A private Redis per run

Every vitest run starts its OWN `redis-server` in `globalSetup`
(`tests/helpers/global-setup.ts`, implemented once in
`scripts/lib/owned-redis.mjs`):

- It listens only on a unix socket, `<run dir>/redis.sock`, inside a fresh
  0700 directory under `$TMPDIR` (`--port 0`: no TCP listener at all), and
  persists nothing (`--save ''`, `--appendonly no`).
- Before anything is exported, it is verified private over the socket:
  `CONFIG GET port` must be `0`, `CONFIG GET unixsocket` must be this run's
  socket, and `INFO server` `process_id` must be the spawned pid. Any
  mismatch stops the server and fails the run.
- Workers and every child they spawn get
  `REDIS_URL=<run dir>/redis.sock?db=15`. ioredis and the gptqueue entry
  points accept that form directly. Any `REDIS_URL` the caller set is
  replaced, never used.
- Teardown stops it (SIGTERM, then SIGKILL after a bound) and removes the
  run directory.

Tests read the connection only through `testRedisUrl()`
(`tests/helpers/test-redis.ts`). It accepts nothing but that private socket
form, in a 0700 run directory owned by this user, and refuses every TCP URL,
including `redis://127.0.0.1:6379/15`. There is no fallback. The shared flush
helper (`flushTestKeys`) also checks that the client itself is connected to
that socket and database, and deletes only `gptq:*` keys (never FLUSHDB).

### Concurrency

Runs share nothing: each has its own Redis instance and socket, and the
integration HTTP server binds an OS-assigned loopback port (`--port 0`; the
bound port is read from its startup line). Any number of worktrees, or
several vitest processes in one worktree, can run at the same time.

### Running tests

```sh
export TMPDIR=/path/to/short/disk-backed/tmp   # optional; keep it short
node node_modules/vitest/vitest.mjs run tests/<file>.test.ts
```

There is no Redis URL to pass. The socket path must fit in 107 bytes, so keep
`TMPDIR` short; startup fails clearly otherwise.

Overrides:

- `GPTQ_TEST_REDIS_SERVER`: the `redis-server` binary (default
  `/usr/bin/redis-server`).
- `TMPDIR`: where run directories (`gptq-redis-XXXXXX`) are created.

Verify scripts (`scripts/verify-*.mjs`), the release installer
(`scripts/install-runtime.mjs`) and acceptance fixtures (`startOwnedRedis`)
start their own instance through the same helper.
`node scripts/with-owned-redis.mjs -- <command>` runs a non-vitest command
against one.

### Crash safety

Each run directory records its owner (pid, process start time, pid namespace)
and the server pid in `owner.json`. A process that exits without teardown
still kills its server on exit. A server orphaned by a killed owner is reaped
when the next instance starts under the same `$TMPDIR`. It is killed only if
the owner is provably gone and `/proc/<pid>/cmdline` shows a `redis-server`
serving exactly that directory's socket. A directory from another pid
namespace (for example a run inside a separate PID-namespace sandbox, which
reaps its own processes) is removed only after nothing has answered on its socket for 60 s.

## Agent name ownership (decision D2)

Agent names are exclusive by default. `register_agent` (and a rename onto
another name) fails with error code `AGENT_NAME_HELD`, details `name`,
`holder_session_tag` (a non-reversible tag: the holder's session id is a
bearer credential and is never disclosed), `holder_liveness_verifiable` and
`holder_lease_ttl_ms`, while a live session holds the name. A session is live
while its lease is unexpired and, when it recorded a pid on this host, that
pid is alive. A dead holder is taken over atomically and audited to the
`gptq:identity-audit` stream; its mailbox, claims and history are kept.
Sharing is opt-in: pass `shared: true` on every registrant (a shared holder
never admits a non-shared claimant, or the reverse), or set
`GPTQ_AGENT_SHARED=1` as the default for processes that cannot pass the
flag. Reconnecting by `session_id` is unaffected.
