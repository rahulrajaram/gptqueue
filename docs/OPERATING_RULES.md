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

- `receive_message` — destructive BLPOP, at-most-once, for plain agents
  (notifications where losing one message is acceptable).
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
  (`.gptqueue/launch-allowlist.json`); dispatch re-checks it fail-closed.
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

The live server uses Redis db0. Every test suite runs against db15
(`REDIS_URL=redis://127.0.0.1:6379/15`) and the shared flush helper refuses
db0 unless `GPTQUEUE_ALLOW_DB0=1`. Never flush db0.
