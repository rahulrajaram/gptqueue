# Inbox capacity and waking admission

Each agent inbox is one Redis list, `gptq:q:<name>`, of raw message envelopes.
Its `max_size` comes from `GPTQ_QUEUE_BOUND` (default 10) and is reported by
`get_queue_status`.

## Two classes of mail

Every queued entry, and every incoming send, is classified by the same rule
the inbox dispatcher uses to decide whether mail wakes its recipient
(`wakesRecipient` in `src/core/inbox-events.ts`, with its Lua twin in
`src/mcp-server/lua/waking-admission-lib.lua`):

- **Waking:** a `task`, or a `result`/`error` addressed to the inbox whose
  `payload.in_reply_to` names a request the inbox's owner still has
  outstanding, sent by that request's expected replier
  (`gptq:outstanding:<inbox>:<in_reply_to>` equals the sender).
- **Quiet:** everything else: `status`, `ping`, unowned or stale replies
  (the outstanding key expired or names someone else), mail addressed to
  another name, and malformed entries that still decode as JSON (not an
  object, or missing a string `id` or a `payload`).
- **Undecodable entries count as waking.** The admission check decodes with
  Redis's cjson, which rejects some text `JSON.parse` accepts (an escaped
  unpaired surrogate, nesting past its depth limit). Consumers may wake for
  such an entry, so admission counts every entry it cannot decode as waking
  and never undercounts the mail consumers act on. No gptqueue writer
  produces one, since every ordinary send cjson-decodes its envelope; the cost
  is that undecodable junk written into an inbox by hand occupies waking
  capacity.

Classification is evaluated at admission time over the whole current queue,
so a reply that was waking when it arrived becomes quiet once its outstanding
key expires or changes. Raw envelopes are never modified.

A reply wakes only when its `from` and `payload.in_reply_to` are both
non-empty strings, in the Lua classifier and in `wakesRecipient` alike, so a
non-string `in_reply_to` (for example a number, which only a raw writer
could store) or a null/missing/non-string `from` is quiet. `send_message` only
produces strings: its schema declares `in_reply_to: z.string().optional()`
(`src/mcp-server/tools/send-message.ts:36-39`).

## Admission of ordinary sends

`max_size` is the ordinary-send quota for each class:

| Incoming send | Admitted when | Refusal reason |
|---|---|---|
| Waking | fewer than `max_size` waking entries are queued, whatever the total length | `waking_full` |
| Quiet | the queue holds fewer than `max_size` entries (the rule before waking admission) | `full` |
| Any, when the queue holds more than 1024 entries | never | `oversize` |

Quiet mail therefore never refuses waking mail. While classification is
stable, ordinary sends bound an inbox at about `2 x max_size` entries. That
figure is not a general bound: when an owned reply's outstanding key expires
or changes owner, the reply turns quiet and frees a waking slot, so ownership
changes alone can let ordinary sends grow an inbox beyond `2 x max_size`,
until it passes the 1024-entry ceiling below. The ceiling is a pre-send
threshold: a send that finds exactly 1024 entries is still evaluated, so an
inbox can reach 1025. Admission, the classification scan
and the enqueue are one atomic Lua script. A refused send writes nothing: no
queue entry, metadata, inbox event, task ledger, answered marker,
outstanding key or idempotency record. An idempotent retry of a send that was
already delivered returns the original message id before any capacity check.

`send_message` retries a refused send with exponential backoff (up to 10
attempts) and then fails with `QUEUE_FULL`, `retryable: true`, and the last
attempt's `reason`. The watchdog's reminder send uses the same admission
(a reminder is a task) and reports the reason in its non-delivery error.

## Lossless writers may overshoot

These paths move mail that was already accepted and never refuse it for
capacity, so an inbox can exceed `2 x max_size`: claim recovery and requeue,
DLQ requeue, provisional-mailbox forwarding, rename migration, and the
provisional sweeper's retirement replies. Later admission counts the actual
contents, including any overshoot.

## Operational ceiling

An inbox with more than 1024 entries refuses every ordinary send with reason
`oversize` before any scan, so admission cost stays bounded. Drain it (claim
and acknowledge, or move entries to the DLQ) to restore ordinary admission.

## Status

`get_queue_status` reports `depth`, `max_size`, `waking_count`,
`quiet_count`, `dead_lettered_count` and `dead_lettered_dropped`. The waking
and quiet counts use the admission classification and are read in the same
pipeline as `depth`, not the same transaction.

`dead_lettered_count` is the mailbox's current dead-letter queue length (LLEN
of `gptq:dlq:<agent>`): tasks that lazy claim recovery quarantined after
repeated claim expiry, or mail dead-lettered for a retired recipient.
`dead_lettered_dropped` is the DLQ drop counter (`gptq:dlq-dropped:<agent>`):
entries the per-mailbox DLQ bound (`DLQ_MAX_LENGTH`, 1000) has discarded, which
are gone for good; it is `null` when the stored counter is unreadable. Both
are plain reads in the same pipeline; a missing key reads as 0 and no key is
created. A non-zero `dead_lettered_count` means work is stuck: the mailbox's
owner inspects it with `dlq_status` and re-queues entries with `dlq_requeue`
(the sidecar exposes both too, after the parity release). Observers see the
counts only, never the entries.

## Mixed versions

Quiet admission is unchanged, so an older writer is never stricter than
before. An older writer still refuses waking mail when the total length
reaches `max_size`; the protection holds only for sends through a writer that
includes waking admission.
