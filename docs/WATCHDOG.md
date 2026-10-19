# gptqueue watchdog

`gptqueue-watchdog --once [--config PATH] [--dry-run] [--json] [--redis-url URL]` runs one pass over the
configured leads. Scheduling it (a timer) is an operator decision and is not installed by gptqueue.

- **Journal output:** a quiet pass (no actions, errors, deferred or rate-limited work) prints one line:
  `gptqueue watchdog: ok at=<iso> leads=N waiting=W stalled=S idle=I uncorrelated=U`. Any other pass
  prints the full JSON report. `--json` always prints the full report.
- **Status:** every real invocation records `gptq:watchdog:last-pass`, including passes skipped because
  another pass held the lock (`skipped: true`). Every real pass that was not skipped also records
  `gptq:watchdog:last-substantive-pass` as `{at, exit, leads, actions, failed, rate_limited, deferred,
  uncorrelated}`, plus `error` when the pass failed. Both keys have a 7-day TTL, and dry runs record
  neither. `gptqueue-watchdog --status [--redis-url URL]` judges only the substantive record:
  - `healthy` (exit 0): under 3 minutes old, exit 0, and `failed` = 0;
  - `degraded` (exit 1): it had failed actions;
  - `unhealthy` (exit 1): it is missing, stale, unreadable or exited non-zero.
  It prints `{state, healthy, reason, last_pass, last_skip}`, so a skip never masks a failed pass. A pass
  that ends at its deadline or on a Redis error is recorded when Redis is still reachable. So is a config failure (exit 2,
  `error: config_missing` or `config_invalid`), written only after the pass has decided to exit, so a
  broken config cannot leave an old healthy status in place.
- **Unit environment:** the timer unit sets `Environment=XDG_STATE_HOME=<state-dir>`, so the
  watchdog's files land in `<state-dir>/gptqueue/watchdog/`. Without it the XDG default
  applies (`~/.local/state/gptqueue/watchdog/`).
- **Rotation and retention:** before each append, `escalations.jsonl` is renamed to
  `escalations-<UTC date>.jsonl` (with `-1`, `-2`, ... on collision) when it has reached 50 MB, or when
  its first line is 30 days old. Each real pass deletes rotated logs and `outbox/*.eml` files whose
  modification time is more than 30 days old.
- **Per-recipient rate:** at most `max_reminders_per_recipient_per_hour` reminders (default 6, allowed
  1 to 60) are enqueued for one recipient in any rolling hour, measured by Redis's own clock (`TIME`).
  - **One atomic script:** rate admission and the enqueue happen in the same Lua script, composed from
    the normal idempotent send (`bounded-push-idempotent.lua`). It checks, in order:
    1. the idempotency key: a duplicate changes nothing and adds no rate entry;
    2. the forwarding tombstone;
    3. waking admission (a reminder is a task, so it is refused only with `waking_full` or
       `oversize`; see [QUEUE-CAPACITY.md](QUEUE-CAPACITY.md)): a refusal changes nothing;
    4. the rate set `gptq:watchdog:sent:<recipient>`: entries older than one hour by Redis TIME are
       trimmed, and at the cap the reminder is refused (`limited`) with nothing changed.

    Only then does it enqueue, with the same queue entry, inbox event, outstanding key, task ledger and
    idempotency record as a normal task send, and record the fire key at Redis TIME (2-hour TTL). No
    caller clock, slow pass or stalled connection can move the hour.
  - **Outcomes:**
    - **`limited`:** reported as `rate_limited`; the claim is released, and a later pass retries.
    - **Full mailbox:** a definite non-delivery; the claim is released.
    - **Thrown or uncertain send:** keeps the pending claim. The retry's idempotency key returns
      `duplicate` if the first attempt was enqueued, so there is no second entry and no second count.
  - **Planning:** planning reads the same set (read-only) to keep over-cap reminders from using
    `max_actions_per_run` slots. The script is the authority. Escalations are not capped.
- **Recognisable texts:** every watchdog-authored text (reminders, escalations, alert bodies) starts
  with `[gptqueue watchdog] ` and comes from the reserved sender `gptqueue-system`. Agents cannot
  register that name, and the send tool takes the sender from the bound session.

- **Config:** `$XDG_CONFIG_HOME/gptqueue/watchdog.json` (default `~/.config/gptqueue/watchdog.json`):
  `schema_version`, `root`, `leads`, `operator`, `thresholds_min` (`waiting`, `escalate_after`, `idle`),
  `max_actions_per_run`, optional `max_reminders_per_recipient_per_hour`.
- **Exit codes:** 0 success (including a pass skipped because another holds the pass lock), 2 usage or
  config problem, 1 Redis unavailable or pass deadline.
- **Output (full report):** `{ at, leads: [{ lead, state, sw4rm_state }], actions: [{ kind, rule, to, fire_key, text, status }],
  deferred, uncorrelated, rate_limited, dry_run, operator_alerts }`.
  - `uncorrelated` counts outstanding requests with no task-ledger record (sent before the ledger existed).
    They cannot be matched to replies, so they never make a lead `waiting` or `stalled`.
- **The escalation log is at-least-once.** Every line carries `fire_key`, and consumers must dedupe by it.
  A retried escalation skips its append when the file's last line already has the same `fire_key`, but
  that guard covers only the common retry. The operator alert sink records the alert ids it has emitted
  for 7 days and never emits one twice, alone or in a digest.
- **Escalations** are appended to `$XDG_STATE_HOME/gptqueue/watchdog/escalations.jsonl`. They also go to the
  operator mailbox when one is configured, and to the operator alert sink
  (`$XDG_CONFIG_HOME/gptqueue/operator-alerts.json`, mode 0600, dry run only).

## Limits

- **One turn state per agent name.** The turn state is a single record per agent name, and the newest
  hook report wins. Names are exclusive by default. A name registered with `shared: true` and used by
  several concurrent sessions can therefore read `idle` while another session is mid-turn. The watchdog
  may then send a reminder or an "assign or retire" notice for an agent that is working.
- **Answers are recognised by reply.** A request counts as answered only when its owner replies with a
  `result` or `error` carrying `in_reply_to`. Work acknowledged any other way still looks open until
  its outstanding record expires (7 days).
