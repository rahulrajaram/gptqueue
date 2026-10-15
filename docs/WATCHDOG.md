# gptqueue watchdog

`gptqueue-watchdog --once [--config PATH] [--dry-run] [--redis-url URL]` runs one pass over the
configured leads and prints one JSON object. Scheduling it (a timer) is an operator decision and is
not installed by gptqueue.

- **Config:** `$XDG_CONFIG_HOME/gptqueue/watchdog.json` (default `~/.config/gptqueue/watchdog.json`):
  `schema_version`, `root`, `leads`, `operator`, `thresholds_min` (`waiting`, `escalate_after`, `idle`),
  `max_actions_per_run`.
- **Exit codes:** 0 success, 2 usage or config problem, 1 Redis unavailable.
- **Output:** `{ at, leads: [{ lead, state, sw4rm_state }], actions: [{ kind, rule, to, fire_key, text, status }],
  deferred, uncorrelated, dry_run, operator_alerts }`.
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
