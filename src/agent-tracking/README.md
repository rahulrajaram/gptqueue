# Agent tracking: seats

A **seat** is a durable role: a job card plus a policy for who may hold it.
Seats outlive runtime sessions. A session never owns a seat. Its process
*holds* the seat for one **tenure**. Each tenure gets a new, strictly
increasing **epoch**, so a successor can fence out anything its predecessor
might still try to write.

This directory is a self-contained library. It imports only `node:` builtins
and its own files, so it can become its own package later. GPTQueue plugs it
in through one seam, `src/integrations/agent-tracking.ts`.

## How a session gets a seat

1. The operator declares seats in a **seat file** selected by `GPTQ_SEAT_FILE`
   (see the default path below).
2. A launcher starts the runtime with `GPTQ_SEAT=<seat_id>`, and
   optionally `GPTQ_SEAT_FILE=<path>`. The default path is
   `$XDG_CONFIG_HOME/gptqueue/seats.json`, or
   `~/.config/gptqueue/seats.json` when `XDG_CONFIG_HOME` is unset.
3. The runtime's startup hook calls `startSeat`. The hook first checks the
   seat against the seat file and the launcher policy (runtime and working
   directory), then binds it to the runtime process. It puts the job card into
   startup context, or a visible refusal if the bind fails. The model makes no
   tool call.

With `GPTQ_SEAT` unset, nothing changes: the startup context is the same
byte for byte.

For Claude Code, the GPTQueue SessionStart hook (`src/claude/hook.ts`) does
this. The holding process is the Claude process that the session record
names. Codex and Pi have startup seams (`codex-hook.ts`, `pi-extension.ts`)
that can call `startSeat` in the same way. Slice 1 does not wire them yet.

## Seat file

```json
{
  "schema_version": 1,
  "seats": [
    {
      "seat_id": "acme.supervisor",
      "name": "Acme supervisor",
      "description": "Coordinates the Acme leads and owns the merge gate.",
      "capabilities": ["merge-gate", "decision-packets"],
      "policy": { "runtimes": ["claude"], "cwd_roots": ["/path/to/project-a"] }
    }
  ]
}
```

| Field | Limit |
|---|---|
| `seat_id` | `[A-Za-z0-9._-]{1,64}`, unique |
| `name` | 1–80 characters |
| `description` | at most 200 words and 1,600 characters |
| `capabilities` | at most 12, each 1–48 characters |
| `policy.runtimes` | at least one runtime name |
| `policy.cwd_roots` | at least one absolute path, matched by whole path segments |

## Tenure rules

The holding **process** is the tenure unit: machine id, boot id, PID
namespace, pid and start time together, plus the hostname as a guard.

| Situation | Result |
|---|---|
| The seat is vacant | Bind at `epoch + 1` (`bind`, reason `vacant`) |
| The same process binds again | Same tenure, lease renewed. A new native session id (e.g. after Claude `/clear`) is recorded as `rebind`. |
| Another process binds and the holder is **provably alive** | Refused with `seat_held`, even after the lease expires |
| Another process binds and the holder **provably exited** (process gone, or machine rebooted) | Takeover at `epoch + 1`, no waiting: `vacate` (`holder_exit_proven`), then `bind` (`takeover`) |
| Another process binds and the holder is **unobservable** | `seat_held` until the lease expires, then takeover (`lease_expired`) |
| `renew` or `release` with an old epoch | `stale_epoch` |
| `renew` or `release` from another process | `not_holder` |

The liveness evidence works like this:

- **Proof requires the same machine.** The machine id (`/etc/machine-id`)
  and hostname must match. Hostnames and the initial PID namespace inode are
  shared by cloned hosts, so neither proves anything on its own. Machine ids
  must be unique per host, which is also a systemd requirement.
- **A different boot of the same machine proves exit.** `boot_id` is random
  per boot, and a reboot ends every process. A seat held before a reboot is
  free again at once; it does not wait out the lease.
- **Within the same boot, the pid must be in the same PID namespace.** Proof
  of exit then means `/proc/<pid>` is gone, the process is a zombie, or the pid
  belongs to a process with a different start time (the pid was recycled).
- **Everything else is unobservable.** That covers another machine, another
  PID namespace, and unreadable identity or process data. Silence never
  counts as exit.
- **The lease exists only for unobservable holders.** The default is 6 hours.

**History is append-only.** It records `bind`, `rebind` and `vacate` entries
with the epoch, runtime, native session id, time and reason. Refused
operations and no-op reaps append nothing.

## Ports and adapters

| Port | Adapters |
|---|---|
| `SeatStore` (read, compare-and-set commit with history append, history) | `createMemorySeatStore`, `createRedisSeatStore` (needs only an `eval` client) |
| `Clock` | any `{ now(): Date }` |
| `ProcessProbe` (local machine, boot and pidns identity; observe pid) | `createNodeProcessProbe` (Linux `/proc`, `/etc/machine-id`) |

Both stores pass one contract suite: `tests/agent-tracking/seat-contract.ts`.

## sw4rm vocabulary

Field shapes follow the sw4rm vocabulary, checked against the
`@sw4rm/js-sdk` dev dependency in tests. This library does not use the sw4rm
wire protocol.

| Here | sw4rm |
|---|---|
| Seat card (`seat_id`, `name`, `description`, `capabilities`) | `AgentDescriptor` (`agent_id`, `name`, `description` ≤200 words, `capabilities`) |
| `SeatErrorCode` | `ErrorCode`, through `SEAT_ERROR_TO_SW4RM`. For example, `seat_held` maps to `ALREADY_IN_PROGRESS` and `stale_epoch` to `FORCED_PREEMPTION`. |

`@sw4rm/js-sdk` is a devDependency only. The AT11 test checks the
hand-written values against it.

## Acceptance tests (slice 1)

| AT | What it proves | Where |
|---|---|---|
| AT1 | A launcher-declared seat is bound at startup, and its card reaches context with no tool call | `at01-startup-binding.test.ts` (library and real Claude hook) |
| AT2 | Exclusivity, an idempotent rebind, and exactly one winner under concurrent binds | `seat-contract.ts` |
| AT3 | Dead-holder release on proven exit; silence and lease alone don't vacate a live holder | `seat-contract.ts` |
| AT4 | Epoch fencing of renew, release and `fence` | `seat-contract.ts` |
| AT5 | Append-only tenure history | `seat-contract.ts` |
| AT9 | Only declared seats, from allowed runtimes and working directories | `at09-authorization.test.ts` |
| AT10 | One contract suite passes on memory and Redis; an import boundary is enforced | `at10-*.test.ts` |
| AT11 | sw4rm vocabulary conformance against the SDK | `at11-sw4rm.test.ts` |
| AT12 | No new model-visible tools, byte-identical context without a seat, and a bounded card | `at12-overhead.test.ts` |

These are deferred to later slices, because each changes public queue
behavior:

- **AT6:** job-card goal history.
- **AT7:** seat-addressed mailbox continuity.
- **AT8:** staged ACKs (`RECEIVED`/`READ`/`FULFILLED`).
