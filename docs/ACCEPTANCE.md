# GPTQueue bounded acceptance contract

This contract decides a bounded question: for an installed runtime route that
can register with GPTQueue, can it establish an independently addressable
identity, discover a peer, and complete an evidenced exchange with that peer?
It is an acceptance aid for the enumerated run and does not claim universal
reliability, provider quality, or success for routes that were not run.

The semantic dimensions are evaluated separately:

1. **Communication**: registration, peer discovery, and a deliberate exact
   exchange.
2. **Automatic handling**: a task arriving while the runtime is idle is
   claimed, processed, replied to with the exact correlation, and acknowledged
   under the applicable consumption contract. A correlated result or error
   arriving for outstanding work must also give the agent a turn to continue
   that work and acknowledge its claim. Test tasks, results, and errors
   separately. A busy agent may finish its current turn before handling the
   arrival. Ping and status messages require readable delivery.
3. **Initiative**: three independent natural tasks per model-backed route are
   completed 3/3. The task wording must not name GPTQueue or instruct the model
   to send a message. The tasks should require complementary information so a
   memorized or copied answer is detectable.

Automatic delivery and initiative are different claims. A route can pass a
communication exchange while remaining unresolved or failing either of the
other dimensions.

## Where communication guidance reaches the agent

The generic MCP server supplies `GPTQUEUE_INSTRUCTIONS` in
`src/transports/setup-tools.ts`. It tells agents to choose GPTQueue for shared
coordination, register first, retain their session identity, and use safe retry
and session-close behavior. It also discourages combining GPTQueue and native
collaboration in the same workflow. The evaluation must check whether this
guidance works when a native child is the intended peer.

The registered bridge in `src/experimental-wrapper/bridge.ts` adds automatic
identity, peer discovery, readiness checks, and delivery-status guidance.
The Pi extension appends those MCP instructions to the model's system prompt
in `src/registered-shell/pi-extension.ts`. The runtime notification in
`src/registered-shell/runtime.ts` explicitly requests claim, processing,
correlated reply, acknowledgement, and continuation on result/error messages.
Its declared native runtime clients are currently Codex and Pi.

These are distinct mechanisms: MCP instructions can encourage an active agent,
while a runtime adapter can give an idle agent another turn. The initiative
trials measure observed behavior with the shipped guidance. Without a separate
controlled comparison, they do not establish that the guidance caused it.

The generic `register_agent.name` schema description currently asks the agent
to ask its user for a name if none is known (`src/mcp-server/tools/register-agent.ts`).
Natural trials should therefore include an agent without a preassigned name:
a mechanically successful trial that supplies a name cannot establish
autonomous self-registration. The registered bridge bypasses this question
because it creates an identity before exposing tools.

## Route population

The run manifest must enumerate every installed, registration-capable route in
the host. Host routes are recorded separately even when they share a process or
transport, because identity correctness is the unit of addressability:

| Host family | Routes to enumerate |
| --- | --- |
| Codex | interactive, appserver, headless, native child, fork, resume |
| Pi | interactive, headless, native subagent, managed, RPC, SDK |
| OpenCode | TUI, run, native Task, fork, resume, server attach, ACP |
| Other model hosts | each installed Claude and Gemini route that can register |
| Generic | stdio and HTTP clients that can register |

For every ordered pair of registration-capable routes `(A, B)`, record a row
for `A -> B` and a row for `B -> A`. Keep unbound, setup-failed, timed-out,
and otherwise failed rows in the manifest. A shared process or transport is
allowed when A and B still have the intended distinct identities. Missing setup
is recorded as a setup gap; it is not silently collapsed into a route pass.

The pair matrix is finite coverage of the manifest only. Adding a route or
changing its binding invalidates the affected rows and requires a new run.

## Exact exchange relation

The pure checker in
[`tests/acceptance/oracle.ts`](../tests/acceptance/oracle.ts) consumes an
`ExchangeEvidence` value. A passing exchange requires all of these observable
facts:

- a fresh, non-empty request ID and a different fresh reply ID;
- the request has `from = A`, `to = B`;
- the reply has `from = B`, `to = A` and `in_reply_to = request.id`;
- the reply content matches an independently computed expected answer;
- B has a runtime consumption trace for the exact request ID;
- A has a runtime consumption trace for the exact reply ID;
- claim-based consumers acknowledge the exact claim; legacy `receive_message`
  consumers instead provide the returned exact envelope. The driver declares
  which consumption contract applies on each side.

Queue acceptance, an empty queue, an agent-directory assertion, a send receipt,
or a model success marker is insufficient. A marker without receiver and sender
consumption traces fails the relation. Swapped identities, stale/reused IDs,
wrong correlation, and missing claim acknowledgement fail it as well.

The checker reports `outcome` separately from `execution`:

| Execution | Evidence | Outcome |
| --- | --- | --- |
| completed | every relation holds | `meets` |
| completed | any required relation is false | `does_not_meet` |
| not_run | any evidence state | `uncertain` |
| failed | execution prevented a semantic decision | `uncertain` |
| unsupported | route cannot execute this check | `uncertain` |

`evaluateTotalVerdict` aggregates explicitly enumerated rows. A required known
failure makes the total `does_not_meet`; an unrun or unsupported required row
remains listed as unresolved and makes the total `uncertain` unless a required
failure already dominates. Non-required setup-gap rows remain visible without
changing the total.

## Run and evidence rules

The default local command is:

```sh
REDIS_URL=redis://127.0.0.1:6379/15 npx --no-install vitest run tests/acceptance
```

The default run executes the pure oracle tests. Live
route checks are opt-in and must use DB 15, owned temporary profiles and owned
records. Preserve the host's existing authentication configuration; never copy,
print, or persist credentials. A live driver records route, model/provider,
configuration revision, timestamps, exact observed IDs and traces, execution
status, and cleanup result. It must not repair product code or alter global
configuration as part of acceptance.

The frozen September 12 run contains 25 routes and 625 ordered route pairs,
including pairs of independent agents launched through the same route. Its
retained evidence and generated coverage report live in
`.gptqueue/acceptance/20260912-evaluation/`. A populated matrix is an inventory
of obligations, not evidence that all 625 exchanges ran. The report must show
executed, blocked, and unrun coverage separately. A self-message can prove a
tool works in a launch mode; it cannot satisfy a distinct-peer pair.

Live probe gates are explicit environment variables in each test file. Run a
selected file with its gate set to `1`, retaining the command and log. The
report generator uses `GPTQUEUE_ACCEPTANCE_REPORT=1` and reads existing
receipts without invoking models. Pure answer and redaction checks run by
default. Original failed or confounded attempts remain in the evidence tree;
a fixture correction creates a new receipt rather than rewriting history.

To enforce acceptance after generating the report, add
`GPTQUEUE_ACCEPTANCE_ENFORCE=1`:

```sh
GPTQUEUE_ACCEPTANCE_REPORT=1 GPTQUEUE_ACCEPTANCE_ENFORCE=1 \
  REDIS_URL=redis://127.0.0.1:6379/15 \
  npx --no-install vitest run tests/acceptance/evidence-report.test.ts
```

This command preserves the report and exits unsuccessfully unless the required
aggregate meets the contract. Ordinary report generation checks the evidence
ledger without treating its own successful execution as product acceptance.

The run already establishes two failures of the universal premise: OpenCode's
native Task child replaces its parent's registration on the shared MCP
connection, and an idle OpenCode server session did not consume an incoming
task within a 300-second observation window. Those failures decide the overall
acceptance verdict without resolving the remaining coverage obligations.

The contract is scoped to the run's installed routes, environment, toolchain,
and evidence capture. It does not establish delivery guarantees outside those
conditions, and it does not turn a successful sample into a universal model or
runtime reliability claim.
