# Autonomous Agent Coordination Product Thesis

- Status: draft for principal review
- Last updated: 2026-08-27
- Initial clients: Codex CLI and Pi coding agent

## Customer promise

An agent can find the right collaborator, contact it, and receive a reliable
result without a human having to discover, start, or shuttle messages between
agent sessions.

## The problem

Coding agents can call tools and start local workers, but independent agent
sessions do not share a dependable operating model for identity, discovery,
delivery, activation, or completion. GPTQueue currently supplies named Redis
mailboxes through MCP tools. It does not yet supply autonomous coordination.

The consequence is a human-in-the-loop relay:

1. A person remembers which specialist exists.
2. The person starts or finds the specialist's shell.
3. The sender and receiver manually register compatible names.
4. The receiver polls and consumes a message.
5. The person notices when delivery, execution, or reply stalls.

The product succeeds when that relay is unnecessary for ordinary bounded
delegation.

## Product identity

GPTQueue is an actor directory and durable task router for coding agents. Its
mailbox is an internal delivery mechanism, not the primary product
abstraction.

GPTQueue is:

- a durable registry of logical agents and their capabilities;
- a reliable transport for messages and work items;
- an activation coordinator for approved Codex CLI and Pi runtimes;
- a task lifecycle that makes responsibility and completion observable; and
- a client-neutral protocol with runtime-specific adapters.

GPTQueue is not:

- a model provider or coding agent;
- a replacement for Codex CLI or Pi;
- an unrestricted remote shell;
- a system where a sender chooses another agent's command or permissions; or
- a requirement that every interaction create a child agent.

Sub-agent lineage is useful identity metadata, but parent/child relationships
are not the organizing center. Independent agents, persistent specialists,
ephemeral workers, and sub-agents use the same actor and task contracts.

## Destination, first wedge, and expansion

### Destination

A Codex or Pi agent can describe work, resolve a suitable collaborator,
delegate it, and continue. GPTQueue persists the work, activates the receiver
when allowed, coalesces concurrent requests into one runtime, records
acknowledgement and completion, and returns the result to the sender.

### First independently useful wedge

Two locally managed agents can coordinate reliably across offline periods:

- both have durable identities and discoverable capabilities;
- a sender delegates to a name or capability;
- an offline recipient is activated under its registered contract;
- one activation drains a batch of pending work;
- delivery is acknowledged rather than destroyed on read; and
- the sender can observe pending, claimed, completed, or failed state.

### Expansion path

1. Qualify the contract with Codex CLI and Pi on one host.
2. Add capability routing and multiple eligible specialists.
3. Add bounded concurrency, scheduling, and richer team structures.
4. Add authenticated multi-host operation only after the local trust model is
   proven.

At every stage, the recurring structural idea is the same: durable actors own
policy; temporary runtimes perform work; durable tasks join them.

## Ratified product choices

These choices were explicitly accepted in the product conversation. They are
inputs to the draft, not claims that the current implementation supports them.

1. The overarching problem is autonomous communication between agents.
2. Codex CLI and Pi are the primary initial clients.
3. Sub-agents must work, but should not receive undue architectural emphasis.
4. Messages remain durable while a receiver is offline.
5. A sender may wake an offline receiver within the receiver's bounded
   activation contract.
6. Waking is coalesced: a burst of requests should not create one runtime per
   request.
7. Agents have default workspace, working, state, and run directories.
8. An agent normally operates from a default workspace containing the source
   it owns and improves.

## Recommended operating model

### Durable actor, temporary runtime

A logical agent survives process exit. A Codex or Pi process is one temporary
runtime incarnation of that actor.

The minimum durable actor profile is:

| Field | Meaning | Default |
| --- | --- | --- |
| `actor_id` | Stable internal identity | Generated once |
| `alias` | Human-facing address such as `metabuilder` | Chosen at registration |
| `capabilities` | Structured routing claims | Empty until declared |
| `workspace_root` | Source repository the actor owns | Registration directory |
| `working_directory` | Default process directory | `workspace_root` |
| `state_directory` | Persistent runtime-specific state | GPTQueue-managed actor path |
| `run_directory` | Isolated state for one incarnation | Created per activation |
| `runtime` | Approved adapter and launch profile | Inferred from registering client |
| `activation_policy` | Who may wake it and within which bounds | Owner-managed local policy |
| `max_concurrency` | Simultaneous runtime limit | `1` |

Runtime identity is separate and includes an incarnation ID, session ID,
start time, lease, and observed presence. Optional lineage includes native
runtime ID, parent actor ID, and root actor ID.

### Presence states

Discovery returns durable actors regardless of current process state:

| State | Meaning |
| --- | --- |
| `active` | A leased runtime is processing work |
| `idle` | A leased runtime can accept work immediately |
| `starting` | One activation owns the wake lease |
| `offline_launchable` | No runtime exists; policy permits activation |
| `offline_store_only` | Messages can wait but activation is disabled |
| `unavailable` | The actor exists but its launch contract cannot run |

Unknown recipients are rejected. Sending to an arbitrary string must not
silently create an orphan mailbox.

### Delivery and activation

The default delivery mode is `wake_if_offline`; `store_only` is an explicit
alternative.

The activation sequence is:

1. Persist the message or task before any wake attempt.
2. Resolve the receiver's owner-controlled activation profile.
3. Reuse an idle or active runtime when one exists.
4. Otherwise acquire one per-actor wake lease.
5. Start the configured adapter in the actor's default directories.
6. Let one runtime claim and drain the accumulated batch.
7. Deliver arrivals during startup to that same runtime.
8. Keep the runtime alive through a configurable idle grace period.
9. Exit only after the inbox remains empty and no claimed work is active.

The sender can request activation but cannot supply an arbitrary executable,
directory, credential, permission set, or concurrency override. Receiver
policy remains authoritative.

### Message and task semantics

A message communicates information. A task establishes responsibility and a
lifecycle. Both are durable, but only a task is claimable and completion
tracked.

The recommended task states are:

```text
pending -> claimed -> running -> completed
                    \-> failed
                    \-> blocked
```

Expired claims return through an explicit recovery transition. An ambiguous
activation or external effect remains `unknown` until reconciled; it is not
blindly retried.

Delivery should be at-least-once with stable IDs and explicit acknowledgement.
Consumers must be idempotent. Reading a work item must not delete the only
durable copy.

### Agent-facing surface

The primary interface should be small and intention-revealing:

| Operation | Purpose |
| --- | --- |
| `team_context` | Join or resume an actor and see relevant collaborators and pending work |
| `delegate_task` | Resolve a recipient, persist work, and request policy-bounded activation |
| `team_inbox` | Claim or inspect messages and tasks with acknowledgement semantics |
| `resolve_task` | Complete, fail, block, or release claimed work with evidence |

Low-level send, receive, registration, and queue-status operations may remain
as administrative or compatibility surfaces. Agents should not need to
compose them correctly for the ordinary path.

MCP tools carry commands and results. The durable records live in GPTQueue's
store; they are not MCP resources or per-agent files. Read-only MCP resources
may later expose directory or history views, but they are not the delivery
channel.

## Trust and authority

- Identity names an actor; it does not grant permission.
- Capability says what a runtime can technically do; it does not authorize it.
- The actor owner controls launch profiles, workspaces, permissions, and wake
  policy.
- The sender owns the requested outcome and may request activation.
- The receiver owns whether and how it can be activated.
- GPTQueue owns leases, task transitions, durable delivery evidence, and
  routing decisions.
- Runtime output is a claim. Task completion evidence is interpreted under
  the caller's or harness's acceptance contract.

The first wedge assumes one trusted local operator. Network listeners,
cross-user access, and multi-host activation require authentication and are
outside that assumption.

## Product principles

1. Autonomous by default, policy-bounded always.
2. Persist intent before effects.
3. Wake once per inactive-to-active transition, not once per message.
4. Make the common path zero-configuration after actor registration.
5. Keep stable actor identity separate from runtime and session identity.
6. Prefer explicit task state over inference from mailbox depth.
7. Preserve ambiguous outcomes and reconcile before retrying.
8. Make discovery structured enough for agents to choose collaborators.
9. Keep runtime adapters thin and the coordination contract client-neutral.
10. Reject unknown recipients and unsupported effects visibly.

## Constraints and non-goals for the first wedge

- Do not add MetaBuilder as a GPTQueue runtime dependency.
- Do not require Redis knowledge from agent clients.
- Do not depend on typing into an arbitrary historical shell.
- Do not allow a sender to override a receiver's execution contract.
- Do not imply authentication while the deployment is an unauthenticated
  local service.
- Do not require multiple concurrent runtimes for one actor; concurrency above
  one is opt-in.
- Do not make remote deployment, billing, or organization-level tenancy part
  of the initial proof.

## Truth ledger

| Classification | Statement |
| --- | --- |
| Observed fact | Current messages are sent through MCP tools and stored in bounded Redis lists. |
| Observed fact | Current receive uses destructive pop semantics rather than acknowledgement. |
| Observed fact | Offline mailboxes retain messages, but GPTQueue has no activation operation. |
| Observed fact | Current discovery can omit a known offline durable identity while its mailbox still exists. |
| Observed fact | Codex CLI and Pi can both consume MCP tools; their lifecycle hooks differ. |
| Supported inference | Thin client adapters can normalize registration, inbox notifications, and runtime identity. |
| Strategic bet | Default activation and batching will materially reduce human relay work. |
| Strategic bet | Four intention-level tools are easier for agents to use correctly than seven low-level queue tools. |
| Preference | Redis Streams with consumer groups is the leading delivery substrate. |
| Known unknown | The narrowest safe launcher contract shared by Codex CLI and Pi. |
| Known unknown | Whether capability routing initially needs ranking or only exact filtering. |
| Protection | Stable IDs, leases, idempotency, bounded retries, and explicit `unknown` contain failures while choices evolve. |

## First-wedge acceptance evidence

A controller-owned conformance suite should prove at least these scenarios:

1. A registered offline actor remains discoverable and addressable.
2. An unknown recipient is rejected without creating queue data.
3. Ten concurrent tasks for one offline actor produce one activation attempt.
4. Tasks arriving during startup join the same receiver batch.
5. A launched Pi receiver starts in its registered workspace and working
   directory with isolated state and run directories.
6. A launched Codex receiver receives equivalent actor and task context.
7. A claimed task survives receiver failure and becomes recoverable after its
   lease expires.
8. Redelivery does not duplicate a task effect when the same stable task ID is
   acknowledged twice.
9. `store_only` persists work without activation.
10. Receiver policy denies an unauthorized wake while retaining or rejecting
    the task according to the declared delivery contract.
11. Multiple live sessions for one actor do not corrupt registration,
    presence, or mailbox ownership.
12. A sub-agent receives a distinct actor identity with optional parent and
    root lineage, without changing ordinary delivery semantics.

## Open decisions before an agreed build brief

These are not yet ratified product behavior:

1. Whether `delegate_task` accepts only an actor ID, only a capability query,
   or both with an explicit routing mode.
2. Whether ordinary informational messages default to activation or only
   delegated tasks do.
3. The precise acknowledgement, retry, dead-letter, and retention periods.
4. The initial launcher boundary for Codex CLI and Pi and how a receiver
   proves that the requested runtime actually started.
5. Whether an actor's default workspace may be dirty, or activation requires a
   clean tree or isolated checkout under receiver policy.
6. The minimal authentication boundary for the local HTTP transport.
7. Which compatibility guarantees apply to the seventeen existing MCP tools.

## Success, stop, and revision signals

Continue when the conformance suite shows autonomous delegation reduces manual
steps without duplicate activations or lost tasks.

Revise the model if leases cannot prevent duplicate costly effects, runtime
adapters require client-specific task semantics, or capability routing makes
recipient choice less predictable than explicit addressing.

Stop activation work if GPTQueue cannot enforce the receiver's launch policy,
cannot preserve an ambiguous outcome, or requires exposing unrestricted host
control to agents.

## Now, next, horizon

- **Now:** agree on this operating model and convert acceptance scenarios into
  an executable client-conformance harness.
- **Next:** implement reliable delivery, durable actor discovery, activation
  leases, and one Pi plus one Codex adapter.
- **Horizon:** capability-routed, authenticated coordination across many
  locally and remotely hosted specialist agents.

## Language ladder

- **Customer:** Your coding agents can find and work with each other without
  you relaying every request.
- **Product:** GPTQueue gives persistent specialist agents a shared directory,
  durable tasks, and policy-controlled activation.
- **Practitioner:** Register an agent once with its capabilities and workspace;
  delegations are stored, wake one suitable runtime, batch pending work, and
  expose completion state.
- **Technical:** Durable actor profiles and leased runtime incarnations are
  joined by acknowledged task records; Codex and Pi adapters implement a
  client-neutral activation and inbox contract.
