// GPTQueue communication-lifecycle intent — defineIntent source rendition.
//
// This is the TypeScript-esque authoring surface for the same program that is
// shipped and executed as the hand-authored Harness Module v3
// (MODULE.json -> bundle.json). The source states meaning only: obligations,
// claims, artifact contracts, and control flow. It contains no commands,
// paths, toolchains, or capability grants — per the defineIntent contract,
// those arrive later as generated implementation commitments, which require
// paid dispatch and are not part of this repository's governed route today.
//
// Validate the parse with:
//   metabuilder intent check --input intent.mb.ts --json

export default defineIntent({
  id: "gptqueue-communication-lifecycle",
  objective:
    "Prove GPTQueue agent communication across the full lifecycle: distinct registration and readiness, 1-to-1, 1-to-N, and 3x2 complete-bipartite delivery with exact payload, sender attribution, no missing edges, no duplicate logical delivery, and no cross-delivery; retry idempotency via idempotency keys; stateless process continuity through session re-binding; stale transport recovery; graceful close versus terminal unregister with cleanup back to baseline; and typed observable backpressure refusal - through bounded deterministic scenarios, three consecutive rounds each, without any live server, live Redis db0, network egress, or target-source writes.",

  artifacts: {
    "typecheck-result": artifact({
      mediaType: "application/json",
      contract: described("The no-emit typecheck outcome for the committed TypeScript target"),
      maxBytes: 65536,
    }),
    "topology-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of registration, readiness, 1-to-1, 1-to-N, and 3x2 matrix delivery with exact edge multisets"),
      maxBytes: 1048576,
    }),
    "topology-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that every topology invariant held for all rounds"),
      maxBytes: 65536,
    }),
    "idempotency-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of same-key retry and distinct-key delivery behavior"),
      maxBytes: 1048576,
    }),
    "idempotency-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that retry idempotency held for all rounds"),
      maxBytes: 65536,
    }),
    "continuity-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of stateless session re-binding and stale transport recovery"),
      maxBytes: 1048576,
    }),
    "continuity-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that process continuity and stale transport recovery held for all rounds"),
      maxBytes: 65536,
    }),
    "cleanup-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of graceful close versus terminal unregister and baseline restoration"),
      maxBytes: 1048576,
    }),
    "cleanup-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that exit and cleanup semantics held for all rounds"),
      maxBytes: 65536,
    }),
    "backpressure-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of the typed queue-full refusal and zero silent loss"),
      maxBytes: 1048576,
    }),
    "backpressure-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that backpressure semantics held for all rounds"),
      maxBytes: 65536,
    }),
  },

  workflow: sequence(
    achieve({
      id: "typecheck-target",
      objective:
        "Typecheck the committed TypeScript target without emit, inside the sandboxed adapter, with no filesystem writes to the target workspace",
      produces: ["typecheck-result"],
    }),

    achieve({
      id: "exercise-registration-and-topology",
      objective:
        "H1-H4: register actors with distinct usable session identities, pass an explicit readiness barrier through list_agents and get_queue_status, deliver one correlated 1-to-1 edge with exact payload and sender attribution, fan out one sender over N ordinary send_message calls to exactly the intended receivers, and complete the deterministic 3x2 complete bipartite matrix producing six uniquely identified edges with complete sender attribution - zero missing edges, zero duplicate logical deliveries, zero cross-deliveries - across three consecutive bounded rounds, correlating by scenario id, round id, sender, receiver, and a unique per-edge idempotency key",
      produces: ["topology-summary"],
    }),
    establish({
      id: "verify-registration-and-topology",
      claim:
        "Every topology invariant held: distinct session identities, readiness before transmission, exact payload and attribution on the 1-to-1 edge, exact fan-out with no bystander deliveries, and the 3x2 matrix delivered exactly for all three consecutive rounds",
      verification: {
        subject: "topology-summary",
        evidence: "topology-verification",
      },
    }),

    achieve({
      id: "exercise-retry-idempotency",
      objective:
        "H5: repeat one edge with the identical idempotency key and observe the original message id returned with exactly one logical delivery, then send a distinct key on the same sender-receiver pair and observe a separate logical delivery, across three consecutive bounded rounds",
      produces: ["idempotency-summary"],
    }),
    establish({
      id: "verify-retry-idempotency",
      claim:
        "Retry idempotency held: same-key retries never produced a second logical delivery and distinct keys remained distinct for all three consecutive rounds",
      verification: {
        subject: "idempotency-summary",
        evidence: "idempotency-verification",
      },
    }),

    achieve({
      id: "exercise-process-continuity",
      objective:
        "H6-H7: queue a message, resume the receiver's session from a fresh transport connection by passing only the retained session_id without re-registering, then invalidate the server-side transport session and observe the documented stale-session refusal, successful re-initialization on the same endpoint, and a valid post-recovery delivery with correct attribution, across three consecutive bounded rounds",
      produces: ["continuity-summary"],
    }),
    establish({
      id: "verify-process-continuity",
      claim:
        "Process continuity and stale transport recovery held: session re-binding never re-registered, the stale transport id was refused through the documented response, re-initialization succeeded, and post-recovery messaging delivered with correct attribution for all three consecutive rounds",
      verification: {
        subject: "continuity-summary",
        evidence: "continuity-verification",
      },
    }),

    achieve({
      id: "exercise-exit-and-cleanup",
      objective:
        "H8: exercise graceful close_session (session_closed with the mailbox preserved, lease refresh stopped, subsequent session-scoped calls refused with the documented session-unavailable error) versus terminal unregister_agent (removed from list_agents, session-scoped calls refused, sends refused as unknown_recipient, mailbox removed), then verify the isolated server's health session count returned to its recorded baseline, across three consecutive bounded rounds",
      produces: ["cleanup-summary"],
    }),
    establish({
      id: "verify-exit-and-cleanup",
      claim:
        "Exit and cleanup semantics held: graceful close and terminal unregister behaved with their distinct documented contracts and the transport-session count returned to baseline for all three consecutive rounds",
      verification: {
        subject: "cleanup-summary",
        evidence: "cleanup-verification",
      },
    }),

    achieve({
      id: "exercise-backpressure",
      objective:
        "H9: fill a small bounded queue, observe the documented typed queue-full refusal marked retryable, receive every pre-refusal message intact with correct sender attribution and its per-edge idempotency key (zero silent loss), and observe a fresh-key delivery after the receiver drains, across three consecutive bounded rounds",
      produces: ["backpressure-summary"],
    }),
    establish({
      id: "verify-backpressure",
      claim:
        "Backpressure held: the bounded refusal was typed and observable, never silent message loss, and the queue recovered for all three consecutive rounds",
      verification: {
        subject: "backpressure-summary",
        evidence: "backpressure-verification",
      },
    }),
  ),

  acceptance: established("verify-backpressure"),
});
