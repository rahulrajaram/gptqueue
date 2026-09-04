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
    "Prove GPTQueue agent communication across the full lifecycle: distinct registration and readiness, 1-to-1, 1-to-N, and 3x2 complete-bipartite delivery with exact payload, sender attribution, no missing edges, no duplicate logical delivery, and no cross-delivery; retry idempotency via idempotency keys; stateless process continuity through session re-binding; stale transport recovery; graceful close versus terminal unregister with cleanup back to baseline; typed observable backpressure refusal; concurrent interleaving of a 2x3 complete bipartite matrix where two senders transmit concurrently via Promise.all over ordinary send_message calls, yielding exactly six unique edges with correct attribution and zero duplicates or cross-delivery; and restart durability where a message accepted as sent immediately before a real server SIGKILL and respawn remains receivable afterward through session re-binding - through bounded deterministic scenarios, three consecutive rounds each, without any live server, live Redis db0, network egress, or target-source writes.",

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
    "concurrent-interleaving-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of the 2x3 concurrent matrix: six send_message calls issued concurrently with no ordering assumptions, edge uniqueness, sender attribution, and per-receiver observations"),
      maxBytes: 1048576,
    }),
    "concurrent-interleaving-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that concurrent interleaving held for all rounds"),
      maxBytes: 65536,
    }),
    "restart-durability-summary": artifact({
      mediaType: "application/json",
      contract: described("Per-round summaries of message survival across a real server SIGKILL and respawn with session re-binding on a fresh transport"),
      maxBytes: 1048576,
    }),
    "restart-durability-verification": artifact({
      mediaType: "application/json",
      contract: described("Controller verdict that restart durability held for all rounds"),
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

    achieve({
      id: "exercise-concurrent-interleaving",
      objective:
        "H10: have two senders each transmit their three edges of the deterministic 2x3 complete bipartite matrix concurrently (Promise.all over the ordinary send_message calls, no ordering assumptions) and observe exactly six unique edges with complete sender attribution, each of the three receivers observing exactly two messages with one attributed to each sender - zero missing edges, zero duplicate logical deliveries, zero cross-deliveries - matched purely by idempotency key and attribution across three consecutive bounded rounds",
      produces: ["concurrent-interleaving-summary"],
    }),
    establish({
      id: "verify-concurrent-interleaving",
      claim:
        "Concurrent interleaving held: the six concurrent sends yielded exactly six unique edges with complete sender attribution and per-receiver observations of exactly two messages, zero missing edges, zero duplicate logical deliveries, and zero cross-deliveries, unmatched by arrival order, for all three consecutive rounds",
      verification: {
        subject: "concurrent-interleaving-summary",
        evidence: "concurrent-interleaving-verification",
      },
    }),

    achieve({
      id: "exercise-restart-durability",
      objective:
        "H11: have a sender's message accepted (sent result observed) immediately before the runner kills the HTTP server with a real SIGKILL and respawns it against the same in-sandbox redis-server, then have a fresh client process re-bind the retained receiver session_id on a fresh transport connection without re-registering and receive that exact message byte-for-byte with correct sender attribution and its per-edge idempotency key, across three consecutive bounded rounds",
      produces: ["restart-durability-summary"],
    }),
    establish({
      id: "verify-restart-durability",
      claim:
        "Restart durability held: a message accepted immediately before a real server SIGKILL and respawn remained receivable byte-for-byte through session re-binding on a fresh transport without re-registration, served by a genuinely fresh server process, for all three consecutive rounds",
      verification: {
        subject: "restart-durability-summary",
        evidence: "restart-durability-verification",
      },
    }),
  ),

  acceptance: established("verify-restart-durability"),
});