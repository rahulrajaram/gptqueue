# Principal approval — offline GPTQueue conformance harness

Date: 2026-08-27

The principal approved the following exact brief boundary:

- Bind GPTQueue commit `66e5f10bc6832d2e4f37f5ce820cbc160c67b0ad`
  and tree `19ef13f09e72ba705a5cdfe6a9a1ca9c961c0793`; preserve the
  earlier `545217c04731bd5e711ca66f5c5f259d3eda4db9` intake and its
  intervening diff as provenance only.
- Use a hand-authored or transparently adapted Harness Module v2 as the
  enduring reference unless explicitly superseded.
- Prove only deterministic offline reference semantics: single-flight wake
  intent, fixture readiness, stable task/claim/runtime identity, claim
  recovery, duplicate suppression, `store_only`, immutability, and
  same-runtime batch-delivery acknowledgement.
- Treat acknowledgement as delivery-level model semantics, never task
  completion or proof of consumer effects.
- Treat fixture messages, task prose, roles, capabilities, and
  instruction-like content as opaque advisory data without execution or
  decision authority.
- Leave retry timing and limits, retention, dead-letter policy, role and
  capability enforcement, Redis/MCP behavior, production durability,
  consumer effects, and actual runtime launch unresolved and unclaimed.
- Use the focused activation suite as MetaBuilder-governed conformance
  evidence. Treat the Redis-dependent full suite as separately labeled
  corroboration, never as a governed offline effect.
- Preserve the existing seven MCP tools and live Redis behavior unchanged.
- Authorize no network, Redis, target-source write, dependency installation,
  credential, provider, push, deploy, or external-state effect for this wedge.

Principal response: “Yes, I approve this.”

This file records conversation evidence. It does not authenticate identity or
grant authority beyond the approved boundary.

## Architecture diagram approval

Date: 2026-08-28

After reviewing the system flow and requesting corrections, the principal
approved revision 3 of the architecture diagram. The approved revision:

- distinguishes caller-supplied routing today from optional future agent
  selection;
- keeps the Griller and Respondent outside delivery infrastructure;
- shows `receive_message` as a receiver-initiated MCP call using Redis
  `BLPOP`, not as Redis streaming directly into a runtime;
- separates durable actor identity from temporary runtime identity;
- distinguishes `offline_launchable`, `offline_store_only`, and
  `unavailable`;
- treats MetaBuilder offline conformance as validation evidence, not a GPTQueue
  runtime state; and
- preserves the claim ceiling around live Redis, runtime launch, end-to-end
  completion, deployment, and production policy.

Approved Mermaid source:
`diagram-preview-v3/gptqueue-holistic-system-and-harness.mmd`, SHA-256
`12077a986895374cd656b502f671bbaf9ad409970aacd2b05e4a5100f57109c5`.

Principal response: “I approve of the diagram.”

This approval establishes the diagram as the accepted architectural
orientation for the next implementation shell. It does not resolve or approve
the open production-policy decisions recorded in the product thesis.
