import { describe, expect, it } from "vitest";
import {
  checkExchangeEvidence,
  evaluateTotalVerdict,
  type AcceptanceRow,
  type ExchangeEvidence,
} from "./oracle.js";

const validEvidence = (
  overrides: Partial<ExchangeEvidence> = {},
): ExchangeEvidence => ({
  sender: "route-a",
  recipient: "route-b",
  expected_reply_content: "73",
  request: { id: "request-1", from: "route-a", to: "route-b" },
  reply: {
    id: "reply-1",
    from: "route-b",
    to: "route-a",
    in_reply_to: "request-1",
    content: "73",
  },
  request_consumption: {
    message_id: "request-1",
    actor: "route-b",
    consumed: true,
    claim_id: "claim-request",
    acknowledged: true,
  },
  reply_consumption: {
    message_id: "reply-1",
    actor: "route-a",
    consumed: true,
    claim_id: "claim-reply",
    acknowledged: true,
  },
  execution: { status: "completed" },
  ...overrides,
});

const row = (
  id: string,
  outcome: AcceptanceRow["outcome"],
  status: AcceptanceRow["execution"]["status"] = "completed",
  required = true,
): AcceptanceRow => ({
  id,
  dimension: "communication",
  outcome,
  execution: { status },
  required,
});

describe("bounded acceptance exchange oracle", () => {
  it("rejects identity collapse even if a self-exchange has complete traces", () => {
    const encoded = JSON.stringify(validEvidence()).replaceAll('route-b', 'route-a');
    expect(checkExchangeEvidence(JSON.parse(encoded)).outcome).toBe('does_not_meet');
  });
  it("accepts an exact two-sided exchange with correlated reply and acks", () => {
    const result = checkExchangeEvidence(validEvidence());

    expect(result.outcome).toBe("meets");
    expect(result.execution.status).toBe("completed");
    expect(result.checks.every(({ passed }) => passed)).toBe(true);
    expect(Object.isFrozen(result)).toBe(true);
  });

  it("rejects a forged model marker without actual consumption traces", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      success_marker: "SENDER_RECEIVED_42",
      request_consumption: undefined,
      reply_consumption: undefined,
    });

    expect(result.outcome).toBe("does_not_meet");
    expect(result.reasons.join(" ")).toContain("consumption trace");
  });

  it("rejects swapped request identities", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      request: { id: "request-1", from: "route-b", to: "route-a" },
    });

    expect(result.outcome).toBe("does_not_meet");
    expect(result.checks.find(({ name }) => name === "request_route")?.passed).toBe(false);
  });

  it("rejects a reply with the wrong correlation", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      reply: {
        id: "reply-1",
        from: "route-b",
        to: "route-a",
        in_reply_to: "other-request",
      },
    });

    expect(result.outcome).toBe("does_not_meet");
    expect(result.checks.find(({ name }) => name === "reply_correlation")?.passed).toBe(false);
  });

  it("rejects a consumed request when its claim acknowledgement is absent", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      request_consumption: {
        message_id: "request-1",
        actor: "route-b",
        consumed: true,
        acknowledged: false,
      },
    });

    expect(result.outcome).toBe("does_not_meet");
    expect(result.checks.find(({ name }) => name === "request_acknowledged")?.passed).toBe(false);
  });

  it("keeps absent execution uncertain even when the relation is structurally complete", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      execution: undefined,
    });

    expect(result.outcome).toBe("uncertain");
    expect(result.execution.status).toBe("not_run");
    expect(result.reasons).toContain("execution was not run");
  });

  it("keeps an execution failure separate from an established semantic failure", () => {
    const result = checkExchangeEvidence({
      ...validEvidence(),
      execution: { status: "failed", detail: "runtime exited" },
    });

    expect(result.outcome).toBe("uncertain");
    expect(result.reasons).toContain("execution failed: runtime exited");
  });

  it("keeps an entirely unexecuted exchange uncertain", () => {
    expect(checkExchangeEvidence({ sender: "a", recipient: "b", expected_reply_content: "73" }).outcome).toBe("uncertain");
  });

  it("rejects a correlated but incorrect answer", () => {
    const evidence = validEvidence();
    expect(checkExchangeEvidence({ ...evidence, reply: { ...evidence.reply!, content: "74" } }).outcome).toBe("does_not_meet");
  });

  it("accepts exact legacy consumption without inventing claim acknowledgements", () => {
    const evidence = validEvidence();
    expect(checkExchangeEvidence({ ...evidence, reply_requires_ack: false,
      reply_consumption: { message_id: "reply-1", actor: "route-a", consumed: true, acknowledged: false } }).outcome).toBe("meets");
  });
});

describe("bounded acceptance total verdict", () => {
  it("lets a required failure dominate an unrun row while retaining both IDs", () => {
    const result = evaluateTotalVerdict([
      row("pair-a-b", "does_not_meet"),
      row("pair-b-a", "uncertain", "not_run"),
    ]);

    expect(result.outcome).toBe("does_not_meet");
    expect(result.failed).toEqual(["pair-a-b"]);
    expect(result.unresolved).toEqual(["pair-b-a"]);
  });

  it("reports a finite all-pass set as meets", () => {
    expect(
      evaluateTotalVerdict([
        row("communication-a-b", "meets"),
        row("automatic-a-b", "meets"),
        row("initiative-a", "meets"),
      ]).outcome,
    ).toBe("meets");
  });

  it("does not hide non-required setup gaps", () => {
    const result = evaluateTotalVerdict([
      row("communication-a-b", "meets"),
      row("setup-opencode-acp", "not_applicable", "unsupported", false),
    ]);

    expect(result.outcome).toBe("meets");
    expect(result.rows.map(({ id }) => id)).toEqual([
      "communication-a-b",
      "setup-opencode-acp",
    ]);
  });
});
