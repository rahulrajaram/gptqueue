/** Pure, bounded relations for evidence captured by an acceptance driver. */

export type Outcome = "meets" | "does_not_meet" | "uncertain" | "not_applicable";
export type ExecutionStatus = "not_run" | "completed" | "failed" | "unsupported";
export type AcceptanceDimension = "communication" | "automatic_tasks" | "initiative";
export type ExecutionEvidence = Readonly<{ status: ExecutionStatus; detail?: string }>;
export type MessageEvidence = Readonly<{ id: string; from: string; to: string; in_reply_to?: string; content?: string }>;
/** A runtime trace proving consumption and claim acknowledgement. */
export type ConsumptionEvidence = Readonly<{
  message_id: string; actor: string; consumed: boolean; claim_id?: string; acknowledged: boolean;
}>;
export type ExchangeEvidence = Readonly<{
  sender: string; recipient: string; request?: MessageEvidence; reply?: MessageEvidence;
  request_consumption?: ConsumptionEvidence; reply_consumption?: ConsumptionEvidence;
  prior_message_ids?: readonly string[]; success_marker?: string; execution?: ExecutionEvidence;
  expected_reply_content: string;
  request_requires_ack?: boolean; reply_requires_ack?: boolean;
}>;
export type ExchangeCheck = Readonly<{
  name: "distinct_actors" | "request_id" | "reply_id" | "request_route" | "reply_route" | "reply_correlation" |
    "request_consumed" | "reply_consumed" | "request_acknowledged" | "reply_acknowledged" | "reply_content";
  passed: boolean; detail: string;
}>;
export type ExchangeVerdict = Readonly<{
  outcome: Outcome; execution: ExecutionEvidence; checks: readonly ExchangeCheck[]; reasons: readonly string[];
}>;
export type AcceptanceRow = Readonly<{
  id: string; dimension: AcceptanceDimension; route?: string; outcome: Outcome;
  execution: ExecutionEvidence; required?: boolean; detail?: string;
}>;
export type TotalVerdict = Readonly<{
  outcome: Outcome; rows: readonly AcceptanceRow[]; failed: readonly string[];
  unresolved: readonly string[]; reasons: readonly string[];
}>;

const nonEmpty = (value: string | undefined): value is string =>
  typeof value === "string" && value.length > 0;
const execution = (value: ExecutionEvidence | undefined): ExecutionEvidence =>
  Object.freeze(value ?? { status: "not_run" });
const check = (name: ExchangeCheck["name"], passed: boolean, detail: string): ExchangeCheck =>
  Object.freeze({ name, passed, detail });
const trace = (
  value: ConsumptionEvidence | undefined, id: string | undefined, actor: string,
  name: "request_consumed" | "reply_consumed",
): ExchangeCheck => {
  const passed = value?.consumed === true && value.message_id === id && value.actor === actor;
  return check(name, passed, passed ? `${actor} consumed ${id}` : `${actor} has no exact consumption trace for ${id ?? "the message"}`);
};
const ack = (
  value: ConsumptionEvidence | undefined, id: string | undefined, actor: string,
  name: "request_acknowledged" | "reply_acknowledged",
): ExchangeCheck => {
  const passed = value?.acknowledged === true && nonEmpty(value.claim_id) && value.message_id === id && value.actor === actor;
  return check(name, passed, passed ? `${actor} acknowledged claim ${value?.claim_id}` : `${actor} has no exact claim acknowledgement for ${id ?? "the message"}`);
};

/** Queue acceptance, an agent assertion, or a model marker cannot pass this relation. */
export const checkExchangeEvidence = (evidence: ExchangeEvidence): ExchangeVerdict => {
  const run = execution(evidence.execution), request = evidence.request, reply = evidence.reply;
  const prior = new Set(evidence.prior_message_ids ?? []);
  const present = nonEmpty(request?.id) && nonEmpty(reply?.id);
  const distinct = present && request.id !== reply.id;
  const fresh = present && !prior.has(request.id) && !prior.has(reply.id);
  const checks: readonly ExchangeCheck[] = Object.freeze([
    check("distinct_actors", nonEmpty(evidence.sender) && nonEmpty(evidence.recipient) && evidence.sender !== evidence.recipient,
      "a peer exchange requires distinct non-empty sender and recipient identities"),
    check("request_id", distinct && fresh, distinct && fresh ? `fresh request ID ${request.id}` : "request and reply IDs must be present, distinct, and fresh"),
    check("reply_id", distinct && fresh, distinct && fresh ? `fresh reply ID ${reply.id}` : "request and reply IDs must be present, distinct, and fresh"),
    check("request_route", request?.from === evidence.sender && request.to === evidence.recipient, request?.from === evidence.sender && request.to === evidence.recipient ? "request has the declared direction" : "request from/to does not match the declared direction"),
    check("reply_route", reply?.from === evidence.recipient && reply.to === evidence.sender, reply?.from === evidence.recipient && reply.to === evidence.sender ? "reply has the reverse direction" : "reply from/to does not match the reverse direction"),
    check("reply_correlation", reply?.in_reply_to === request?.id, reply?.in_reply_to === request?.id ? "reply correlates to the exact request ID" : "reply correlation does not name the exact request ID"),
    check("reply_content", reply?.content === evidence.expected_reply_content,
      reply?.content === evidence.expected_reply_content ? "reply matches the independently expected answer" : "reply does not match the independently expected answer"),
    trace(evidence.request_consumption, request?.id, evidence.recipient, "request_consumed"),
    trace(evidence.reply_consumption, reply?.id, evidence.sender, "reply_consumed"),
    ...(evidence.request_requires_ack === false ? [] : [ack(evidence.request_consumption, request?.id, evidence.recipient, "request_acknowledged")]),
    ...(evidence.reply_requires_ack === false ? [] : [ack(evidence.reply_consumption, reply?.id, evidence.sender, "reply_acknowledged")]),
  ]);
  const failures = checks.filter(({ passed }) => !passed);
  const reasons = [
    ...failures.map(({ detail }) => detail),
    ...(run.status === "failed" ? [`execution failed${run.detail ? `: ${run.detail}` : ""}`] : []),
    ...(run.status === "not_run" ? ["execution was not run"] : []),
    ...(run.status === "unsupported" ? ["execution is unsupported"] : []),
  ];
  const outcome: Outcome = run.status === "unsupported" || run.status === "not_run" || run.status === "failed" ? "uncertain" :
    failures.length > 0 ? "does_not_meet" : "meets";
  return Object.freeze({ outcome, execution: run, checks, reasons: Object.freeze(reasons) });
};
export const evaluateExchange = checkExchangeEvidence;

/** Required failures dominate; unrun and unsupported rows remain unresolved and visible. */
export const evaluateTotalVerdict = (input: readonly AcceptanceRow[]): TotalVerdict => {
  const rows = Object.freeze(input.map((row) => Object.freeze({ ...row })));
  const required = rows.filter(({ required }) => required !== false);
  const failed = required.filter(({ outcome }) => outcome === "does_not_meet").map(({ id }) => id);
  const unresolved = required.filter(({ outcome, execution: run }) =>
    outcome === "uncertain" || outcome === "not_applicable" || run.status !== "completed",
  ).map(({ id }) => id);
  const outcome: Outcome = rows.length === 0 || required.length === 0 ? "not_applicable" :
    failed.length > 0 ? "does_not_meet" : unresolved.length > 0 ? "uncertain" : "meets";
  const reasons = [
    ...(failed.length > 0 ? [`required failures: ${failed.join(", ")}`] : []),
    ...(unresolved.length > 0 ? [`unresolved required rows: ${unresolved.join(", ")}`] : []),
  ];
  return Object.freeze({ outcome, rows, failed: Object.freeze(failed), unresolved: Object.freeze(unresolved), reasons: Object.freeze(reasons) });
};
export const evaluateAcceptance = evaluateTotalVerdict;
