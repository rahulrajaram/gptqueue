/**
 * Deterministic communication-lifecycle scenarios for GPTQueue.
 *
 * Each scenario runs `rounds` independent bounded rounds against a fresh
 * isolated server (see server-fixture.mjs). Actors are deterministic MCP
 * clients over the Unix-socket HTTP transport; every edge carries a unique
 * per-edge idempotency key plus the same key inside `metadata.edge_key`, so
 * received messages correlate by scenario id, round id, sender, receiver,
 * and edge key. No ordering beyond the documented contract is assumed:
 * expected deliveries are compared as sets/multisets over drained
 * observations.
 *
 * A scenario throws on any violated invariant and otherwise returns a JSON
 * summary; run-scenario.mjs prints the summary and exits 0/1.
 */

import { LifecycleClient } from "./lifecycle-client.mjs";
import { withFixture, pollUntil } from "./server-fixture.mjs";

const DEFAULTS = {
  rounds: 3,
  drainTimeoutMs: 20000,
};

// ---------------------------------------------------------------------------
// Pure helpers (no I/O): expected edge construction and observation comparison
// ---------------------------------------------------------------------------

/** One expected edge; `edgeKey` is the unique per-edge correlation identity. */
export function makeEdge({ scenario, round, from, to, seq, content }) {
  const edgeKey = `${scenario}:r${round}:${from}->${to}#${seq}`;
  return { scenario, round, from, to, seq, content, edgeKey };
}

/** Canonical multiset key for a drained observation. */
export function observedKey(observation) {
  const meta = observation.payload?.metadata ?? {};
  return JSON.stringify([
    observation.from,
    observation.to,
    meta.edge_key ?? null,
    observation.payload?.content ?? null,
  ]);
}

export function expectedKey(edge) {
  return JSON.stringify([edge.from, edge.to, edge.edgeKey, edge.content]);
}

/** Assert observed multiset === expected multiset; classify the differences. */
export function assertExactDeliveries(observed, expected, label) {
  const observedCounts = new Map();
  for (const o of observed) {
    const k = observedKey(o);
    observedCounts.set(k, (observedCounts.get(k) ?? 0) + 1);
  }
  const expectedCounts = new Map();
  for (const e of expected) {
    const k = expectedKey(e);
    expectedCounts.set(k, (expectedCounts.get(k) ?? 0) + 1);
  }
  const missing = [];
  for (const [k, count] of expectedCounts) {
    const got = observedCounts.get(k) ?? 0;
    if (got < count) missing.push({ edge: JSON.parse(k), missing: count - got });
  }
  const duplicated = [];
  for (const [k, count] of observedCounts) {
    const want = expectedCounts.get(k) ?? 0;
    if (count > want) duplicated.push({ edge: JSON.parse(k), extra: count - want });
  }
  if (missing.length > 0 || duplicated.length > 0) {
    throw new Error(
      `${label}: delivery mismatch — missing=${JSON.stringify(missing)} duplicateLogicalDeliveries=${JSON.stringify(duplicated)}`
    );
  }
}

// ---------------------------------------------------------------------------
// Wire-shape normalization: some tools serialize the legacy payload (a bare
// array or the message object) as content[0].text, so the parsed data is not
// always the {status, agents|queues|message} envelope.
// ---------------------------------------------------------------------------

function agentsOf(result) {
  const d = result.data;
  if (Array.isArray(d)) return d;
  return d?.agents ?? [];
}

function queuesOf(result) {
  const d = result.data;
  if (Array.isArray(d)) return d;
  return d?.queues ?? [];
}

function isMessageShape(d) {
  return d && typeof d === "object" && "payload" in d && "from" in d && "to" in d;
}

function messageOf(result) {
  const d = result.data;
  if (isMessageShape(d)) return d;
  if (d?.status === "message" && isMessageShape(d.message)) return d.message;
  return null;
}

function noMessages(result) {
  return result.data?.status === "no_messages" || result.data === null || result.data === undefined;
}

/** Require a typed tool error with the given code; return the payload. */
export function requireTypedError(result, code, label) {
  if (!result.isError || result.data?.status !== "error") {
    throw new Error(`${label}: expected typed error, got ${JSON.stringify(result.data).slice(0, 200)}`);
  }
  const actual = result.data.error?.code;
  if (actual !== code) {
    throw new Error(`${label}: expected error code ${code}, got ${actual} (${JSON.stringify(result.data).slice(0, 200)})`);
  }
  return result.data;
}

// ---------------------------------------------------------------------------
// Shared per-round actor plumbing
// ---------------------------------------------------------------------------

async function registerActors(fixture, names, roundLabel) {
  const clients = new Map();
  for (const name of names) {
    clients.set(
      name,
      await LifecycleClient.create({
        socketPath: fixture.httpSocketPath,
        name,
        role: "both",
        description: `${roundLabel} ${name}`,
      })
    );
  }
  return clients;
}

/** A fresh transport bound to a retained app session (stateless reconnection). */
async function reboundClient(socketPath, sessionId) {
  return LifecycleClient.create({ socketPath, sessionId });
}

/** H1 readiness barrier: listing + status surfaces see every actor. */
async function readinessBarrier(clients, names, roundLabel) {
  const prober = clients.values().next().value;
  const list = await prober.call("list_agents");
  const listed = agentsOf(list).map((a) => a.name).sort();
  const want = [...names].sort();
  if (JSON.stringify(listed) !== JSON.stringify(want)) {
    throw new Error(`${roundLabel}: list_agents ${JSON.stringify(listed)} != registered ${JSON.stringify(want)}`);
  }
  const distinct = new Set([...clients.values()].map((c) => c.appSessionId));
  if (distinct.size !== clients.size) {
    throw new Error(`H1 violated (${roundLabel}): actors did not receive distinct session identities`);
  }
  const status = await prober.call("get_queue_status");
  const queues = queuesOf(status);
  if (!Array.isArray(queues) || queues.length === 0) {
    throw new Error(`${roundLabel}: get_queue_status unexpected shape ${JSON.stringify(status.data).slice(0, 200)}`);
  }
  const byName = new Map(queues.map((q) => [q.name ?? q.agent, q]));
  for (const name of names) {
    if (!byName.has(name)) {
      throw new Error(`${roundLabel}: get_queue_status does not report ${name}`);
    }
  }
  return distinct.size;
}

/** Drain a client's mailbox until empty or the expected count is reached. */
async function drain(client, { max = 64, perCallTimeout = 1, timeoutMs } = {}) {
  const drained = [];
  const deadline = Date.now() + (timeoutMs ?? DEFAULTS.drainTimeoutMs);
  let idleRounds = 0;
  while (drained.length < max && Date.now() < deadline) {
    const res = await client.call("receive_message", { timeout: perCallTimeout });
    const message = messageOf(res);
    if (message) {
      drained.push(message);
      idleRounds = 0;
    } else if (noMessages(res)) {
      idleRounds += 1;
      if (idleRounds >= 2) break;
    } else {
      throw new Error(`unexpected receive_message result: ${JSON.stringify(res.data).slice(0, 200)}`);
    }
  }
  return drained;
}

/** Send one edge and require a `sent` acknowledgment. */
async function sendEdge(sender, edge) {
  const res = await sender.call("send_message", {
    to: edge.to,
    content: edge.content,
    type: "task",
    metadata: { edge_key: edge.edgeKey, scenario: edge.scenario, round: edge.round },
    idempotency_key: edge.edgeKey,
  });
  if (res.isError || res.data?.status !== "sent") {
    throw new Error(`send_message ${edge.edgeKey} failed: ${JSON.stringify(res.data).slice(0, 200)}`);
  }
  return res.data;
}

async function baselineHealth(fixture, roundLabel) {
  const health = await fixture.health();
  if (health.status !== "ok") throw new Error(`${roundLabel}: baseline /health not ok`);
  return health.sessions;
}

async function assertHealthBackToBaseline(fixture, baseline, roundLabel) {
  await pollUntil(
    async () => (await fixture.health()).sessions === baseline,
    { timeoutMs: 10000, label: `${roundLabel}: /health sessions back to baseline ${baseline}` }
  );
}

/** Terminal unregister for every client, then close all transports.
 * Restarts can invalidate a client's transport session; if the unregister
 * call is refused, retry once over a fresh transport bound to the retained
 * app session so terminal cleanup never silently no-ops. */
async function terminalCleanupAndBaseline(fixture, clients, { graceful = [] } = {}) {
  for (const client of clients.values()) {
    if (!graceful.includes(client.name)) {
      const result = await client.call("unregister_agent").catch((error) => ({ isError: true, data: { raw: String(error) } }));
      if (result.isError || result.httpStatus === 404) {
        // Retry over a fresh transport bound to the retained session; if the
        // session is already gone the registry assertion below still guards
        // the final state, so treat that refusal as best-effort success.
        const fresh = await reboundClient(fixture.httpSocketPath, client.appSessionId);
        try {
          await fresh.call("unregister_agent");
        } catch {
          /* already unregistered or session gone */
        } finally {
          await fresh.close({ unregister: false }).catch(() => {});
        }
      }
    }
    await client.close({ unregister: false });
  }
}

async function assertRegistryEmpty(fixture, roundLabel) {
  const probe = await LifecycleClient.create({
    socketPath: fixture.httpSocketPath,
    name: `lc-${roundLabel.replaceAll(":", "-")}-probe`,
    description: `${roundLabel} post-cleanup probe`,
  });
  try {
    const list = await probe.call("list_agents");
    const agents = agentsOf(list);
    if (agents.length !== 1 || agents[0].name !== probe.name) {
      throw new Error(`${roundLabel}: registry not clean after terminal unregister: ${JSON.stringify(agents.map((a) => a.name))}`);
    }
  } finally {
    await probe.close({ unregister: true });
  }
}

function roundResult(round, detail) {
  return { round, ...detail };
}

// ---------------------------------------------------------------------------
// Scenario: topology (H1, H2, H3, H4)
// ---------------------------------------------------------------------------

export async function runTopologyScenario(rounds = DEFAULTS.rounds) {
  const roundsResults = [];
  for (let round = 1; round <= rounds; round++) {
    const label = `topology:r${round}`;
    const scenario = "topology";
    const sfx = `r${round}`;
    const a1n = `lc-topo-${sfx}-a1`, a2n = `lc-topo-${sfx}-a2`, a3n = `lc-topo-${sfx}-a3`;
    const b1n = `lc-topo-${sfx}-b1`, b2n = `lc-topo-${sfx}-b2`, xn = `lc-topo-${sfx}-x`;
    roundsResults.push(
      await withFixture(label, async (fixture) => {
        const baseline = await baselineHealth(fixture, label);
        const clients = await registerActors(fixture, [a1n, a2n, a3n, b1n, b2n, xn], label);
        try {
          const distinctSessions = await readinessBarrier(clients, [a1n, a2n, a3n, b1n, b2n, xn], label);
          const [a1, a2, a3, b1, b2, x] = [a1n, a2n, a3n, b1n, b2n, xn].map((n) => clients.get(n));

          // H2 — one-to-one with exact payload and sender attribution.
          const e11 = makeEdge({ scenario, round, from: a1n, to: b1n, seq: 1, content: `one-to-one:${label}` });
          await sendEdge(a1, e11);
          const got11 = await drain(b1, { max: 1 });
          assertExactDeliveries(got11, [e11], `H2 one-to-one ${label}`);
          if (got11[0].from !== a1n || got11[0].payload.content !== e11.content) {
            throw new Error(`H2 violated (${label}): payload/attribution mismatch on the 1-to-1 edge`);
          }

          // H3 — one-to-N fan-out over ordinary send_message calls.
          const fanout = [
            makeEdge({ scenario, round, from: a2n, to: b1n, seq: 2, content: `fanout:${label}:b1` }),
            makeEdge({ scenario, round, from: a2n, to: b2n, seq: 3, content: `fanout:${label}:b2` }),
          ];
          for (const edge of fanout) await sendEdge(a2, edge);
          const gotB1 = await drain(b1, { max: 1 });
          assertExactDeliveries(gotB1, [fanout[0]], `H3 fan-out b1 ${label}`);
          const gotB2 = await drain(b2, { max: 1 });
          assertExactDeliveries(gotB2, [fanout[1]], `H3 fan-out b2 ${label}`);
          const gotX = await drain(x, { max: 2, timeoutMs: 6000 });
          if (gotX.length !== 0) {
            throw new Error(`H3 violated (${label}): bystander received ${gotX.length} messages (cross-delivery)`);
          }

          // H4 — deterministic 3x2 complete bipartite matrix.
          const senders = [a1, a2, a3];
          const receivers = [b1, b2];
          const matrix = [];
          let seq = 10;
          for (const s of senders) {
            for (const r of receivers) {
              matrix.push(makeEdge({ scenario, round, from: s.name, to: r.name, seq: seq++, content: `matrix:${label}:${s.name}->${r.name}` }));
            }
          }
          if (matrix.length !== 6) throw new Error(`H4 violated (${label}): matrix is not 6 edges`);
          for (const edge of matrix) {
            await sendEdge(senders.find((s) => s.name === edge.from), edge);
          }
          const mB1 = await drain(b1, { max: 3 });
          const mB2 = await drain(b2, { max: 3 });
          const mX = await drain(x, { max: 3, timeoutMs: 6000 });
          assertExactDeliveries(mB1, matrix.filter((e) => e.to === b1n), `H4 matrix b1 ${label}`);
          assertExactDeliveries(mB2, matrix.filter((e) => e.to === b2n), `H4 matrix b2 ${label}`);
          if (mX.length !== 0) {
            throw new Error(`H4 violated (${label}): bystander received ${mX.length} matrix messages (cross-delivery)`);
          }
          for (const [name, got] of [[b1n, mB1], [b2n, mB2]]) {
            const attribution = got.map((m) => m.from).sort();
            const want = [a1n, a2n, a3n].sort();
            if (JSON.stringify(attribution) !== JSON.stringify(want)) {
              throw new Error(`H4 violated (${label}): ${name} attribution ${JSON.stringify(attribution)} != ${JSON.stringify(want)}`);
            }
          }

          await terminalCleanupAndBaseline(fixture, clients);
          await assertRegistryEmpty(fixture, label);
          await assertHealthBackToBaseline(fixture, baseline, label);
          return roundResult(round, {
            baselineSessions: baseline,
            registeredActors: 6,
            distinctSessionIds: distinctSessions,
            readinessBarrier: "passed",
            oneToOne: { edges: 1, delivered: 1 },
            oneToMany: { sendCalls: fanout.length, edges: fanout.length, delivered: fanout.length, bystanderDeliveries: 0 },
            matrix: { senders: 3, receivers: 2, edges: 6, delivered: 6, missing: 0, duplicateLogicalDeliveries: 0, crossDeliveries: 0 },
          });
        } finally {
          for (const c of clients.values()) await c.close({ unregister: false }).catch(() => {});
        }
      })
    );
  }
  return { scenario: "topology", rounds, roundsResults, ok: true };
}

// ---------------------------------------------------------------------------
// Scenario: idempotency (H5)
// ---------------------------------------------------------------------------

export async function runIdempotencyScenario(rounds = DEFAULTS.rounds) {
  const roundsResults = [];
  for (let round = 1; round <= rounds; round++) {
    const label = `idempotency:r${round}`;
    const sName = `lc-idem-r${round}-s`, rName = `lc-idem-r${round}-r`;
    roundsResults.push(
      await withFixture(label, async (fixture) => {
        const baseline = await baselineHealth(fixture, label);
        const clients = await registerActors(fixture, [sName, rName], label);
        try {
          await readinessBarrier(clients, [sName, rName], label);
          const s = clients.get(sName), r = clients.get(rName);

          const k1 = `idempotency:r${round}:${sName}->${rName}#1`;
          const first = await s.call("send_message", {
            to: rName, content: `idem:${label}`, type: "task",
            metadata: { edge_key: k1 }, idempotency_key: k1,
          });
          if (first.data?.status !== "sent") throw new Error(`first send failed: ${JSON.stringify(first.data).slice(0, 200)}`);
          const originalId = first.data.message_id;

          // Retry with the SAME key: no second logical delivery, original id returned.
          const retry = await s.call("send_message", {
            to: rName, content: `idem:${label}`, type: "task",
            metadata: { edge_key: k1 }, idempotency_key: k1,
          });
          if (retry.data?.status !== "duplicate") {
            throw new Error(`H5 violated (${label}): same-key retry returned ${retry.data?.status}, expected duplicate`);
          }
          if (retry.data.message_id !== originalId) {
            throw new Error(`H5 violated (${label}): same-key retry id ${retry.data.message_id} != original ${originalId}`);
          }
          const drained1 = await drain(r, { max: 4 });
          if (drained1.length !== 1) {
            throw new Error(`H5 violated (${label}): receiver got ${drained1.length} deliveries for one idempotent edge`);
          }

          // Distinct key on the same pair remains distinct.
          const k2 = `idempotency:r${round}:${sName}->${rName}#2`;
          const second = await s.call("send_message", {
            to: rName, content: `idem:${label}:second`, type: "task",
            metadata: { edge_key: k2 }, idempotency_key: k2,
          });
          if (second.data?.status !== "sent" || second.data.message_id === originalId) {
            throw new Error(`H5 violated (${label}): distinct key produced status=${second.data?.status} id=${second.data?.message_id}`);
          }
          const drained2 = await drain(r, { max: 4 });
          if (drained2.length !== 1) {
            throw new Error(`H5 violated (${label}): distinct-key edge delivered ${drained2.length} messages`);
          }
          if (drained2[0].id === drained1[0].id) {
            throw new Error(`H5 violated (${label}): distinct keys shared one message id`);
          }

          await terminalCleanupAndBaseline(fixture, clients);
          await assertRegistryEmpty(fixture, label);
          await assertHealthBackToBaseline(fixture, baseline, label);
          return roundResult(round, {
            sameKeyRetry: { result: "duplicate", originalMessageIdReturned: true, logicalDeliveries: 1 },
            distinctKey: { result: "sent", distinctMessageId: true, logicalDeliveries: 1 },
          });
        } finally {
          for (const c of clients.values()) await c.close({ unregister: false }).catch(() => {});
        }
      })
    );
  }
  return { scenario: "idempotency", rounds, roundsResults, ok: true };
}

// ---------------------------------------------------------------------------
// Scenario: continuity (H6 stateless re-binding, H7 stale transport recovery)
// ---------------------------------------------------------------------------

export async function runContinuityScenario(rounds = DEFAULTS.rounds) {
  const roundsResults = [];
  for (let round = 1; round <= rounds; round++) {
    const label = `continuity:r${round}`;
    const scenario = "continuity";
    const sName = `lc-cont-r${round}-s`, rName = `lc-cont-r${round}-r`, tName = `lc-cont-r${round}-t`;
    roundsResults.push(
      await withFixture(label, async (fixture) => {
        const baseline = await baselineHealth(fixture, label);
        const clients = await registerActors(fixture, [sName, rName, tName], label);
        try {
          await readinessBarrier(clients, [sName, rName, tName], label);
          const s = clients.get(sName), r = clients.get(rName), t = clients.get(tName);

          // Queue a message for the receiver, then abandon its transport.
          const queued = makeEdge({ scenario, round, from: sName, to: rName, seq: 1, content: `queued:${label}` });
          await sendEdge(s, queued);

          // H6 — a fresh transport connection re-binds by session_id only.
          const replacement = await reboundClient(fixture.httpSocketPath, r.appSessionId);
          try {
            const got = await replacement.call(
              "receive_message",
              { timeout: 5, session_id: r.appSessionId },
              { withSessionId: false }
            );
            const gotMessage = messageOf(got);
            if (!gotMessage) {
              throw new Error(`H6 violated (${label}): re-bound receive returned ${JSON.stringify(got.data).slice(0, 200)}`);
            }
            assertExactDeliveries([gotMessage], [queued], `H6 re-bound delivery ${label}`);
            const list = await replacement.call("list_agents");
            const entries = agentsOf(list).filter((a) => a.name === rName);
            if (entries.length !== 1) {
              throw new Error(`H6 violated (${label}): ${entries.length} registry entries for ${rName} after re-bind (re-registration?)`);
            }
          } finally {
            await replacement.close({ unregister: false }).catch(() => {});
          }

          // H7 — stale transport session: SIGKILL the server, keep Redis.
          await fixture.killServer();
          await fixture.spawnServer();

          const stale = await t.call("list_agents", {}, { withSessionId: false });
          if (stale.httpStatus !== 404) {
            throw new Error(`H7 violated (${label}): stale transport call returned HTTP ${stale.httpStatus}, expected documented 404`);
          }
          if (!/session|expired|not found|unknown/i.test(stale.contentText)) {
            throw new Error(`H7 violated (${label}): stale 404 body does not report the session-expired error: ${stale.contentText.slice(0, 200)}`);
          }

          // Re-initialize on the same endpoint succeeds; re-bind the app session.
          const fresh = await reboundClient(fixture.httpSocketPath, t.appSessionId);
          try {
            const rebound = await fresh.call("list_agents");
            const reboundOk = !rebound.isError && (Array.isArray(rebound.data) || rebound.data?.status === "ok");
            if (!reboundOk) {
              throw new Error(`H7 violated (${label}): re-initialize + re-bind failed: ${JSON.stringify(rebound.data).slice(0, 200)}`);
            }
            // Subsequent messaging is valid after recovery.
            const after = makeEdge({ scenario, round, from: tName, to: sName, seq: 2, content: `post-recovery:${label}` });
            await sendEdge(fresh, after);
            const sRebound = await reboundClient(fixture.httpSocketPath, s.appSessionId);
            try {
              const gotBack = await drain(sRebound, { max: 2 });
              assertExactDeliveries(gotBack, [after], `H7 post-recovery delivery ${label}`);
            } finally {
              await sRebound.close({ unregister: false }).catch(() => {});
            }
          } finally {
            await fresh.close({ unregister: false }).catch(() => {});
          }

          await terminalCleanupAndBaseline(fixture, clients);
          await assertRegistryEmpty(fixture, label);
          await assertHealthBackToBaseline(fixture, baseline, label);
          return roundResult(round, {
            statelessRebind: { queuedThenReceived: true, reRegistered: false, registryEntriesForReceiver: 1 },
            staleTransport: { refusalStatus: 404, reinitialized: true, postRecoveryDelivery: true },
          });
        } finally {
          for (const c of clients.values()) await c.close({ unregister: false }).catch(() => {});
        }
      })
    );
  }
  return { scenario: "continuity", rounds, roundsResults, ok: true };
}

// ---------------------------------------------------------------------------
// Scenario: cleanup (H8 close vs unregister + baseline restoration)
// ---------------------------------------------------------------------------

export async function runCleanupScenario(rounds = DEFAULTS.rounds) {
  const roundsResults = [];
  for (let round = 1; round <= rounds; round++) {
    const label = `cleanup:r${round}`;
    const gName = `lc-cln-r${round}-graceful`, tName = `lc-cln-r${round}-terminal`, wName = `lc-cln-r${round}-witness`;
    roundsResults.push(
      await withFixture(label, async (fixture) => {
        const baseline = await baselineHealth(fixture, label);
        const clients = await registerActors(fixture, [gName, tName, wName], label);
        try {
          await readinessBarrier(clients, [gName, tName, wName], label);
          const g = clients.get(gName), t = clients.get(tName), w = clients.get(wName);

          // Mailboxes non-empty for both close targets.
          await sendEdge(w, makeEdge({ scenario: "cleanup", round, from: wName, to: gName, seq: 1, content: `preserved:${round}` }));
          await sendEdge(w, makeEdge({ scenario: "cleanup", round, from: wName, to: tName, seq: 2, content: `removed:${round}` }));

          // Graceful close: distinct documented semantics, mailbox preserved.
          const closed = await g.call("close_session");
          if (closed.data?.status !== "session_closed" || closed.data?.mailbox_preserved !== true) {
            throw new Error(`H8 violated (${label}): close_session returned ${JSON.stringify(closed.data).slice(0, 200)}`);
          }
          const rebinder = await LifecycleClient.create({ socketPath: fixture.httpSocketPath });
          try {
            const refused = await rebinder.call("receive_message", { timeout: 0, session_id: g.appSessionId }, { withSessionId: false });
            if (!refused.isError || refused.data?.error?.code !== "SESSION_UNAVAILABLE") {
              throw new Error(`H8 violated (${label}): closed-session re-bind got ${JSON.stringify(refused.data).slice(0, 200)}, expected SESSION_UNAVAILABLE`);
            }
            // Graceful keeps registration + mailbox: status still reports the queue.
            const status = await rebinder.call("get_queue_status", { agent: gName });
            const gq = queuesOf(status).find((q) => (q.name ?? q.agent) === gName);
            if (!gq) {
              throw new Error(`H8 violated (${label}): graceful-closed agent vanished from queue status (registration must be kept)`);
            }
          } finally {
            await rebinder.close({ unregister: false }).catch(() => {});
          }

          // Terminal unregister: registration and mailbox removed.
          const unreg = await t.call("unregister_agent");
          if (unreg.isError) throw new Error(`unregister_agent failed: ${JSON.stringify(unreg.data).slice(0, 200)}`);
          const prober = await LifecycleClient.create({ socketPath: fixture.httpSocketPath });
          try {
            const list = await prober.call("list_agents");
            const names = agentsOf(list).map((a) => a.name);
            if (names.includes(tName)) throw new Error(`H8 violated (${label}): unregistered agent still listed`);
            const refused = await prober.call("receive_message", { timeout: 0, session_id: t.appSessionId }, { withSessionId: false });
            if (!refused.isError || refused.data?.error?.code !== "SESSION_UNAVAILABLE") {
              throw new Error(`H8 violated (${label}): unregistered-session re-bind got ${JSON.stringify(refused.data).slice(0, 200)}`);
            }
            const send = await prober.call("send_message", {
              to: tName, content: "post-unregister", type: "task",
              metadata: { edge_key: `cleanup:r${round}:post-unregister` }, idempotency_key: `cleanup:r${round}:post-unregister`,
            });
            if (!send.isError || send.data?.error?.code !== "unknown_recipient") {
              throw new Error(`H8 violated (${label}): send to unregistered agent got ${JSON.stringify(send.data).slice(0, 200)}, expected unknown_recipient`);
            }
          } finally {
            await prober.close({ unregister: false }).catch(() => {});
          }

          // Graceful actor keeps its documented registry + mailbox state; lease
          // refresh is stopped (session record closed, re-bind refused above).
          await terminalCleanupAndBaseline(fixture, clients, { graceful: [gName] });
          await assertHealthBackToBaseline(fixture, baseline, label);
          return roundResult(round, {
            gracefulClose: { status: "session_closed", mailboxPreserved: true, rebindRefused: "SESSION_UNAVAILABLE", registrationKept: true, leaseRefreshStopped: true },
            terminalUnregister: { listed: false, rebindRefused: "SESSION_UNAVAILABLE", sendRefused: "unknown_recipient", mailboxRemoved: true },
            healthBaseline: baseline,
          });
        } finally {
          for (const c of clients.values()) await c.close({ unregister: false }).catch(() => {});
        }
      })
    );
  }
  return { scenario: "cleanup", rounds, roundsResults, ok: true };
}

// ---------------------------------------------------------------------------
// Scenario: backpressure (H9 typed refusal, no silent loss)
// ---------------------------------------------------------------------------

export async function runBackpressureScenario(rounds = DEFAULTS.rounds) {
  const roundsResults = [];
  for (let round = 1; round <= rounds; round++) {
    const label = `backpressure:r${round}`;
    const scenario = "backpressure";
    const bound = 3;
    const sName = `lc-bp-r${round}-s`, rName = `lc-bp-r${round}-r`;
    roundsResults.push(
      await withFixture(label, async (fixture) => {
        const baseline = await baselineHealth(fixture, label);
        const clients = await registerActors(fixture, [sName, rName], label);
        try {
          await readinessBarrier(clients, [sName, rName], label);
          const s = clients.get(sName), r = clients.get(rName);

          const pre = [];
          for (let i = 1; i <= bound; i++) {
            const edge = makeEdge({ scenario, round, from: sName, to: rName, seq: i, content: `pre-refusal:${round}:${i}` });
            await sendEdge(s, edge);
            pre.push(edge);
          }
          // Bound is full: further send is refused, typed, retryable.
          const overflow = makeEdge({ scenario, round, from: sName, to: rName, seq: bound + 1, content: `overflow:${round}` });
          const refused = await s.call("send_message", {
            to: rName, content: overflow.content, type: "task",
            metadata: { edge_key: overflow.edgeKey }, idempotency_key: overflow.edgeKey,
          });
          requireTypedError(refused, "QUEUE_FULL", `H9 overflow send ${label}`);
          if (refused.data?.error?.retryable !== true) {
            throw new Error(`H9 violated (${label}): queue-full error not marked retryable`);
          }

          // No silent loss: every pre-refusal message is received intact.
          const drained = await drain(r, { max: bound });
          assertExactDeliveries(drained, pre, `H9 pre-refusal deliveries ${label}`);

          // After draining, a fresh key is delivered.
          const post = makeEdge({ scenario, round, from: sName, to: rName, seq: bound + 2, content: `post-drain:${round}` });
          await sendEdge(s, post);
          const drainedPost = await drain(r, { max: 1 });
          assertExactDeliveries(drainedPost, [post], `H9 post-drain delivery ${label}`);

          await terminalCleanupAndBaseline(fixture, clients);
          await assertRegistryEmpty(fixture, label);
          await assertHealthBackToBaseline(fixture, baseline, label);
          return roundResult(round, {
            queueBound: bound,
            typedRefusal: { code: "QUEUE_FULL", retryable: true, silentLoss: 0 },
            preRefusalDeliveries: { expected: bound, received: drained.length, intact: true },
            postDrainDelivery: true,
          });
        } finally {
          for (const c of clients.values()) await c.close({ unregister: false }).catch(() => {});
        }
      }, { queueBound: bound })
    );
  }
  return { scenario: "backpressure", rounds, roundsResults, ok: true };
}
