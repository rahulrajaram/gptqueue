#!/usr/bin/env node
/**
 * Stateless re-binding client for the restart-durability scenario (H11).
 *
 * A tiny Node CLI that resumes an EXISTING registered session by passing only
 * the retained session_id to a fresh transport connection and calls
 * receive_message once. It intentionally contains NO register_agent call
 * anywhere: re-registration would create a duplicate registry entry and would
 * invalidate the "no re-registration" acceptance for the scenario.
 *
 * Usage: node harness/lifecycle/rebind-client.mjs <httpSocketPath> <sessionId>
 *
 * Prints exactly one JSON line to stdout:
 *   { "received": true, "from", "to", "content", "metadata" }  -> exit 0
 *   { "error": "..." }                                         -> exit 1
 */

import { LifecycleClient } from "./lifecycle-client.mjs";

function printAndExit(obj, code) {
  process.stdout.write(JSON.stringify(obj) + "\n");
  process.exit(code);
}

async function main() {
  const [socketPath, sessionId] = process.argv.slice(2);
  if (!socketPath || !sessionId) {
    printAndExit({ error: "usage: rebind-client.mjs <httpSocketPath> <sessionId>" }, 1);
  }
  let client = null;
  try {
    // Fresh transport bound to the retained app session; no registration.
    client = await LifecycleClient.create({ socketPath, sessionId });
    const res = await client.call(
      "receive_message",
      { timeout: 5, session_id: sessionId },
      { withSessionId: false }
    );
    const d = res.data;
    const message =
      d && typeof d === "object" && "payload" in d && "from" in d && "to" in d ? d : null;
    if (!message) {
      printAndExit({ error: "no_messages", raw: d }, 1);
    }
    // Close the transport (DELETE /mcp) so the in-memory transport-session
    // record is dropped before exit; the durable registration is preserved.
    await client.close({ unregister: false });
    printAndExit(
      {
        received: true,
        from: message.from,
        to: message.to,
        content: message.payload?.content,
        metadata: message.payload?.metadata ?? {},
      },
      0
    );
  } catch (error) {
    if (client) {
      try {
        await client.close({ unregister: false });
      } catch {
        /* best-effort */
      }
    }
    printAndExit({ error: String(error) }, 1);
  }
}

await main();