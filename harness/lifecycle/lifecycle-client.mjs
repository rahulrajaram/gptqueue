/**
 * Dependency-free MCP client for the GPTQueue Streamable HTTP transport over a
 * Unix domain socket, mirroring tests/helpers/mcp-agent.ts but implemented on
 * node:http with `socketPath` instead of `fetch`.
 *
 * `fetch` cannot be used inside the MetaBuilder sandboxed command adapter: the
 * 4 GiB address-space limit prevents undici's llhttp WASM from instantiating.
 * TCP loopback is also unavailable there (`--unshare-all` leaves `lo` down),
 * so the server listens on GPTQUEUE_HTTP_SOCKET and every request targets the
 * socket path.
 *
 * Wire behavior is identical to the TCP transport: initialize ->
 * mcp-session-id header -> notifications/initialized -> tools/call, with
 * tool results wrapped as content[0].text JSON (parsed here) plus an
 * optional top-level isError and the HTTP status preserved for callers that
 * must assert documented transport-level refusals (e.g. stale session 404).
 */

import http from "node:http";

/** Split a possibly-SSE HTTP body into parsed JSON fragments. */
export function parseBody(text) {
  const trimmed = text.trim();
  if (trimmed.length === 0) return [];
  if (trimmed.includes("\n") && trimmed.includes("data:")) {
    return trimmed
      .split("\n")
      .filter((l) => l.trim().startsWith("data:"))
      .map((l) => {
        try {
          return JSON.parse(l.slice(5).trim());
        } catch {
          return null;
        }
      })
      .filter((x) => x !== null);
  }
  try {
    return [JSON.parse(trimmed)];
  } catch {
    return [{ raw: trimmed }];
  }
}

function rawPost(socketPath, body, transportSession, timeoutMs) {
  return new Promise((resolve, reject) => {
    const payload = Buffer.from(JSON.stringify(body), "utf8");
    const req = http.request(
      {
        socketPath,
        path: "/mcp",
        method: "POST",
        timeout: timeoutMs,
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json, text/event-stream",
          "Content-Length": payload.length,
          ...(transportSession ? { "mcp-session-id": transportSession } : {}),
        },
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({
            status: res.statusCode,
            sessionId: res.headers["mcp-session-id"] ?? null,
            text: Buffer.concat(chunks).toString("utf8"),
          })
        );
      }
    );
    req.on("timeout", () => req.destroy(new Error("request timeout")));
    req.on("error", reject);
    req.end(payload);
  });
}

export function rawDelete(socketPath, transportSession, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath,
        path: "/mcp",
        method: "DELETE",
        timeout: timeoutMs,
        headers: transportSession ? { "mcp-session-id": transportSession } : {},
      },
      (res) => {
        res.resume();
        res.on("end", () => resolve({ status: res.statusCode }));
      }
    );
    req.on("timeout", () => req.destroy(new Error("request timeout")));
    req.on("error", reject);
    req.end();
  });
}

export function rawGetHealth(socketPath, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { socketPath, path: "/health", method: "GET", timeout: timeoutMs },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) });
          } catch (error) {
            reject(new Error(`unparseable /health body: ${String(error)}`));
          }
        });
      }
    );
    req.on("timeout", () => req.destroy(new Error("health timeout")));
    req.on("error", reject);
    req.end();
  });
}

/**
 * One agent = one MCP HTTP transport session. Registering agents pass a name;
 * continuity agents skip registration and re-bind a retained session_id on
 * their first session-scoped call (stateless reconnection contract).
 */
export class LifecycleClient {
  /**
   * @param {object} opts
   * @param {string} opts.socketPath UDS path of the server
   * @param {string} [opts.name] register under this name when provided
   * @param {string} [opts.sessionId] re-bind this retained app session instead of registering
   * @param {string} [opts.role]
   */
  static async create({
    socketPath,
    name,
    sessionId,
    role = "both",
    description,
  }) {
    const client = new LifecycleClient(socketPath);
    await client.initialize();
    if (sessionId !== undefined) {
      client.appSessionId = sessionId;
    } else if (name !== undefined) {
      const reg = await client.call("register_agent", {
        name,
        role,
        description: description ?? `${name} lifecycle-runner agent`,
      });
      const returned = reg.data.session_id;
      if (!returned) {
        throw new Error(`register_agent for '${name}' did not return a session_id: ${reg.contentText}`);
      }
      client.appSessionId = returned;
      client.name = name;
    } // else: bare unregistered transport (used for probes and stale-session tests)
    return client;
  }

  constructor(socketPath) {
    this.socketPath = socketPath;
    this.transportSession = null;
    this.appSessionId = null;
    this.name = null;
    this.nextId = 1;
    this.closed = false;
  }

  async initialize() {
    const res = await rawPost(
      this.socketPath,
      {
        jsonrpc: "2.0",
        id: this.nextId++,
        method: "initialize",
        params: {
          protocolVersion: "2025-03-26",
          capabilities: {},
          clientInfo: { name: "gptqueue-lifecycle-runner", version: "1.0.0" },
        },
      },
      this.transportSession,
      10000
    );
    if (res.status !== 200) {
      throw new Error(`initialize failed: HTTP ${res.status} ${res.text.slice(0, 200)}`);
    }
    if (res.sessionId) this.transportSession = res.sessionId;
    await rawPost(
      this.socketPath,
      { jsonrpc: "2.0", method: "notifications/initialized" },
      this.transportSession,
      10000
    );
    if (!this.transportSession) {
      throw new Error("server did not return an mcp-session-id during initialize");
    }
  }

  /**
   * Call a tool. When `withSessionId` is true the retained app session_id is
   * attached as an argument (the stateless reconnection contract); when false
   * the call exercises only the transport-local registration state.
   */
  async call(tool, args = {}, { withSessionId = true } = {}) {
    const fullArgs =
      withSessionId && this.appSessionId && !("session_id" in args)
        ? { ...args, session_id: this.appSessionId }
        : args;
    const res = await rawPost(
      this.socketPath,
      {
        jsonrpc: "2.0",
        id: this.nextId++,
        method: "tools/call",
        params: { name: tool, arguments: fullArgs },
      },
      this.transportSession,
      30000
    );
    if (res.status !== 200) {
      // Preserve the documented transport-level refusal (e.g. stale 404).
      return {
        httpStatus: res.status,
        data: { raw: res.text.slice(0, 500) },
        isError: true,
        contentText: res.text.slice(0, 500),
      };
    }
    const messages = parseBody(res.text);
    const message =
      messages.find((m) => m?.id !== undefined && m?.id !== null) ?? messages[0];
    if (message?.error) {
      throw new Error(`RPC error for ${tool}: ${JSON.stringify(message.error)}`);
    }
    const result = message?.result ?? {};
    const contentText = result?.content?.[0]?.text ?? "";
    let data;
    try {
      const parsed = JSON.parse(contentText);
      data = parsed && typeof parsed === "object" ? parsed : { raw: parsed };
    } catch {
      data = { raw: contentText };
    }
    return { httpStatus: res.status, data, isError: result?.isError === true, contentText };
  }

  /**
   * Close the transport session: best-effort terminal unregister when the
   * client owns a registration it should not leave behind, then DELETE /mcp
   * so the server drops the in-memory transport record (health baseline).
   */
  async close({ unregister = true } = {}) {
    if (this.closed) return;
    this.closed = true;
    if (unregister && this.appSessionId) {
      try {
        await this.call("unregister_agent", {});
      } catch {
        /* best-effort */
      }
    }
    try {
      await rawDelete(this.socketPath, this.transportSession);
    } catch {
      /* best-effort */
    }
  }
}
