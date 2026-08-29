/**
 * A small, dependency-free MCP client for talking to the GPTQueue Streamable
 * HTTP transport over the real wire, mirroring the install-verify probe's
 * connection pattern (initialize -> mcp-session-id header -> notifications/
 * initialized -> tools/call).
 *
 * Each `connectAgent` owns one MCP HTTP session (and therefore one server-side
 * RedisClient that retains its own registration state).
 */

/**
 * The raw JSON body returned by the server for a tool call. GPTQueue wraps
 * tool results as `content: [{ type: "text", text: "<JSON String>" }]`, with an
 * optional top-level `isError`. We parse the embedded text JSON and return it.
 */
export interface ToolCallResult {
  /** Parsed payload (typically `{ status: "ok" | ... , ... }`). */
  data: Record<string, any>;
  /** True when the server marked the tool result as an error. */
  isError: boolean;
  contentText: string;
}

export interface Agent {
  /** Call an MCP tool on this agent's (single) session. */
  call: (tool: string, args?: Record<string, unknown>) => Promise<ToolCallResult>;
  /** The app-level GPTQueue session_id returned by register_agent. */
  sessionId: string;
  /** The registered agent name. */
  name: string;
  /** Best-effort unregister + abandon the HTTP session. */
  close: () => Promise<void>;
}

/** Split a possibly-SSE HTTP body into parsed JSON fragments. */
function parseBody(text: string): any[] {
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

export async function connectAgent(
  baseUrl: string,
  name: string,
  opts: { role?: "publisher" | "consumer" | "both"; description?: string } = {}
): Promise<Agent> {
  const role = opts.role ?? "both";
  const description =
    opts.description ?? `${name} integration-test agent`;

  // The transport mcp-session-id header id (internal to the HTTP handshake),
  // tracked separately from the app-level GPTQueue session_id.
  let transportSession: string | null = null;
  // App-level GPTQueue session_id, first provisionally from the handshake then
  // definitively from register_agent's result.
  let appSessionId: string | null = null;
  let nextId = 1;

  const post = async (
    method: string,
    params: Record<string, unknown> | undefined,
    isNotification = false
  ): Promise<any> => {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
    };
    if (transportSession) headers["mcp-session-id"] = transportSession;

    const body: Record<string, unknown> = { jsonrpc: "2.0", method };
    if (!isNotification) {
      body.id = nextId++;
      if (params !== undefined) body.params = params;
    } else if (params !== undefined) {
      body.params = params;
    }

    const res = await fetch(baseUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    const sid = res.headers.get("mcp-session-id");
    if (sid && transportSession === null) transportSession = sid;

    const text = await res.text();
    const messages = parseBody(text);
    if (isNotification) return null;
    const message =
      messages.find((m) => m?.id !== undefined && m?.id !== null) ?? messages[0];
    if (message?.error) {
      const err = new Error(
        JSON.stringify(message.error) ?? `RPC error (${res.status})`
      ) as Error & { rpcError?: unknown; status?: number };
      err.rpcError = message.error;
      err.status = res.status;
      throw err;
    }
    if (message?.result === undefined && messages.length === 0) {
      throw new Error(`Empty response for ${method} (status ${res.status})`);
    }
    return message?.result ?? message;
  };

  // ---- Handshake ---------------------------------------------------------
  await post("initialize", {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "gptqueue-integration", version: "1.0.0" },
  });
  await post("notifications/initialized", undefined, true);
  if (!transportSession) {
    throw new Error("server did not return an mcp-session-id during initialize");
  }

  const call = async (
    tool: string,
    args: Record<string, unknown> = {}
  ): Promise<ToolCallResult> => {
    const result = (await post("tools/call", {
      name: tool,
      arguments: args,
    })) as {
      content?: Array<{ type: string; text: string }>;
      structuredContent?: Record<string, any>;
      isError?: boolean;
    };
    const contentText = result?.content?.[0]?.text ?? "";
    let data: Record<string, any>;
    try {
      const parsed = JSON.parse(contentText);
      data = parsed && typeof parsed === "object" ? parsed : { raw: parsed };
    } catch {
      data = { raw: contentText };
    }
    return { data, isError: result?.isError === true, contentText };
  };

  const close = async (): Promise<void> => {
    try {
      await call("unregister_agent", { session_id: appSessionId ?? undefined });
    } catch {
      /* best-effort */
    }
  };

  // Register the agent now that the session is bound, then adopt the app-level
  // session_id that every session-scoped tool expects.
  const reg = await call("register_agent", { name, role, description });
  const returnedSessionId = reg.data.session_id as string | undefined;
  if (!returnedSessionId) {
    throw new Error(`register_agent for '${name}' did not return a session_id`);
  }
  appSessionId = returnedSessionId;

  return {
    call,
    sessionId: appSessionId,
    name,
    close,
  };
}