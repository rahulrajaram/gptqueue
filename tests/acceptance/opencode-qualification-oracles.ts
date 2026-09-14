type JsonObject = Record<string, unknown>;

export type ChildReadinessEvidence = Readonly<{
  reportedAgent: string;
  runtimeID: string;
  assistantText: string;
}>;

export const decode = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};

const asObject = (value: unknown): JsonObject | undefined => {
  const decoded = decode(value);
  return decoded !== null && typeof decoded === "object" && !Array.isArray(decoded)
    ? decoded as JsonObject
    : undefined;
};

type ToolTrace = Readonly<{
  name: string;
  status: string;
  error: boolean;
  output?: unknown;
}>;

const toolTraces = (value: unknown, found: ToolTrace[] = []): readonly ToolTrace[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) {
    decoded.forEach((item) => toolTraces(item, found));
    return found;
  }
  const object = asObject(decoded);
  if (!object) return found;
  const state = asObject(object.state);
  if (typeof object.tool === "string" && state) {
    found.push({
      name: object.tool,
      status: typeof state.status === "string" ? state.status : "unknown",
      error: state.status === "error" || state.error !== undefined,
      output: state.output,
    });
  }
  Object.values(object).forEach((item) => toolTraces(item, found));
  return found;
};

const toolPayload = (trace: ToolTrace): JsonObject | undefined => {
  const output = asObject(trace.output);
  const structured = asObject(output?.structuredContent);
  if (structured) return structured;
  return output;
};

const assistantTexts = (history: unknown): readonly string[] => {
  const decoded = decode(history);
  const entries = Array.isArray(decoded) ? decoded : [];
  return entries.flatMap((entry) => {
    const object = asObject(entry);
    if (object?.info && asObject(object.info)?.role !== "assistant") return [];
    if (!object?.info) return [];
    const parts = Array.isArray(object.parts) ? object.parts : [];
    return parts.flatMap((part) => {
      const item = asObject(part);
      return item?.type === "text" && typeof item.text === "string" ? [item.text] : [];
    });
  });
};

const readinessPresentation = (text: string, expectedAgent: string): boolean => {
  const lines = text.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  const inline = lines.flatMap((line) => {
    const match = /^CHILD_READY\s+`?([^`\s]+)`?$/u.exec(line);
    return match?.[1] ? [match[1]] : [];
  });
  const standalone = lines.filter((line) => /^CHILD_READY$/u.test(line));
  const bound = lines.flatMap((line) => {
    const match = /^(?:[-*]\s*)?Bound gptqueue-opencode name:\s*`?([^`\s]+)`?$/u.exec(line);
    return match?.[1] ? [match[1]] : [];
  });
  if (inline.length > 0) return inline.length === 1 && inline[0] === expectedAgent && bound.length === 0 && standalone.length === 0;
  return standalone.length === 1 && bound.length === 1 && bound[0] === expectedAgent;
};

/** Extract only complete GPTQueue envelope records from native tool history. */
export const parseMcpEnvelope = (value: unknown, found: JsonObject[] = []): readonly JsonObject[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) {
    decoded.forEach((item) => parseMcpEnvelope(item, found));
    return found;
  }
  const object = asObject(decoded);
  if (!object) return found;
  const payload = asObject(object.payload);
  if (typeof object.id === "string" && typeof object.from === "string" &&
      typeof object.to === "string" && typeof object.type === "string" && payload) {
    found.push(object);
  }
  Object.values(object).forEach((item) => parseMcpEnvelope(item, found));
  return found;
};

/**
 * Require both the child assistant marker and the completed exact runtime
 * probe. User prompts and tool output text cannot manufacture readiness.
 */
export const childReadinessEvidence = (
  history: unknown,
  expectedAgent: string,
  expectedRuntimeID: string,
): ChildReadinessEvidence | undefined => {
  const decoded = decode(history);
  const entries = Array.isArray(decoded) ? decoded : [];
  const candidates = assistantTexts(history).filter((text) => /(?:^|\n)\s*CHILD_READY\b|Bound gptqueue-opencode name:/u.test(text));
  if (candidates.length !== 1 || !readinessPresentation(candidates[0]!, expectedAgent)) return undefined;

  const runtimeTrace = toolTraces(entries)
    .filter((trace) => trace.name === "gptqueue_get_runtime_status" || trace.name.endsWith("/gptqueue_get_runtime_status"))
    .find((trace) => trace.status === "completed" && !trace.error);
  const runtimePayload = runtimeTrace ? toolPayload(runtimeTrace) : undefined;
  const runtime = asObject(runtimePayload?.runtime);
  if (runtimePayload?.agent !== expectedAgent || runtime?.runtime_id !== expectedRuntimeID) return undefined;
  return Object.freeze({ reportedAgent: expectedAgent, runtimeID: String(runtime.runtime_id), assistantText: candidates[0]! });
};
