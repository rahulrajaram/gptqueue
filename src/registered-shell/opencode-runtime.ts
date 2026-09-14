import { createHash } from "node:crypto";
import { resolve } from "node:path";
import type {
  ActivationOutcome,
  ActivationRequest,
  OpenCodeBinding,
  RuntimeAdapter,
} from "./runtime.js";

export type OpenCodeSessionStatus = "idle" | "busy" | "retry";
export type OpenCodeHistoryPart = Readonly<{ type?: string; text?: string }>;
export type OpenCodeHistoryEntry = Readonly<{
  info?: Readonly<{
    id?: string;
    role?: string;
    sessionID?: string;
  }>;
  parts?: readonly OpenCodeHistoryPart[];
}>;

/** Narrow native host port; the adapter never imports or infers host state. */
export interface OpenCodeRuntimePort {
  readIdentity(signal: AbortSignal): Promise<Readonly<{
    runtime_id: string;
    working_directory: string;
    epoch?: string;
  }>>;
  status(signal: AbortSignal): Promise<OpenCodeSessionStatus>;
  history(signal: AbortSignal): Promise<readonly OpenCodeHistoryEntry[]>;
  /** Mirrors OpenCode prompt_async's deterministic native messageID field. */
  promptAsync(prompt: Readonly<{ messageID: string; text: string }>, signal: AbortSignal): Promise<void>;
  close?(): Promise<void>;
}

export interface OpenCodeRuntimeOptions {
  readonly timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10_000;
const marker = (operationId: string): string => `GPTQueue activation operation: ${operationId}`;
export const nativeMessageId = (operationId: string): string =>
  `msg_${createHash("sha256").update(operationId).digest("hex")}`;

const identityMatches = (
  binding: OpenCodeBinding,
  identity: Readonly<{ runtime_id: string; working_directory: string; epoch?: string }>,
): boolean => identity.runtime_id === binding.runtime_id &&
  resolve(identity.working_directory) === resolve(binding.working_directory) &&
  (identity.epoch === undefined || identity.epoch === binding.epoch);

const entryContains = (entry: OpenCodeHistoryEntry, operationId: string, runtimeId: string): boolean =>
  entry.info?.role === "user" &&
  entry.info.id === nativeMessageId(operationId) &&
  entry.info.sessionID === runtimeId &&
  entry.parts?.some((part) => part.type === "text" &&
    typeof part.text === "string" &&
    (part.text === marker(operationId) || part.text.startsWith(`${marker(operationId)}\n`))) === true;

const turnId = (entry: OpenCodeHistoryEntry, operationId: string, runtimeId: string): string => {
  const id = entry.info?.id;
  if (!id || !entryContains(entry, operationId, runtimeId)) {
    throw new Error("OpenCode history entry is not a matching native user message");
  }
  return id;
};

const withDeadline = async <T>(
  work: (signal: AbortSignal) => Promise<T>,
  parent: AbortSignal,
  timeoutMs: number,
): Promise<T> => {
  parent.throwIfAborted();
  const controller = new AbortController();
  const abort = () => controller.abort();
  parent.addEventListener("abort", abort, { once: true });
  if (parent.aborted) controller.abort();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const deadline = new Promise<never>((_, reject) => controller.signal.addEventListener(
    "abort", () => reject(new Error("OpenCode runtime deadline exceeded")), { once: true },
  ));
  try {
    const result = work(controller.signal);
    return await Promise.race([
      result,
      deadline,
    ]);
  } finally {
    clearTimeout(timer);
    parent.removeEventListener("abort", abort);
  }
};

/** OpenCode's native prompt API is asynchronous; durable inbox semantics remain in startInboxDispatcher. */
export const createOpenCodeRuntime = async (
  binding: OpenCodeBinding,
  port: OpenCodeRuntimePort,
  options: OpenCodeRuntimeOptions = {},
): Promise<RuntimeAdapter> => {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (binding.client !== "opencode") throw new Error("OpenCode runtime requires an OpenCode binding");
  try {
    const identity = await withDeadline((signal) => port.readIdentity(signal), new AbortController().signal, timeoutMs);
    if (!identityMatches(binding, identity)) throw new Error("OpenCode session identity does not match binding");
  } catch (error) {
    if (port.close) await port.close().catch(() => undefined);
    throw error;
  }

  let closed = false;
  let closeResult: Promise<void> | undefined;
  return {
    binding,
    activate: async (request: ActivationRequest, signal: AbortSignal): Promise<ActivationOutcome> => {
      if (closed || signal.aborted) return { status: "unavailable" };
      let identity: Awaited<ReturnType<OpenCodeRuntimePort["readIdentity"]>>;
      try {
        identity = await withDeadline((inner) => port.readIdentity(inner), signal, timeoutMs);
      } catch {
        return { status: "unavailable" };
      }
      if (!identityMatches(binding, identity)) return { status: "unavailable" };

      let history: readonly OpenCodeHistoryEntry[];
      try {
        history = await withDeadline((inner) => port.history(inner), signal, timeoutMs);
      } catch {
        return { status: "ambiguous" };
      }
      const existing = history.find((entry) => entryContains(entry, request.operation_id, binding.runtime_id));
      if (existing) {
        try {
          const status = await withDeadline((inner) => port.status(inner), signal, timeoutMs);
          return status === "busy" || status === "retry"
            ? { status: "started", turn_id: turnId(existing, request.operation_id, binding.runtime_id) }
            : { status: "completed", turn_id: turnId(existing, request.operation_id, binding.runtime_id) };
        } catch {
          return { status: "ambiguous" };
        }
      }
      if (request.recover_only) return { status: "ambiguous" };

      let status: OpenCodeSessionStatus;
      try {
        status = await withDeadline((inner) => port.status(inner), signal, timeoutMs);
      } catch {
        return { status: "unavailable" };
      }
      if (status !== "idle") return { status: "busy" };

      const prompt = `${marker(request.operation_id)}\n${request.prompt}`;
      try {
        await withDeadline((inner) => port.promptAsync({ messageID: nativeMessageId(request.operation_id), text: prompt }, inner), signal, timeoutMs);
        return { status: "queued" };
      } catch {
        // Any native rejection may have crossed the delivery boundary. Persist
        // the operation as ambiguous so recovery performs history
        // reconciliation instead of blindly submitting again.
        return { status: "ambiguous" };
      }
    },
    close: () => {
      if (!closeResult) {
        closed = true;
        closeResult = port.close ? port.close() : Promise.resolve();
      }
      return closeResult;
    },
  };
};
