import { isAbsolute } from "node:path";

const MAX_SESSION_ID_LENGTH = 200;

/**
 * Owns one injected GPTQueue backend per trusted OpenCode session.
 *
 * OpenCode native Task children have distinct host session IDs but may share
 * the host's MCP connection.  The host adapter uses this owner to give each
 * session an independent backend connection without changing GPTQueue's
 * public tool contracts.
 */

export type OpenCodeSessionIdentity = Readonly<{
  sessionID: string;
  directory: string;
}>;

export type OpenCodeSessionBackend = Readonly<{
  callTool: (
    name: string,
    args: Readonly<Record<string, unknown>>,
    signal?: AbortSignal,
  ) => Promise<unknown>;
  close: () => Promise<void>;
}>;

export type OpenCodeSessionHandle = OpenCodeSessionIdentity &
  Readonly<{
    callTool: OpenCodeSessionBackend["callTool"];
    close: () => Promise<void>;
  }>;

export type OpenCodeSessionBackendFactory = (
  identity: OpenCodeSessionIdentity
) => Promise<OpenCodeSessionBackend>;

type SessionEntry = {
  readonly identity: OpenCodeSessionIdentity;
  readonly result: Promise<OpenCodeSessionHandle>;
  cancelled: boolean;
  ready: boolean;
};

const invalidIdentity = (identity: OpenCodeSessionIdentity): Error =>
  new Error(
    `OpenCode session identity requires a non-empty sessionID and directory (sessionID=${JSON.stringify(identity.sessionID)})`
  );

const validateIdentity = (
  identity: OpenCodeSessionIdentity
): OpenCodeSessionIdentity => {
  if (
    identity.sessionID.trim().length === 0 ||
    identity.sessionID.length > MAX_SESSION_ID_LENGTH ||
    identity.directory.trim().length === 0 ||
    !isAbsolute(identity.directory)
  ) {
    throw invalidIdentity(identity);
  }
  return Object.freeze({
    sessionID: identity.sessionID,
    directory: identity.directory,
  });
};

class SessionClosedDuringCreationError extends Error {
  public constructor(sessionID: string) {
    super(`OpenCode session '${sessionID}' was closed during backend creation`);
    this.name = "SessionClosedDuringCreationError";
  }
}

/**
 * Session-scoped backend owner. Creation is single-flight per exact host
 * session ID. Retirement is explicit: there is intentionally no idle timer.
 */
export class OpenCodeSessionOwner {
  private readonly entries = new Map<string, SessionEntry>();
  private disposed = false;
  private disposal: Promise<void> | undefined;

  public constructor(private readonly factory: OpenCodeSessionBackendFactory) {}

  public get size(): number {
    return this.entries.size;
  }

  public sessionIDs(): readonly string[] {
    return Object.freeze([...this.entries.keys()]);
  }

  public getOrCreate(
    rawIdentity: OpenCodeSessionIdentity
  ): Promise<OpenCodeSessionHandle> {
    const identity = validateIdentity(rawIdentity);
    if (this.disposed) throw new Error("OpenCode session owner is disposed");
    const existing = this.entries.get(identity.sessionID);
    if (existing) {
      if (existing.identity.directory !== identity.directory) {
        throw new Error(
          `OpenCode session '${identity.sessionID}' is already owned for directory '${existing.identity.directory}'`
        );
      }
      return existing.result;
    }

    let entry!: SessionEntry;
    const result = Promise.resolve()
      .then(() => this.factory(identity))
      .then(async (backend) => {
        if (entry.cancelled) {
          const cancellation = new SessionClosedDuringCreationError(identity.sessionID);
          try {
            await backend.close();
          } catch (error) {
            throw new AggregateError(
              [cancellation, error],
              `OpenCode session '${identity.sessionID}' backend cleanup failed`
            );
          }
          throw cancellation;
        }

        let closing = false;
        let closed = false;
        let closeResult: Promise<void> | undefined;
        const handle: OpenCodeSessionHandle = Object.freeze({
          ...identity,
          callTool: (name, args, signal) => {
            if (closing || closed) {
              return Promise.reject(
                new Error(`OpenCode session '${identity.sessionID}' is closing or closed`)
              );
            }
            return signal === undefined
              ? backend.callTool(name, args)
              : backend.callTool(name, args, signal);
          },
          close: () => {
            if (!closeResult) {
              closing = true;
              closeResult = backend.close().finally(() => {
                closed = true;
                if (this.entries.get(identity.sessionID) === entry) {
                  this.entries.delete(identity.sessionID);
                }
              });
            }
            return closeResult;
          },
        });
        entry.ready = true;
        return handle;
      })
      .catch((error: unknown) => {
        if (this.entries.get(identity.sessionID) === entry) {
          this.entries.delete(identity.sessionID);
        }
        throw error;
      });

    entry = {
      identity,
      result,
      cancelled: false,
      ready: false,
    };
    this.entries.set(identity.sessionID, entry);
    return result;
  }

  /** Retire one session. Closing an in-flight creation closes any late handle. */
  public async close(sessionID: string, directory?: string): Promise<boolean> {
    const entry = this.entries.get(sessionID);
    if (!entry) return false;
    if (directory !== undefined && entry.identity.directory !== directory) return false;

    entry.cancelled = true;
    try {
      const handle = await entry.result;
      await handle.close();
    } catch (error) {
      // A cancelled or failed creation has already attempted backend cleanup.
      // Only surface a close failure from an established backend.
      if (entry.ready || !(error instanceof SessionClosedDuringCreationError)) throw error;
    } finally {
      if (this.entries.get(sessionID) === entry) {
        this.entries.delete(sessionID);
      }
    }
    return true;
  }

  /** Retire all sessions; every close is attempted before reporting failure. */
  public async dispose(): Promise<void> {
    if (this.disposal) return this.disposal;
    this.disposed = true;
    this.disposal = (async () => {
      const outcomes = await Promise.allSettled(
        [...this.entries.keys()].map((sessionID) => this.close(sessionID))
      );
      const failures = outcomes.flatMap((outcome) =>
        outcome.status === "rejected" ? [outcome.reason] : []
      );
      if (failures.length > 0) {
        throw new AggregateError(failures, "OpenCode session disposal failed");
      }
    })();
    return this.disposal;
  }
}
