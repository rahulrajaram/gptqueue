/** Types for scripts/lib/owned-redis.mjs (the one private test Redis implementation). */

export declare const RUN_DIR_PREFIX: string;

export interface PrivateRedisTarget {
  /** Absolute unix-socket path: <runDir>/redis.sock. */
  readonly path: string;
  /** The 0700 run directory holding the socket. */
  readonly dir: string;
  /** Logical database inside the private instance (1-15). */
  readonly db: number;
}

/** Shape check only: `<.../gptq-redis-XXXXXX>/redis.sock?db=1..15`; throws on anything else (any TCP URL). */
export declare function parsePrivateRedisUrl(value: string | undefined): PrivateRedisTarget;

/** Shape check plus live ownership (0700 run dir and socket owned by this user); returns the value. */
export declare function privateRedisUrl(value: string | undefined): string;

/** Every environment variable that names a Redis endpoint (URL aliases and check-queue.sh components). */
export declare const REDIS_ENDPOINT_ENV: readonly string[];

/**
 * The one child-environment builder: `base` with every REDIS_ENDPOINT_ENV
 * variable set to the private URL (default base.REDIS_URL, validated by
 * privateRedisUrl; throws for anything else).
 */
export declare function privateRedisEnv<T extends Record<string, string | undefined>>(base: T, url?: string): T & Record<string, string>;

export interface ServerFacts {
  readonly port: string;
  readonly unixsocket: string;
  readonly processId: number;
}

export type ServerProbe = (target: { socket: string; timeoutMs: number }) => Promise<ServerFacts>;

export declare const probeServer: ServerProbe;

/** A /proc reader: start time (field 22 of /proc/<pid>/stat) and argv, or null when unreadable. */
export interface ProcReader {
  start(pid: number): string | null;
  argv(pid: number): string[] | null;
}

/** The real /proc reader. */
export declare const linuxProc: ProcReader;

export declare function isRedisServingSocket(pid: number, socket: string, proc?: ProcReader): boolean;

/** A recorded redis-server identity (owner.json): pid, start time, socket and (optionally) argv. */
export interface ServerIdentity {
  readonly pid: number;
  readonly start: string;
  readonly socket: string;
  readonly argv?: readonly string[];
}

/** True only while `server` is still the same process (start time, redis-server argv serving its socket). */
export declare function sameServer(server: ServerIdentity, proc?: ProcReader): boolean;

export type Signaller = (pid: number, signal: "SIGTERM" | "SIGKILL") => void;

export interface StopOptions {
  proc?: ProcReader;
  kill?: Signaller;
  sleep?: (ms: number) => Promise<void>;
  boundMs?: number;
}

/** SIGTERM then SIGKILL, each sent only after re-verifying `server`; returns the signals sent. */
export declare function stopVerifiedServer(server: ServerIdentity, options?: StopOptions): Promise<Array<"SIGTERM" | "SIGKILL">>;

export interface ReapOptions {
  root?: string;
  now?: number;
  foreignStaleMs?: number;
  stopTimeoutMs?: number;
  proc?: ProcReader;
  kill?: Signaller;
  sleep?: (ms: number) => Promise<void>;
}

export declare function reapStaleOwnedRedis(options?: ReapOptions): Promise<Array<{ dir: string; signals: Array<"SIGTERM" | "SIGKILL"> }>>;

export interface StartOwnedRedisOptions {
  /** Database encoded in `url` (default 15). */
  db?: number;
  /** Parent of the run directory (default os.tmpdir(), i.e. $TMPDIR). */
  root?: string;
  /** redis-server binary (default $GPTQ_TEST_REDIS_SERVER, then /usr/bin/redis-server). */
  serverBin?: string;
  readyTimeoutMs?: number;
  stopTimeoutMs?: number;
  /** Privacy probe; injectable so a forced mismatch can be tested. */
  probe?: ServerProbe;
  /** Reap stale instances under `root` first (default true). */
  reap?: boolean;
}

export interface OwnedRedis {
  /** `<runDir>/redis.sock?db=N`, accepted by ioredis and the gptqueue CLIs. */
  readonly url: string;
  readonly socket: string;
  readonly dir: string;
  readonly db: number;
  readonly pid: number;
  /** SIGTERM, SIGKILL after a bound, then remove the run dir. Idempotent. */
  close(): Promise<void>;
  /** Synchronous variant for exit/signal paths: SIGKILL and remove the run dir. */
  stop(): void;
}

export declare function startOwnedRedis(options?: StartOwnedRedisOptions): Promise<OwnedRedis>;
